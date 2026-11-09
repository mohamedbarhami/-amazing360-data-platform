
from pathlib import Path
import argparse
import duckdb
import json
import os
import shutil
import time

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data/bronze"
SILVER = ROOT / "data/silver/events"
TEMP = ROOT / "data/temp"
MANIFESTS = ROOT / "data/manifests"
SALT_FILE = ROOT / "data/private/user_hash_salt.txt"

MONTHS = {
    "2019-Oct": "2019-10",
    "2019-Nov": "2019-11",
    "2019-Dec": "2019-12",
    "2020-Jan": "2020-01",
    "2020-Feb": "2020-02",
    "2020-Mar": "2020-03",
    "2020-Apr": "2020-04",
}


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def validate_parquet(con, path, audit, period, full):
    year, month_number = map(int, period.split("-"))

    if month_number == 12:
        next_month = f"{year + 1}-01-01"
    else:
        next_month = f"{year}-{month_number + 1:02d}-01"

    first_day = f"{period}-01"

    query = """
        SELECT
            COUNT(*) AS total_events,

            COUNT(*) FILTER (
                WHERE event_type = 'view'
            ) AS views,

            COUNT(*) FILTER (
                WHERE event_type = 'cart'
            ) AS carts,

            COUNT(*) FILTER (
                WHERE event_type = 'purchase'
            ) AS purchases,

            COUNT(*) FILTER (
                WHERE category_code IS NULL
            ) AS missing_categories,

            COUNT(*) FILTER (
                WHERE brand IS NULL
            ) AS missing_brands,

            COUNT(*) FILTER (
                WHERE session_key IS NULL
            ) AS missing_sessions,

            COUNT(*) FILTER (
                WHERE price = 0
            ) AS zero_prices,

            COUNT(*) FILTER (
                WHERE event_timestamp_utc IS NULL
                   OR event_timestamp_utc < CAST(? AS TIMESTAMP)
                   OR event_timestamp_utc >= CAST(? AS TIMESTAMP)
                   OR event_date IS DISTINCT FROM
                      CAST(event_timestamp_utc AS DATE)
                   OR source_month IS DISTINCT FROM ?
                   OR event_type IS NULL
                   OR event_type NOT IN (
                       'view', 'cart', 'purchase'
                   )
                   OR price IS NULL
                   OR price < 0
                   OR user_key IS NULL
                   OR LENGTH(user_key) <> 64
                   OR (
                       session_key IS NOT NULL
                       AND LENGTH(session_key) <> 64
                   )
            ) AS invalid_rows

        FROM read_parquet(?)
    """

    cursor = con.execute(
        query, [first_day, next_month, period, str(path)]
    )

    names = [column[0] for column in cursor.description]
    results = dict(zip(names, cursor.fetchone()))

    expected_total = (
        audit["total_events"] if full
        else min(100_000, audit["total_events"])
    )

    if results["total_events"] != expected_total:
        raise RuntimeError(
            "Nombre de lignes incorrect : "
            f"{results['total_events']} != {expected_total}"
        )

    if results["invalid_rows"] != 0:
        raise RuntimeError(
            f"{results['invalid_rows']} lignes invalides"
        )

    if full:
        for key in [
            "views",
            "carts",
            "purchases",
            "missing_categories",
            "missing_brands",
            "missing_sessions",
            "zero_prices",
        ]:
            if key in audit and results[key] != audit[key]:
                raise RuntimeError(
                    f"{key}: {results[key]} != {audit[key]}"
                )

    return results


def convert(month, full=False):
    period = MONTHS[month]

    source = BRONZE / f"{month}.csv.gz"
    audit_file = MANIFESTS / f"{month}.audit.json"

    if full:
        output = SILVER / f"{month}.parquet"
    else:
        output = TEMP / f"{month}.sample.parquet"

    temporary = output.with_name(
        output.name + ".inprogress"
    )

    for required in [source, audit_file, SALT_FILE]:
        if not required.is_file():
            raise FileNotFoundError(required)

    audit = json.loads(
        audit_file.read_text(encoding="utf-8")
    )

    # Ne jamais générer un nouveau sel ici :
    # on réutilise celui de tous les mois.
    salt = SALT_FILE.read_text(
        encoding="utf-8"
    ).strip()

    if not salt:
        raise RuntimeError("Sel de pseudonymisation vide")

    SILVER.mkdir(parents=True, exist_ok=True)
    TEMP.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("AMAZING 360 - BRONZE TO SILVER")
    print("=" * 55)
    print("Mois :", month)
    print("Mode :", "FULL" if full else "SAMPLE")

    start = time.perf_counter()

    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")

        con.execute(
            "SET temp_directory = "
            + sql_text((TEMP / "duckdb_swap").as_posix())
        )

        con.execute(
            "SET max_temp_directory_size = '8GB'"
        )

        if output.exists():
            print("\nFichier deja present.")
            print("Verification du fichier existant...")

            results = validate_parquet(
                con, output, audit, period, full
            )

            print(
                f"SKIP : {results['total_events']:,} "
                "evenements deja valides"
            )
            return

        if temporary.exists():
            raise RuntimeError(
                f"Fichier temporaire existant : {temporary}"
            )

        if full and shutil.disk_usage(ROOT).free < 25 * 1024**3:
            raise RuntimeError(
                "Moins de 25 Gio disponibles sur le disque"
            )

        limit = "" if full else "LIMIT 100000"

        query = f"""
            COPY (
                WITH parsed AS (
                    SELECT
                        STRPTIME(
                            event_time,
                            '%Y-%m-%d %H:%M:%S UTC'
                        ) AS event_timestamp_utc,

                        event_type,
                        product_id,
                        category_id,

                        NULLIF(TRIM(category_code), '')
                            AS category_code,

                        NULLIF(TRIM(brand), '')
                            AS brand,

                        CAST(price AS DECIMAL(18,2))
                            AS price,

                        SHA256(
                            {sql_text(salt + '|user|')}
                            || user_id
                        ) AS user_key,

                        CASE
                            WHEN user_session IS NULL
                              OR TRIM(user_session) = ''
                            THEN NULL
                            ELSE SHA256(
                                {sql_text(salt + '|session|')}
                                || user_session
                            )
                        END AS session_key

                    FROM read_csv(
                        {sql_text(source.as_posix())},
                        header = true,
                        compression = 'gzip',
                        all_varchar = true,
                        strict_mode = true,
                        ignore_errors = false
                    )

                    {limit}
                )

                SELECT
                    *,
                    CAST(event_timestamp_utc AS DATE)
                        AS event_date,
                    {sql_text(period)} AS source_month

                FROM parsed
            )

            TO {sql_text(temporary.as_posix())}
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        print("\nTransformation en cours...")
        con.execute(query)

        results = validate_parquet(
            con, temporary, audit, period, full
        )

        # Publication apres validation uniquement.
        if output.exists():
            raise FileExistsError(output)

        os.rename(temporary, output)

        duration = time.perf_counter() - start

        print("\nTRANSFORMATION TERMINEE !")
        print(f"Evenements : {results['total_events']:,}")
        print(f"Lignes invalides : {results['invalid_rows']}")
        print(
            f"Taille Parquet : "
            f"{output.stat().st_size / 1024**2:.2f} Mio"
        )
        print(f"Duree : {duration:.2f} secondes")
        print(f"Fichier : {output}")

    finally:
        con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--month",
        choices=MONTHS.keys(),
        default="2019-Oct"
    )

    parser.add_argument(
        "--full",
        action="store_true"
    )

    args = parser.parse_args()

    convert(args.month, args.full)
