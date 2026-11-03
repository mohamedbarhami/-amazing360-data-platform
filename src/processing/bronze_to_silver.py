
from pathlib import Path
import argparse
import duckdb
import json
import os
import shutil
import time

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/2019-Oct.csv.gz"
AUDIT = ROOT / "data/manifests/2019-Oct.audit.json"
SALT_FILE = ROOT / "data/private/user_hash_salt.txt"

SILVER = ROOT / "data/silver/events"
TEMP = ROOT / "data/temp"

EXPECTED_MONTH = "2019-10"


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def convert(sample=True):
    SILVER.mkdir(parents=True, exist_ok=True)
    TEMP.mkdir(parents=True, exist_ok=True)

    for required in [SOURCE, AUDIT, SALT_FILE]:
        if not required.exists():
            raise FileNotFoundError(required)

    # Réutiliser obligatoirement le sel déjà créé.
    salt = SALT_FILE.read_text(encoding="utf-8").strip()

    if not salt:
        raise RuntimeError("Sel de pseudonymisation vide")

    if sample:
        output = TEMP / "october_sample.parquet"
        limit = "LIMIT 100000"
    else:
        output = SILVER / "2019-Oct.parquet"
        limit = ""

    # Écriture intermédiaire dans le même dossier.
    temporary_output = output.with_name(
        output.name + ".inprogress"
    )

    if output.exists():
        print(f"Déjà présent : {output}")
        return

    if temporary_output.exists():
        raise RuntimeError(
            "Ancien fichier temporaire détecté : "
            f"{temporary_output}"
        )

    free_space = shutil.disk_usage(ROOT).free

    if free_space < 25 * 1024**3:
        raise RuntimeError(
            "Moins de 25 Gio disponibles sur le disque"
        )

    audit = json.loads(AUDIT.read_text(encoding="utf-8"))

    print("=" * 55)
    print("AMAZING 360 - BRONZE TO SILVER")
    print("=" * 55)
    print("Mode :", "SAMPLE" if sample else "FULL")
    print("Source :", SOURCE.name)

    start = time.perf_counter()

    try:
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
                        {sql_text(SOURCE.as_posix())},
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
                    '2019-10' AS source_month

                FROM parsed
            )
            TO {sql_text(temporary_output.as_posix())}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """

            print("\nTransformation en cours...")
            con.execute(query)

            stats = con.execute("""
                SELECT
                    COUNT(*) AS total,
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
                        WHERE event_timestamp_utc IS NULL
                           OR user_key IS NULL
                           OR LENGTH(user_key) <> 64
                           OR price IS NULL
                           OR price < 0
                           OR event_type NOT IN (
                               'view', 'cart', 'purchase'
                           )
                    ) AS invalid_rows
                FROM read_parquet(?)
            """, [str(temporary_output)]).fetchone()

            total, views, carts, purchases, invalid = stats

            expected_total = (
                100_000 if sample else audit["total_events"]
            )

            if total != expected_total:
                raise RuntimeError(
                    f"Nombre de lignes incorrect : "
                    f"{total} != {expected_total}"
                )

            if invalid != 0:
                raise RuntimeError(
                    f"{invalid} lignes invalides détectées"
                )

            if not sample:
                expected_counts = (
                    audit["views"],
                    audit["carts"],
                    audit["purchases"]
                )

                if (views, carts, purchases) != expected_counts:
                    raise RuntimeError(
                        "Répartition des événements différente "
                        "de l'audit Bronze"
                    )

        finally:
            con.close()

        # Publier uniquement après validation.
        os.replace(temporary_output, output)

        duration = time.perf_counter() - start
        size_mib = output.stat().st_size / 1024**2

        print("\nTRANSFORMATION COMPLETE !")
        print(f"Événements : {total:,}")
        print(f"Vues : {views:,}")
        print(f"Paniers : {carts:,}")
        print(f"Achats : {purchases:,}")
        print(f"Lignes invalides : {invalid}")
        print(f"Taille Parquet : {size_mib:,.2f} Mio")
        print(f"Durée : {duration:.2f} secondes")
        print(f"Fichier : {output}")

    except BaseException:
        temporary_output.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--full",
        action="store_true",
        help="Convertir toutes les lignes d'octobre"
    )

    args = parser.parse_args()
    convert(sample=not args.full)
