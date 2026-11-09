
from pathlib import Path
import argparse
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]
MANIFESTS = ROOT / "data/manifests"

MONTHS = {
    "2019-Oct": "2019-10",
    "2019-Nov": "2019-11",
    "2019-Dec": "2019-12",
    "2020-Jan": "2020-01",
    "2020-Feb": "2020-02",
    "2020-Mar": "2020-03",
    "2020-Apr": "2020-04",
}


def load_json(path):
    if not path.is_file():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def validate(month, full=False):
    period = MONTHS[month]

    if full:
        parquet = ROOT / f"data/silver/events/{month}.parquet"
    else:
        parquet = ROOT / f"data/temp/{month}.sample.parquet"

    if not parquet.is_file():
        raise FileNotFoundError(parquet)

    year, number = map(int, period.split("-"))
    first_day = f"{period}-01"

    if number == 12:
        next_day = f"{year + 1}-01-01"
    else:
        next_day = f"{year}-{number + 1:02d}-01"

    print("=" * 55)
    print("AMAZING 360 - SILVER VALIDATION")
    print("=" * 55)
    print("Mois :", month)
    print("Mode :", "FULL" if full else "SAMPLE")

    start = time.perf_counter()
    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")

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
                WHERE price = 0 AND event_type = 'view'
            ) AS zero_views,

            COUNT(*) FILTER (
                WHERE price = 0 AND event_type = 'cart'
            ) AS zero_carts,

            COUNT(*) FILTER (
                WHERE price = 0 AND event_type = 'purchase'
            ) AS zero_purchases,

            COUNT(*) FILTER (
                WHERE price IS NULL OR price < 0
            ) AS invalid_prices,

            COUNT(*) FILTER (
                WHERE event_type IS NULL
                   OR event_type NOT IN (
                       'view', 'cart', 'purchase'
                   )
            ) AS invalid_types,

            COUNT(*) FILTER (
                WHERE user_key IS NULL
                   OR LENGTH(user_key) <> 64
            ) AS invalid_users,

            COUNT(*) FILTER (
                WHERE session_key IS NOT NULL
                  AND LENGTH(session_key) <> 64
            ) AS invalid_sessions,

            COUNT(*) FILTER (
                WHERE event_timestamp_utc IS NULL
                   OR event_timestamp_utc
                       < CAST(? AS TIMESTAMP)
                   OR event_timestamp_utc
                       >= CAST(? AS TIMESTAMP)
                   OR event_date IS DISTINCT FROM
                       CAST(event_timestamp_utc AS DATE)
            ) AS invalid_dates,

            COUNT(*) FILTER (
                WHERE source_month IS DISTINCT FROM ?
            ) AS invalid_months

        FROM read_parquet(?)
        """

        cursor = con.execute(
            query,
            [first_day, next_day, period, str(parquet)]
        )

        names = [c[0] for c in cursor.description]
        results = dict(zip(names, cursor.fetchone()))

    finally:
        con.close()

    expected = {
        "invalid_prices": 0,
        "invalid_types": 0,
        "invalid_users": 0,
        "invalid_sessions": 0,
        "invalid_dates": 0,
        "invalid_months": 0,
    }

    if full:
        bronze = load_json(
            MANIFESTS / f"{month}.audit.json"
        )

        expected.update({
            "total_events": bronze["total_events"],
            "views": bronze["views"],
            "carts": bronze["carts"],
            "purchases": bronze["purchases"],
            "missing_categories": bronze["missing_categories"],
        })

        # Les nouveaux audits Bronze contiennent
        # directement ces indicateurs.
        if all(
            key in bronze for key in (
                "missing_brands",
                "missing_sessions",
                "zero_prices",
            )
        ):
            expected.update({
                "missing_brands": bronze["missing_brands"],
                "missing_sessions": bronze["missing_sessions"],
                "zero_prices": bronze["zero_prices"],
            })

        elif month == "2019-Oct":
            # Compatibilite avec l'audit historique
            # d'octobre, sans modifier son rapport.
            quality = load_json(
                MANIFESTS / "2019-Oct.quality.json"
            )

            expected.update({
                "missing_brands":
                    quality["brand_null"]
                    + quality["brand_empty"],
                "missing_sessions":
                    quality["user_session_null"]
                    + quality["user_session_empty"],
                "zero_prices": quality["zero_prices"],
            })

        else:
            raise ValueError(
                "Indicateurs manquants dans l'audit Bronze"
            )

        # Verification detaillee des prix a zero
        # si les comptes par type sont disponibles.
        zero_keys = [
            "zero_views",
            "zero_carts",
            "zero_purchases",
        ]

        if all(key in bronze for key in zero_keys):
            expected.update({
                key: bronze[key] for key in zero_keys
            })

        elif month == "2019-Oct":
            zero_report = load_json(
                MANIFESTS / "2019-Oct.zero_prices.json"
            )

            zero_by_type = {
                row["event_type"]: row["zero_prices"]
                for row in zero_report["results"]
            }

            expected.update({
                "zero_views": zero_by_type["view"],
                "zero_carts": zero_by_type["cart"],
                "zero_purchases": zero_by_type["purchase"],
            })

    else:
        expected["total_events"] = 100_000

    checks = {
        key: results[key] == value
        for key, value in expected.items()
    }

    print("\n=== CONTROLES ===")

    for key, passed in checks.items():
        print(f"{key}: {'PASS' if passed else 'FAIL'}")

    success = all(checks.values())
    duration = round(time.perf_counter() - start, 2)

    print(f"\nControles reussis : {sum(checks.values())}/{len(checks)}")
    print(f"Duree : {duration} secondes")

    if full:
        report = {
            "month": period,
            "status": "PASS" if success else "FAIL",
            "results": results,
            "checks": checks,
            "execution_seconds": duration,
        }

        output = (
            MANIFESTS / f"{month}.silver_validation.json"
        )

        output.write_text(
            json.dumps(report, indent=4),
            encoding="utf-8"
        )

        print("Rapport :", output)

    if not success:
        raise RuntimeError("Validation Silver echouee")

    print("\nSUCCESS : SILVER VALIDATED !")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--month",
        choices=MONTHS.keys(),
        default="2019-Oct",
    )

    parser.add_argument(
        "--full",
        action="store_true",
    )

    args = parser.parse_args()
    validate(args.month, args.full)
