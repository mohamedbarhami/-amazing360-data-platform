
from pathlib import Path
import argparse
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]

MANIFESTS = ROOT / "data/manifests"

BRONZE_AUDIT = MANIFESTS / "2019-Oct.audit.json"
QUALITY_AUDIT = MANIFESTS / "2019-Oct.quality.json"
ZERO_AUDIT = MANIFESTS / "2019-Oct.zero_prices.json"

SILVER_FULL = (
    ROOT / "data/silver/events/2019-Oct.parquet"
)

SILVER_SAMPLE = (
    ROOT / "data/temp/october_sample.parquet"
)


def load_json(path):
    if not path.exists():
        raise FileNotFoundError(path)

    return json.loads(path.read_text(encoding="utf-8"))


def validate(full=False):

    file = SILVER_FULL if full else SILVER_SAMPLE

    if not file.exists():
        raise FileNotFoundError(file)

    print("=" * 55)
    print("AMAZING 360 - SILVER VALIDATION")
    print("=" * 55)

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
                    WHERE price = 0
                      AND event_type = 'view'
                ) AS zero_views,

                COUNT(*) FILTER (
                    WHERE price = 0
                      AND event_type = 'cart'
                ) AS zero_carts,

                COUNT(*) FILTER (
                    WHERE price = 0
                      AND event_type = 'purchase'
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
                          < TIMESTAMP '2019-10-01'
                       OR event_timestamp_utc
                          >= TIMESTAMP '2019-11-01'
                       OR event_date IS DISTINCT FROM
                          CAST(event_timestamp_utc AS DATE)
                ) AS invalid_dates,

                COUNT(*) FILTER (
                    WHERE source_month IS DISTINCT FROM
                          '2019-10'
                ) AS invalid_months

            FROM read_parquet(?)
        """

        cursor = con.execute(query, [str(file)])
        columns = [c[0] for c in cursor.description]

        results = dict(zip(columns, cursor.fetchone()))

    finally:
        con.close()

    # Controles applicables a l'echantillon
    expected = {
        "total_events": (
            42448764 if full else 100000
        ),
        "invalid_prices": 0,
        "invalid_types": 0,
        "invalid_users": 0,
        "invalid_sessions": 0,
        "invalid_dates": 0,
        "invalid_months": 0
    }

    if full:
        bronze = load_json(BRONZE_AUDIT)
        quality = load_json(QUALITY_AUDIT)
        zero = load_json(ZERO_AUDIT)

        zero_by_type = {
            item["event_type"]: item["zero_prices"]
            for item in zero["results"]
        }

        expected.update({
            "total_events": bronze["total_events"],
            "views": bronze["views"],
            "carts": bronze["carts"],
            "purchases": bronze["purchases"],

            "missing_categories": (
                quality["category_code_null"]
                + quality["category_code_empty"]
            ),

            "missing_brands": (
                quality["brand_null"]
                + quality["brand_empty"]
            ),

            "missing_sessions": (
                quality["user_session_null"]
                + quality["user_session_empty"]
            ),

            "zero_prices": quality["zero_prices"],
            "zero_views": zero_by_type["view"],
            "zero_carts": zero_by_type["cart"],
            "zero_purchases": zero_by_type["purchase"]
        })

    checks = {
        name: results[name] == value
        for name, value in expected.items()
    }

    print("\n=== RESULTATS ===")

    for name, value in results.items():
        print(f"{name}: {value:,}")

    print("\n=== CONTROLES ===")

    for name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{name}: {status}")

    success = all(checks.values())

    duration = round(time.perf_counter() - start, 2)

    print(f"\nDuree : {duration} secondes")

    if full:
        report = {
            "month": "2019-10",
            "status": "PASS" if success else "FAIL",
            "results": results,
            "checks": checks,
            "execution_seconds": duration
        }

        output = (
            MANIFESTS /
            "2019-Oct.silver_validation.json"
        )

        output.write_text(
            json.dumps(report, indent=4),
            encoding="utf-8"
        )

        print(f"Rapport : {output}")

    if not success:
        raise RuntimeError(
            "Validation Silver echouee !"
        )

    print("\nSUCCESS : SILVER VALIDATED !")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--full",
        action="store_true"
    )

    args = parser.parse_args()

    validate(full=args.full)
