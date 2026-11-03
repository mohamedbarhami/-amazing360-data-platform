
from pathlib import Path
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data/silver/events/2019-Oct.parquet"
REPORT = ROOT / "data/manifests/2019-Oct.zero_prices.json"


def audit_zero_prices():

    print("=" * 55)
    print("AMAZING 360 - ZERO PRICE ANALYSIS")
    print("=" * 55)

    if not SILVER.exists():
        raise FileNotFoundError(SILVER)

    start = time.perf_counter()

    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")

        query = """
            SELECT
                event_type,
                COUNT(*) AS total_events,

                COUNT(*) FILTER (
                    WHERE price = 0
                ) AS zero_prices,

                COUNT(*) FILTER (
                    WHERE price > 0
                ) AS positive_prices,

                ROUND(
                    100.0 * COUNT(*) FILTER (
                        WHERE price = 0
                    ) / COUNT(*),
                    4
                ) AS zero_percentage

            FROM read_parquet(?)

            GROUP BY event_type
            ORDER BY event_type
        """

        cursor = con.execute(query, [str(SILVER)])
        columns = [c[0] for c in cursor.description]

        rows = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

        print("\n=== RESULTATS PAR TYPE ===")

        for row in rows:
            print(f"\nType : {row['event_type']}")
            print(f"Total : {row['total_events']:,}")
            print(f"Prix zero : {row['zero_prices']:,}")
            print(f"Prix positifs : {row['positive_prices']:,}")
            print(f"Pourcentage zero : {row['zero_percentage']} %")

        total_zero = sum(
            row["zero_prices"] for row in rows
        )

        duration = round(time.perf_counter() - start, 2)

        report = {
            "month": "2019-10",
            "total_zero_prices": total_zero,
            "results": rows,
            "execution_seconds": duration
        }

        REPORT.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        REPORT.write_text(
            json.dumps(report, indent=4),
            encoding="utf-8"
        )

        print("\n=== BILAN ===")
        print(f"Total prix zero : {total_zero:,}")
        print(f"Duree : {duration} secondes")
        print(f"Rapport : {REPORT}")

        print("\nAUDIT TERMINE !")

    finally:
        con.close()


if __name__ == "__main__":
    audit_zero_prices()
