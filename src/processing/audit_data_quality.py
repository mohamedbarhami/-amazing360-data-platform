
from pathlib import Path
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/2019-Oct.csv.gz"

REPORT = ROOT / "data/manifests/2019-Oct.quality.json"

COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session"
]


def audit_quality():

    print("=" * 55)
    print("AMAZING 360 - COMPLETE DATA QUALITY AUDIT")
    print("=" * 55)

    if not SOURCE.exists():
        raise FileNotFoundError(SOURCE)

    start = time.perf_counter()

    con = duckdb.connect()
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")

    expressions = ["COUNT(*) AS total_events"]

    for column in COLUMNS:

        expressions.append(f"""
            COUNT(*) FILTER (
                WHERE "{column}" IS NULL
            ) AS "{column}_null"
        """)

        expressions.append(f"""
            COUNT(*) FILTER (
                WHERE "{column}" IS NOT NULL
                AND TRIM("{column}") = ''
            ) AS "{column}_empty"
        """)

    expressions.extend([
        """
        COUNT(*) FILTER (
            WHERE TRY_CAST(price AS DECIMAL(18,2)) IS NULL
        ) AS invalid_prices
        """,
        """
        COUNT(*) FILTER (
            WHERE TRY_CAST(price AS DECIMAL(18,2)) < 0
        ) AS negative_prices
        """,
        """
        COUNT(*) FILTER (
            WHERE TRY_CAST(price AS DECIMAL(18,2)) = 0
        ) AS zero_prices
        """,
        """
        COUNT(*) FILTER (
            WHERE event_type IS NULL
               OR event_type NOT IN (
                   'view', 'cart', 'purchase'
               )
        ) AS unknown_events
        """
    ])

    query = f"""
        SELECT {", ".join(expressions)}
        FROM read_csv(
            ?,
            header = true,
            compression = 'gzip',
            all_varchar = true,
            strict_mode = true,
            ignore_errors = false
        )
    """

    try:
        print("\nAnalyse du fichier complet...")

        cursor = con.execute(query, [str(SOURCE)])

        names = [c[0] for c in cursor.description]
        values = cursor.fetchone()

        results = dict(zip(names, values))

    finally:
        con.close()

    total = results["total_events"]

    print(f"\nTotal : {total:,} evenements")

    print("\n=== NULL ET VALEURS VIDES ===")

    for column in COLUMNS:

        nulls = results[f"{column}_null"]
        empty = results[f"{column}_empty"]

        missing = nulls + empty
        percent = missing / total * 100

        print(
            f"{column:20} | "
            f"NULL: {nulls:>10,} | "
            f"Vides: {empty:>10,} | "
            f"Total: {percent:.2f}%"
        )

    print("\n=== AUTRES CONTROLES ===")

    for key in [
        "invalid_prices",
        "negative_prices",
        "zero_prices",
        "unknown_events"
    ]:
        print(f"{key}: {results[key]:,}")

    duration = time.perf_counter() - start

    results["execution_seconds"] = round(duration, 2)

    REPORT.parent.mkdir(parents=True, exist_ok=True)

    REPORT.write_text(
        json.dumps(results, indent=4),
        encoding="utf-8"
    )

    print(f"\nDuree : {duration:.2f} secondes")
    print(f"Rapport : {REPORT}")
    print("\nAUDIT TERMINE !")


if __name__ == "__main__":
    audit_quality()
