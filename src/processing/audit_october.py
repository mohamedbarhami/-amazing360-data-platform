
from pathlib import Path
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data/bronze/2019-Oct.csv.gz"
REPORT = ROOT / "data/manifests/2019-Oct.audit.json"


def audit_october():
    print("=" * 55)
    print("AMAZING 360 - FULL DATA AUDIT")
    print("=" * 55)

    if not SOURCE.exists():
        raise FileNotFoundError(SOURCE)

    start = time.perf_counter()

    con = duckdb.connect()

    # Configuration pour 16 Go de RAM
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")

    print("\nAnalyse de TOUT le mois d'octobre...")
    print("Aucune limite de 100 000 lignes.\n")

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
                WHERE event_type IS NULL
                   OR event_type NOT IN (
                       'view', 'cart', 'purchase'
                   )
            ) AS unknown_events,

            COUNT(*) FILTER (
                WHERE user_id IS NULL
                   OR TRIM(user_id) = ''
            ) AS missing_users,

            COUNT(*) FILTER (
                WHERE category_code IS NULL
                   OR TRIM(category_code) = ''
            ) AS missing_categories,

            COUNT(*) FILTER (
                WHERE TRY_CAST(price AS DOUBLE) IS NULL
            ) AS invalid_prices,

            COUNT(*) FILTER (
                WHERE TRY_CAST(price AS DOUBLE) < 0
            ) AS negative_prices,

            COUNT(*) FILTER (
                WHERE event_time IS NULL
                   OR LEFT(event_time, 7) <> '2019-10'
            ) AS dates_outside_october,

            MIN(event_time) AS first_event,
            MAX(event_time) AS last_event

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
        cursor = con.execute(query, [str(SOURCE)])

        columns = [col[0] for col in cursor.description]
        values = cursor.fetchone()

        results = dict(zip(columns, values))
        results["execution_seconds"] = round(
            time.perf_counter() - start, 2
        )

        print("=== RESULTATS DE L'AUDIT ===")

        for key, value in results.items():
            print(f"{key}: {value}")

        REPORT.parent.mkdir(parents=True, exist_ok=True)

        REPORT.write_text(
            json.dumps(
                results,
                indent=4,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )

        print("\nAUDIT TERMINE AVEC SUCCES !")
        print(f"Rapport : {REPORT}")

    finally:
        con.close()


if __name__ == "__main__":
    audit_october()
