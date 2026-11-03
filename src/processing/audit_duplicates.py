
from pathlib import Path
from datetime import date, timedelta
import argparse
import duckdb
import json
import time

ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data/silver/events/2019-Oct.parquet"
REPORT_DIR = ROOT / "data/manifests"
TEMP_DIR = ROOT / "data/temp"


def audit_day(con, day):
    """Analyse les doublons candidats d'une journée."""

    query = """
        SELECT
            COUNT(*) AS duplicate_groups,
            COALESCE(SUM(n - 1), 0) AS extra_rows,
            COALESCE(MAX(n), 0) AS max_repetitions
        FROM (
            SELECT COUNT(*) AS n
            FROM read_parquet(?)
            WHERE event_date = CAST(? AS DATE)
            GROUP BY
                event_timestamp_utc,
                event_type,
                product_id,
                category_id,
                category_code,
                brand,
                price,
                user_key,
                session_key
            HAVING COUNT(*) > 1
        ) AS repeated_events
    """

    start = time.perf_counter()

    result = con.execute(
        query, [str(SILVER), day.isoformat()]
    ).fetchone()

    groups, extra, maximum = map(int, result)

    duration = round(time.perf_counter() - start, 2)

    report = {
        "date": day.isoformat(),
        "duplicate_groups": groups,
        "candidate_extra_rows": extra,
        "max_repetitions": maximum,
        "execution_seconds": duration
    }

    output = REPORT_DIR / f"{day}.duplicates.json"
    output.write_text(
        json.dumps(report, indent=4),
        encoding="utf-8"
    )

    print(
        f"{day} | Groupes: {groups:,} | "
        f"Lignes: {extra:,} | {duration}s",
        flush=True
    )

    return report


def audit_october(selected_day=None):
    """Analyse une journée ou tout octobre 2019."""

    if not SILVER.exists():
        raise FileNotFoundError(SILVER)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("AMAZING 360 - DUPLICATE QUALITY AUDIT")
    print("=" * 55)

    if selected_day:
        days = [date.fromisoformat(selected_day)]
    else:
        days = [
            date(2019, 10, 1) + timedelta(days=i)
            for i in range(31)
        ]

    for day in days:
        if day.year != 2019 or day.month != 10:
            raise ValueError("Date hors octobre 2019")

    start = time.perf_counter()

    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")

        temp_path = (TEMP_DIR / "duplicate_swap").resolve()
        safe_path = temp_path.as_posix().replace("'", "''")

        con.execute(
            f"SET temp_directory = '{safe_path}'"
        )
        con.execute(
            "SET max_temp_directory_size = '8GB'"
        )

        reports = []

        print("\nAnalyse des journées...\n")

        for day in days:
            report = audit_day(con, day)
            reports.append(report)

    finally:
        con.close()

    total_groups = sum(
        r["duplicate_groups"] for r in reports
    )

    total_extra = sum(
        r["candidate_extra_rows"] for r in reports
    )

    max_repetitions = max(
        r["max_repetitions"] for r in reports
    )

    duration = round(time.perf_counter() - start, 2)

    print("\n" + "=" * 55)
    print("RESULTATS GLOBAUX")
    print("=" * 55)

    print(f"Jours analyses : {len(reports)}")
    print(f"Groupes repetes : {total_groups:,}")
    print(f"Lignes candidates : {total_extra:,}")
    print(f"Repetitions maximales : {max_repetitions:,}")
    print(f"Duree totale : {duration}s")

    if selected_day is None:
        summary = {
            "month": "2019-10",
            "days_analyzed": len(reports),
            "duplicate_groups": total_groups,
            "candidate_extra_rows": total_extra,
            "max_repetitions": max_repetitions,
            "execution_seconds": duration,
            "daily_results": reports
        }

        output = (
            REPORT_DIR /
            "2019-Oct.duplicates_summary.json"
        )

        output.write_text(
            json.dumps(summary, indent=4),
            encoding="utf-8"
        )

        print(f"\nRapport mensuel : {output}")

    print("\nAUDIT TERMINE !")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--day",
        default=None,
        help="Date particuliere : YYYY-MM-DD"
    )

    args = parser.parse_args()

    audit_october(args.day)
