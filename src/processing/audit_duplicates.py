
from pathlib import Path
from datetime import date, timedelta
import argparse
import calendar
import duckdb
import json
import time

# =========================================================
# AMAZING 360 - DUPLICATE QUALITY AUDIT
# =========================================================

ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data/silver/events"
MANIFESTS = ROOT / "data/manifests"
TEMP = ROOT / "data/temp"

MONTHS = {
    "2019-Oct": (2019, 10),
    "2019-Nov": (2019, 11),
    "2019-Dec": (2019, 12),
    "2020-Jan": (2020, 1),
    "2020-Feb": (2020, 2),
    "2020-Mar": (2020, 3),
    "2020-Apr": (2020, 4),
}


# =========================================================
# 1. SQL STRING ESCAPING
# =========================================================

def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


# =========================================================
# 2. NORMALISATION DU FORMAT DES MOIS
# =========================================================

def normalize_month(value):
    """
    Convertit les formats connus vers YYYY-MM.

    Exemples :
        2019-Oct -> 2019-10
        2019-Nov -> 2019-11
        2019-10  -> 2019-10

    Cela permet de comparer les anciens rapports
    avec les nouveaux sans modifier les fichiers.
    """

    if value in MONTHS:
        year, month_number = MONTHS[value]
        return f"{year:04d}-{month_number:02d}"

    return value


# =========================================================
# 3. ENREGISTREMENT OU COMPARAISON DES RAPPORTS
# =========================================================

def save_or_compare(path, report, keys):
    """
    Si le rapport existe :
        comparer ses indicateurs sans l'ecraser.

    Sinon :
        creer le rapport JSON.

    Le champ month est compare apres normalisation.
    """

    if path.exists():

        previous = json.loads(
            path.read_text(encoding="utf-8")
        )

        for key in keys:

            if key not in previous:
                raise RuntimeError(
                    f"Indicateur absent : {key} "
                    f"dans {path.name}"
                )

            old_value = previous[key]
            new_value = report[key]

            # CORRECTION PRINCIPALE :
            # 2019-Oct et 2019-10 sont equivalents.
            if key == "month":
                old_value = normalize_month(old_value)
                new_value = normalize_month(new_value)

            if old_value != new_value:
                raise RuntimeError(
                    f"Resultat different pour {key} "
                    f"dans {path.name} : "
                    f"{old_value} != {new_value}"
                )

        print(
            f"Rapport existant verifie : {path.name}",
            flush=True
        )

    else:

        path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        path.write_text(
            json.dumps(
                report,
                indent=4,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )

        print(
            f"Nouveau rapport cree : {path.name}",
            flush=True
        )


# =========================================================
# 4. ANALYSE DES DOUBLONS POUR UNE JOURNEE
# =========================================================

def analyze_day(con, parquet, day):

    query = """
        SELECT
            COUNT(*) AS duplicate_groups,

            COALESCE(
                SUM(n - 1),
                0
            ) AS extra_rows,

            COALESCE(
                MAX(n),
                0
            ) AS max_repetitions

        FROM (
            SELECT
                COUNT(*) AS n

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

        ) AS duplicate_candidates
    """

    start = time.perf_counter()

    groups, extra, maximum = con.execute(
        query,
        [
            str(parquet),
            day.isoformat()
        ]
    ).fetchone()

    report = {
        "date": day.isoformat(),
        "duplicate_groups": int(groups),
        "candidate_extra_rows": int(extra),
        "max_repetitions": int(maximum),
        "execution_seconds": round(
            time.perf_counter() - start,
            2
        )
    }

    output = MANIFESTS / f"{day}.duplicates.json"

    save_or_compare(
        output,
        report,
        [
            "date",
            "duplicate_groups",
            "candidate_extra_rows",
            "max_repetitions"
        ]
    )

    print(
        f"{day} | "
        f"Groupes : {groups:,} | "
        f"Lignes candidates : {extra:,} | "
        f"Max : {maximum:,}",
        flush=True
    )

    return report


# =========================================================
# 5. AUDIT COMPLET D'UN MOIS
# =========================================================

def audit_duplicates(month, selected_day=None):

    year, month_number = MONTHS[month]

    parquet = SILVER / f"{month}.parquet"

    if not parquet.is_file():
        raise FileNotFoundError(parquet)

    MANIFESTS.mkdir(
        parents=True,
        exist_ok=True
    )

    TEMP.mkdir(
        parents=True,
        exist_ok=True
    )

    # =====================================================
    # 5.1 SELECTION DES JOURS
    # =====================================================

    if selected_day:

        day = date.fromisoformat(selected_day)

        if (day.year, day.month) != (year, month_number):
            raise ValueError(
                "Date hors du mois selectionne"
            )

        days = [day]

    else:

        number_of_days = calendar.monthrange(
            year,
            month_number
        )[1]

        first_day = date(
            year,
            month_number,
            1
        )

        days = [
            first_day + timedelta(days=i)
            for i in range(number_of_days)
        ]

    print("=" * 55)
    print("AMAZING 360 - DUPLICATE QUALITY AUDIT")
    print("=" * 55)

    print("Mois :", month)
    print("Jours a analyser :", len(days))

    start = time.perf_counter()

    # =====================================================
    # 5.2 CONFIGURATION DUCKDB
    # =====================================================

    con = duckdb.connect()

    try:

        con.execute(
            "SET memory_limit = '4GB'"
        )

        con.execute(
            "SET threads = 2"
        )

        temp_path = TEMP / "duplicate_swap"

        con.execute(
            "SET temp_directory = "
            + sql_text(temp_path.as_posix())
        )

        con.execute(
            "SET max_temp_directory_size = '8GB'"
        )

        # =================================================
        # 5.3 ANALYSE JOUR PAR JOUR
        # =================================================

        reports = []

        for day in days:

            daily_report = analyze_day(
                con,
                parquet,
                day
            )

            reports.append(daily_report)

    finally:
        con.close()

    # =====================================================
    # 6. AGREGATION MENSUELLE
    # =====================================================

    total_groups = sum(
        report["duplicate_groups"]
        for report in reports
    )

    total_extra = sum(
        report["candidate_extra_rows"]
        for report in reports
    )

    maximum = max(
        report["max_repetitions"]
        for report in reports
    )

    duration = round(
        time.perf_counter() - start,
        2
    )

    print("\n=== RESULTATS ===")

    print(f"Jours analyses : {len(reports)}")
    print(f"Groupes repetes : {total_groups:,}")
    print(f"Lignes candidates : {total_extra:,}")
    print(f"Repetitions maximales : {maximum:,}")
    print(f"Duree : {duration} secondes")

    # =====================================================
    # 7. RAPPORT MENSUEL
    # =====================================================

    if selected_day is None:

        summary = {
            "month": month,
            "days_analyzed": len(reports),
            "duplicate_groups": total_groups,
            "candidate_extra_rows": total_extra,
            "max_repetitions": maximum,
            "execution_seconds": duration,
            "daily_results": reports
        }

        output = (
            MANIFESTS /
            f"{month}.duplicates_summary.json"
        )

        save_or_compare(
            output,
            summary,
            [
                "month",
                "days_analyzed",
                "duplicate_groups",
                "candidate_extra_rows",
                "max_repetitions"
            ]
        )

        print("\nRapport mensuel :", output)

    print("\nAUDIT TERMINE !")


# =========================================================
# 8. EXECUTION
# =========================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Amazing 360 - "
            "Duplicate Quality Audit"
        )
    )

    parser.add_argument(
        "--month",
        choices=MONTHS.keys(),
        default="2019-Oct",
        help="Mois REES46 a analyser"
    )

    parser.add_argument(
        "--day",
        default=None,
        help="Analyser une seule date YYYY-MM-DD"
    )

    args = parser.parse_args()

    audit_duplicates(
        month=args.month,
        selected_day=args.day
    )
