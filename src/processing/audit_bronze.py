
from pathlib import Path
from datetime import date
import argparse
import duckdb
import json
import time

# =========================================================
# AMAZING 360 - BRONZE DATA QUALITY AUDIT
# =========================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data/bronze"
MANIFESTS = ROOT / "data/manifests"

MONTHS = {
    "2019-Oct": (2019, 10),
    "2019-Nov": (2019, 11),
    "2019-Dec": (2019, 12),
    "2020-Jan": (2020, 1),
    "2020-Feb": (2020, 2),
    "2020-Mar": (2020, 3),
    "2020-Apr": (2020, 4),
}


def audit_bronze(month, check=False):

    year, month_number = MONTHS[month]

    source = BRONZE / f"{month}.csv.gz"
    report_file = MANIFESTS / f"{month}.audit.json"

    # =====================================================
    # 1. VERIFICATION DES FICHIERS
    # =====================================================

    if not source.is_file():
        raise FileNotFoundError(source)

    if check and not report_file.is_file():
        raise FileNotFoundError(report_file)

    if not check and report_file.exists():
        raise FileExistsError(
            f"Audit deja existant : {report_file}. "
            "Utiliser --check pour comparer sans ecraser."
        )

    # =====================================================
    # 2. CALCUL DE LA PERIODE
    # =====================================================

    start_date = date(year, month_number, 1)

    if month_number == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month_number + 1, 1)

    print("=" * 55)
    print("AMAZING 360 - BRONZE DATA QUALITY AUDIT")
    print("=" * 55)

    print("Mois :", month)
    print("Source :", source.name)
    print("Mode :", "CHECK" if check else "AUDIT")

    start = time.perf_counter()

    # =====================================================
    # 3. CONFIGURATION DUCKDB
    # =====================================================

    con = duckdb.connect()

    try:
        con.execute("SET memory_limit = '4GB'")
        con.execute("SET threads = 2")

        # =================================================
        # 4. AUDIT COMPLET DES EVENEMENTS
        # =================================================

        query = """
            WITH parsed AS (
                SELECT
                    TRY_STRPTIME(
                        event_time,
                        '%Y-%m-%d %H:%M:%S UTC'
                    ) AS event_ts,

                    event_type,
                    category_code,
                    brand,
                    user_id,
                    user_session,

                    TRY_CAST(
                        price AS DECIMAL(18,2)
                    ) AS numeric_price

                FROM read_csv(
                    ?,
                    header = true,
                    compression = 'gzip',
                    all_varchar = true,
                    strict_mode = true,
                    ignore_errors = false
                )
            )

            SELECT

                -- =========================================
                -- VOLUME GLOBAL
                -- =========================================

                COUNT(*) AS total_events,

                -- =========================================
                -- TYPES D'EVENEMENTS
                -- =========================================

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
                           'view',
                           'cart',
                           'purchase'
                       )
                ) AS unknown_events,

                -- =========================================
                -- VALEURS MANQUANTES
                -- =========================================

                COUNT(*) FILTER (
                    WHERE user_id IS NULL
                       OR TRIM(user_id) = ''
                ) AS missing_users,

                COUNT(*) FILTER (
                    WHERE category_code IS NULL
                       OR TRIM(category_code) = ''
                ) AS missing_categories,

                COUNT(*) FILTER (
                    WHERE brand IS NULL
                       OR TRIM(brand) = ''
                ) AS missing_brands,

                COUNT(*) FILTER (
                    WHERE user_session IS NULL
                       OR TRIM(user_session) = ''
                ) AS missing_sessions,

                -- =========================================
                -- QUALITE DES PRIX
                -- =========================================

                COUNT(*) FILTER (
                    WHERE numeric_price IS NULL
                ) AS invalid_prices,

                COUNT(*) FILTER (
                    WHERE numeric_price < 0
                ) AS negative_prices,

                COUNT(*) FILTER (
                    WHERE numeric_price = 0
                ) AS zero_prices,

                -- =========================================
                -- PRIX A ZERO PAR TYPE D'EVENEMENT
                -- =========================================

                COUNT(*) FILTER (
                    WHERE numeric_price = 0
                      AND event_type = 'view'
                ) AS zero_views,

                COUNT(*) FILTER (
                    WHERE numeric_price = 0
                      AND event_type = 'cart'
                ) AS zero_carts,

                COUNT(*) FILTER (
                    WHERE numeric_price = 0
                      AND event_type = 'purchase'
                ) AS zero_purchases,

                -- =========================================
                -- CONTROLE DES DATES
                -- =========================================

                COUNT(*) FILTER (
                    WHERE event_ts IS NULL
                       OR event_ts < CAST(? AS TIMESTAMP)
                       OR event_ts >= CAST(? AS TIMESTAMP)
                ) AS dates_outside_month,

                MIN(event_ts) AS first_event,

                MAX(event_ts) AS last_event

            FROM parsed
        """

        cursor = con.execute(
            query,
            [
                str(source),
                start_date.isoformat(),
                end_date.isoformat()
            ]
        )

        columns = [
            column[0]
            for column in cursor.description
        ]

        results = dict(
            zip(columns, cursor.fetchone())
        )

    finally:
        con.close()

    # =====================================================
    # 5. AFFICHAGE DES RESULTATS
    # =====================================================

    print("\n=== RESULTATS DE L'AUDIT ===")

    for name, value in results.items():
        if isinstance(value, int):
            print(f"{name}: {value:,}")
        else:
            print(f"{name}: {value}")

    duration = round(
        time.perf_counter() - start,
        2
    )

    # =====================================================
    # 6. CONTROLES DE COHERENCE
    # =====================================================

    event_total = (
        results["views"]
        + results["carts"]
        + results["purchases"]
        + results["unknown_events"]
    )

    if event_total != results["total_events"]:
        raise RuntimeError(
            "Incoherence dans le comptage des evenements"
        )

    zero_total = (
        results["zero_views"]
        + results["zero_carts"]
        + results["zero_purchases"]
    )

    if results["unknown_events"] == 0:
        if zero_total != results["zero_prices"]:
            raise RuntimeError(
                "Incoherence dans les prix a zero"
            )

    print("\nPASS : Coherence des types d'evenements")
    print("PASS : Coherence des prix a zero")

    # =====================================================
    # 7. MODE CHECK - COMPARAISON HISTORIQUE
    # =====================================================

    if check:

        previous = json.loads(
            report_file.read_text(
                encoding="utf-8"
            )
        )

        required_keys = [
            "total_events",
            "views",
            "carts",
            "purchases",
            "unknown_events",
            "missing_users",
            "missing_categories",
            "invalid_prices",
            "negative_prices",
        ]

        optional_keys = [
            "missing_brands",
            "missing_sessions",
            "zero_prices",
            "zero_views",
            "zero_carts",
            "zero_purchases",
        ]

        print("\n=== COMPARAISON HISTORIQUE ===")

        for key in required_keys:

            if key not in previous:
                raise ValueError(
                    f"Indicateur absent du rapport : {key}"
                )

            if results[key] != previous[key]:
                raise ValueError(
                    f"{key}: {results[key]} != {previous[key]}"
                )

            print(f"PASS : {key}")

        # Comparer les nouveaux indicateurs uniquement
        # s'ils figurent deja dans le rapport historique.

        for key in optional_keys:

            if key in previous:

                if results[key] != previous[key]:
                    raise ValueError(
                        f"{key}: {results[key]} != {previous[key]}"
                    )

                print(f"PASS : {key}")

        # Compatibilite avec l'ancien audit d'octobre.

        previous_dates = previous.get(
            "dates_outside_month",
            previous.get("dates_outside_october")
        )

        if previous_dates is not None:

            if (
                results["dates_outside_month"]
                != previous_dates
            ):
                raise ValueError(
                    "Dates differentes du rapport historique"
                )

            print("PASS : dates_outside_month")

        # Comparaison des prix a zero avec
        # l'ancien rapport detaille d'octobre.

        zero_report_file = (
            MANIFESTS / f"{month}.zero_prices.json"
        )

        if zero_report_file.is_file():

            zero_report = json.loads(
                zero_report_file.read_text(
                    encoding="utf-8"
                )
            )

            zero_by_type = {
                row["event_type"]: row["zero_prices"]
                for row in zero_report["results"]
            }

            for event_type, key in [
                ("view", "zero_views"),
                ("cart", "zero_carts"),
                ("purchase", "zero_purchases"),
            ]:
                if event_type in zero_by_type:

                    if (
                        results[key]
                        != zero_by_type[event_type]
                    ):
                        raise ValueError(
                            f"{key}: different du rapport "
                            "des prix a zero"
                        )

                    print(f"PASS : {key} (rapport historique)")

        print("\nAUDIT COMPARE AVEC SUCCES !")
        print("Rapport historique conserve.")

    # =====================================================
    # 8. MODE AUDIT - ENREGISTREMENT JSON
    # =====================================================

    else:

        report = {
            "month": month,
            **results,
            "execution_seconds": duration
        }

        MANIFESTS.mkdir(
            parents=True,
            exist_ok=True
        )

        report_file.write_text(
            json.dumps(
                report,
                indent=4,
                default=str
            ),
            encoding="utf-8"
        )

        print("\nAUDIT TERMINE !")
        print("Rapport :", report_file)

    print(f"\nDuree : {duration} secondes")


# =========================================================
# 9. EXECUTION DU SCRIPT
# =========================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Amazing 360 - Bronze Data Quality Audit"
    )

    parser.add_argument(
        "--month",
        choices=MONTHS.keys(),
        default="2019-Oct",
        help="Mois REES46 a analyser"
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="Comparer avec l'audit existant sans l'ecraser"
    )

    args = parser.parse_args()

    audit_bronze(
        month=args.month,
        check=args.check
    )
