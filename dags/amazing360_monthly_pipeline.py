
import json
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, Param, get_current_context


ROOT = Path("/opt/airflow/project")
MANIFESTS = ROOT / "data/manifests"

MONTHS = [
    "2019-Oct",
    "2019-Nov",
    "2019-Dec",
    "2020-Jan",
    "2020-Feb",
    "2020-Mar",
    "2020-Apr",
]


def run_script(relative_path, *arguments):
    """Execute un script existant et remonte les erreurs."""

    script = ROOT / relative_path

    if not script.is_file():
        raise FileNotFoundError(script)

    command = [
        sys.executable,
        str(script),
        *arguments,
    ]

    print("Execution :", command, flush=True)

    subprocess.run(
        command,
        cwd=str(ROOT),
        check=True,
    )


@dag(
    dag_id="amazing360_monthly_pipeline",
    description="Pipeline mensuel REES46 Bronze Silver Quality",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    params={
        "month": Param(
            "2019-Oct",
            type="string",
            enum=MONTHS,
            description="Mois REES46 à traiter",
        )
    },
    tags=["amazing360", "bronze", "silver", "quality"],
)
def amazing360_monthly_pipeline():

    @task
    def select_month():
        month = get_current_context()["params"]["month"]

        if month not in MONTHS:
            raise ValueError(f"Mois non autorise : {month}")

        print(f"Mois selectionne : {month}")
        return month

    @task
    def download_data(month):
        # Ce script utilise un argument positionnel.
        run_script(
            "src/ingestion/download_month.py",
            month,
        )
        return month

    @task
    def validate_bronze(month):
        run_script(
            "src/ingestion/validate_source.py",
            "--month",
            month,
        )
        return month

    @task
    def audit_bronze(month):
        report = MANIFESTS / f"{month}.audit.json"

        arguments = ["--month", month]

        if report.exists():
            print("Audit deja present : comparaison.")
            arguments.append("--check")
        else:
            print("Nouvel audit Bronze.")

        run_script(
            "src/processing/audit_bronze.py",
            *arguments,
        )
        return month

    @task
    def bronze_to_silver(month):
        run_script(
            "src/processing/bronze_to_silver.py",
            "--month",
            month,
            "--full",
        )
        return month

    @task
    def validate_silver(month):
        run_script(
            "src/processing/validate_silver.py",
            "--month",
            month,
            "--full",
        )
        return month

    @task
    def publish_summary(month):
        report_path = (
            MANIFESTS / f"{month}.silver_validation.json"
        )

        if not report_path.is_file():
            raise FileNotFoundError(report_path)

        report = json.loads(
            report_path.read_text(encoding="utf-8")
        )

        checks = report["checks"]

        if (
            report["status"] != "PASS"
            or len(checks) != 17
            or not all(checks.values())
        ):
            raise RuntimeError(
                "Validation qualite incomplete ou echouee"
            )

        print("=" * 55)
        print("AMAZING 360 - MONTHLY PIPELINE SUMMARY")
        print("=" * 55)
        print("Mois :", month)
        print(
            "Evenements :",
            f"{report['results']['total_events']:,}"
        )
        print("Controles :", len(checks), "/ 17")
        print("Statut : PASS")
        print("PIPELINE TERMINE AVEC SUCCES !")

    month = select_month()

    downloaded = download_data(month)
    bronze_validated = validate_bronze(downloaded)
    audited = audit_bronze(bronze_validated)
    converted = bronze_to_silver(audited)
    silver_validated = validate_silver(converted)
    publish_summary(silver_validated)


amazing360_monthly_pipeline()
