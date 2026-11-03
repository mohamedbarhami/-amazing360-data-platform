
import json
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


ROOT = Path("/opt/airflow/project")
MANIFESTS = ROOT / "data/manifests"

SILVER = ROOT / "data/silver/events/2019-Oct.parquet"

VALIDATION_SCRIPT = (
    ROOT / "src/processing/validate_silver.py"
)

VALIDATION_REPORT = (
    MANIFESTS / "2019-Oct.silver_validation.json"
)


@dag(
    dag_id="amazing360_monthly_pipeline",
    description="Amazing 360 - Premier pipeline orchestre pour octobre",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["amazing360", "data-engineering", "silver"],
)
def amazing360_monthly_pipeline():

    @task
    def check_inputs():
        required = [
            SILVER,
            VALIDATION_SCRIPT,
            MANIFESTS / "2019-Oct.audit.json",
            MANIFESTS / "2019-Oct.quality.json",
            MANIFESTS / "2019-Oct.zero_prices.json",
        ]

        for path in required:
            if not path.is_file():
                raise FileNotFoundError(path)

            print(f"OK : {path.name}")

        print("Toutes les entrees sont disponibles.")

        return str(ROOT)

    @task
    def run_silver_validation(project_root):
        script = Path(project_root) / (
            "src/processing/validate_silver.py"
        )

        print("Execution de la validation Silver...", flush=True)

        subprocess.run(
            [sys.executable, str(script), "--full"],
            cwd=project_root,
            check=True,
        )

        print("Script termine avec succes.", flush=True)

        return str(VALIDATION_REPORT)

    @task
    def publish_quality_summary(report_path):
        path = Path(report_path)

        if not path.is_file():
            raise FileNotFoundError(path)

        report = json.loads(
            path.read_text(encoding="utf-8")
        )

        checks = report["checks"]

        if report["status"] != "PASS":
            raise RuntimeError("Audit Silver non conforme")

        if len(checks) != 17 or not all(checks.values()):
            raise RuntimeError(
                "Un ou plusieurs controles ont echoue"
            )

        print("=" * 50)
        print("AMAZING 360 - PIPELINE QUALITY SUMMARY")
        print("=" * 50)

        print("Mois :", report["month"])

        print(
            "Evenements :",
            f"{report['results']['total_events']:,}"
        )

        print("Controles reussis :", len(checks), "/ 17")
        print("Statut :", report["status"])

        print("\nPIPELINE EXECUTE AVEC SUCCES !")

    project_root = check_inputs()
    report_path = run_silver_validation(project_root)
    publish_quality_summary(report_path)


amazing360_monthly_pipeline()
