
import json
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="amazing360_validation",
    description="Validation du premier mois Silver Amazing 360",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["amazing360", "data-quality"],
)
def amazing360_validation():

    @task
    def check_october():
        report_file = Path(
            "/opt/airflow/project/data/manifests/"
            "2019-Oct.silver_validation.json"
        )

        if not report_file.exists():
            raise FileNotFoundError(report_file)

        report = json.loads(
            report_file.read_text(encoding="utf-8")
        )

        checks = report["checks"]

        if report["status"] != "PASS":
            raise ValueError("Validation Silver non conforme")

        if len(checks) != 17 or not all(checks.values()):
            raise ValueError("Un ou plusieurs contrôles ont échoué")

        return {
            "month": report["month"],
            "events": report["results"]["total_events"],
            "checks_passed": len(checks),
        }

    @task
    def display_results(result):
        print("AMAZING 360 - AIRFLOW")
        print("Mois :", result["month"])
        print("Evenements :", result["events"])
        print("Controles valides :", result["checks_passed"])
        print("PREMIER DAG EXECUTE AVEC SUCCES")

    display_results(check_october())


amazing360_validation()
