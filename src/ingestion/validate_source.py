
from pathlib import Path
import argparse
import hashlib
import gzip
import json
import time

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data/bronze"
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

CHUNK_SIZE = 8 * 1024 * 1024


def validate_source(month):
    source = BRONZE / f"{month}.csv.gz"
    manifest = MANIFESTS / f"{month}.json"
    report_path = MANIFESTS / f"{month}.validation.json"

    print("=" * 55)
    print("AMAZING 360 - SOURCE VALIDATION")
    print("=" * 55)
    print(f"Mois : {month}")

    if not source.is_file():
        raise FileNotFoundError(source)

    if not manifest.is_file():
        raise FileNotFoundError(manifest)

    metadata = json.loads(
        manifest.read_text(encoding="utf-8")
    )

    if metadata["filename"] != source.name:
        raise ValueError("Nom du fichier incorrect")

    start = time.perf_counter()

    print("\n1. Verification SHA-256")

    checksum = hashlib.sha256()
    compressed_bytes = 0

    with source.open("rb") as file:
        while chunk := file.read(CHUNK_SIZE):
            checksum.update(chunk)
            compressed_bytes += len(chunk)

    if compressed_bytes != metadata["size_bytes"]:
        raise ValueError("Taille du fichier incorrecte")

    if checksum.hexdigest() != metadata["sha256"]:
        raise ValueError("SHA-256 incorrect")

    print("OK : Taille et SHA-256 verifies")

    print("\n2. Verification GZIP")

    uncompressed_bytes = 0

    with gzip.open(source, "rb") as file:
        while chunk := file.read(CHUNK_SIZE):
            uncompressed_bytes += len(chunk)

    print("OK : GZIP entierement lisible")

    report = {
        "month": month,
        "file": source.name,
        "sha256_valid": True,
        "gzip_valid": True,
        "compressed_bytes": compressed_bytes,
        "uncompressed_bytes": uncompressed_bytes,
        "execution_seconds": round(
            time.perf_counter() - start, 2
        )
    }

    report_path.write_text(
        json.dumps(report, indent=4),
        encoding="utf-8"
    )

    print("\nVALIDATION TERMINEE !")
    print(f"Rapport : {report_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--month",
        choices=MONTHS,
        default="2019-Oct"
    )

    args = parser.parse_args()

    validate_source(args.month)
