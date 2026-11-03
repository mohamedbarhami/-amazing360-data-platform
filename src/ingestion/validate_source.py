
from pathlib import Path
import hashlib
import gzip
import json

ROOT = Path(__file__).resolve().parents[2]

FILE = ROOT / "data/bronze/2019-Oct.csv.gz"
MANIFEST = ROOT / "data/manifests/2019-Oct.json"

CHUNK_SIZE = 8 * 1024 * 1024


def validate_source():
    print("=" * 50)
    print("AMAZING 360 - SOURCE VALIDATION")
    print("=" * 50)

    if not FILE.exists():
        raise FileNotFoundError(FILE)

    if not MANIFEST.exists():
        raise FileNotFoundError(MANIFEST)

    metadata = json.loads(
        MANIFEST.read_text(encoding="utf-8")
    )

    print("\n1. Verification du SHA-256")

    checksum = hashlib.sha256()
    size = 0

    with FILE.open("rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            checksum.update(chunk)
            size += len(chunk)

    assert size == metadata["size_bytes"], (
        "Taille differente du manifeste"
    )

    assert checksum.hexdigest() == metadata["sha256"], (
        "SHA-256 different du manifeste"
    )

    print("OK : Taille et SHA-256 verifies")

    print("\n2. Verification complete GZIP")

    total_uncompressed = 0

    # Lecture complete sans extraction sur disque
    with gzip.open(FILE, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            total_uncompressed += len(chunk)

    print("OK : GZIP entierement lisible")
    print(
        f"Volume decompresse : "
        f"{total_uncompressed / 1024**3:.2f} Gio"
    )

    report = {
        "file": FILE.name,
        "sha256_valid": True,
        "gzip_valid": True,
        "compressed_bytes": size,
        "uncompressed_bytes": total_uncompressed
    }

    output = (
        ROOT / "data/manifests/"
        "2019-Oct.validation.json"
    )

    output.write_text(
        json.dumps(report, indent=4),
        encoding="utf-8"
    )

    print("\nVALIDATION TERMINEE !")


if __name__ == "__main__":
    validate_source()
