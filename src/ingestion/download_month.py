
import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import requests


# Configuration du projet
ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data" / "bronze"
MANIFESTS = ROOT / "data" / "manifests"

BASE_URL = "https://data.rees46.com/datasets/marketplace"

MONTHS = [
    "2019-Oct", "2019-Nov", "2019-Dec",
    "2020-Jan", "2020-Feb", "2020-Mar",
    "2020-Apr"
]

# Toujours conserver au moins 20 Gio libres
MIN_FREE_SPACE = 20 * 1024**3
CHUNK_SIZE = 4 * 1024**2


def download_month(month):

    BRONZE.mkdir(parents=True, exist_ok=True)
    MANIFESTS.mkdir(parents=True, exist_ok=True)

    filename = f"{month}.csv.gz"
    url = f"{BASE_URL}/{filename}"

    destination = BRONZE / filename
    temporary = BRONZE / f"{filename}.part"
    manifest = MANIFESTS / f"{month}.json"

    print("=" * 50)
    print("AMAZING 360 - AUTOMATED DATA INGESTION")
    print("=" * 50)
    print(f"Month: {month}")
    print(f"Source: {url}")

    # Ne pas écraser un fichier déjà présent
    if destination.exists():
        print("Fichier déjà présent : téléchargement ignoré.")
        return

    # Une tentative interrompue sera recommencée
    if temporary.exists():
        temporary.unlink()

    checksum = hashlib.sha256()
    downloaded = 0
    last_progress = -1

    try:
        with requests.get(
            url,
            stream=True,
            timeout=(20, 120),
            headers={"Accept-Encoding": "identity"}
        ) as response:

            response.raise_for_status()

            header_size = response.headers.get(
                "Content-Length"
            )
            expected_size = (
                int(header_size) if header_size else None
            )

            free_space = shutil.disk_usage(BRONZE).free

            if expected_size is not None:
                required = expected_size + MIN_FREE_SPACE
                if free_space < required:
                    raise RuntimeError(
                        "Espace disque insuffisant."
                    )

            print("\nTéléchargement en cours...\n")

            with temporary.open("wb") as file:

                for chunk in response.iter_content(
                    chunk_size=CHUNK_SIZE
                ):
                    if not chunk:
                        continue

                    if downloaded == 0:
                        if chunk[:2] != b"\x1f\x8b":
                            raise RuntimeError(
                                "Signature GZIP invalide."
                            )

                    remaining = shutil.disk_usage(
                        BRONZE
                    ).free

                    if remaining < MIN_FREE_SPACE + len(chunk):
                        raise RuntimeError(
                            "Réserve disque atteinte."
                        )

                    file.write(chunk)
                    checksum.update(chunk)
                    downloaded += len(chunk)

                    current_progress = downloaded // (
                        100 * 1024**2
                    )

                    if current_progress > last_progress:
                        print(
                            f"Téléchargé : "
                            f"{downloaded / 1024**3:.2f} Gio"
                        )
                        last_progress = current_progress

            if expected_size is not None:
                if downloaded != expected_size:
                    raise RuntimeError(
                        "Taille du fichier incorrecte."
                    )

        # Validation initiale réussie
        temporary.replace(destination)

        metadata = {
            "filename": filename,
            "source": url,
            "month": month,
            "size_bytes": downloaded,
            "sha256": checksum.hexdigest(),
            "downloaded_at": datetime.now(
                timezone.utc
            ).isoformat()
        }

        manifest.write_text(
            json.dumps(metadata, indent=4),
            encoding="utf-8"
        )

        print("\nTÉLÉCHARGEMENT TERMINÉ !")
        print(f"Fichier : {destination}")
        print(f"Taille : {downloaded / 1024**3:.2f} Gio")
        print(f"SHA256 : {checksum.hexdigest()}")

    except Exception:
        temporary.unlink(missing_ok=True)
        raise


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "month",
        choices=MONTHS
    )

    args = parser.parse_args()
    download_month(args.month)
