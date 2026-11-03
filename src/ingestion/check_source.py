
import requests

URL = (
    "https://data.rees46.com/datasets/"
    "marketplace/2019-Oct.csv.gz"
)


def check_source():
    print("=== AMAZING 360 ===")
    print("Verification de la source REES46")
    print("-" * 40)

    try:
        # Connexion sans télécharger le gros fichier
        with requests.get(
            URL,
            stream=True,
            timeout=(15, 30)
        ) as response:

            print("Statut HTTP :", response.status_code)
            print("URL finale :", response.url)
            print(
                "Content-Type :",
                response.headers.get("Content-Type")
            )

            response.raise_for_status()

            # Lire uniquement la signature GZIP
            signature = response.raw.read(
                2, decode_content=False
            )

            if signature == b"\x1f\x8b":
                print("SUCCESS : fichier GZIP accessible !")
            else:
                print("ATTENTION : contenu non reconnu comme GZIP")

    except requests.RequestException as error:
        print("ERREUR :", error)


if __name__ == "__main__":
    check_source()
