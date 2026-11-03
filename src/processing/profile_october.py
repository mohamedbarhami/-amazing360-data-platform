
from pathlib import Path
import duckdb

# Racine du projet Amazing 360
ROOT = Path(__file__).resolve().parents[2]

FILE_PATH = ROOT / "data" / "bronze" / "2019-Oct.csv.gz"

SAMPLE_SIZE = 100_000


def profile_data():

    print("=" * 55)
    print("AMAZING 360 - DATA PROFILING")
    print("=" * 55)

    # Vérifier l'existence du fichier source
    if not FILE_PATH.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {FILE_PATH}"
        )

    print(f"\nSource : {FILE_PATH.name}")
    print("Moteur : DuckDB")

    # Connexion DuckDB
    con = duckdb.connect()

    # Configuration adaptée au PC
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 2")

    print("\nLecture de l'échantillon...")

    # Lire les premières 100 000 lignes
    # Sans charger tout le CSV dans un DataFrame
    con.execute(f"""
        CREATE TEMP TABLE sample AS
        SELECT *
        FROM read_csv(
            ?,
            header = true,
            compression = 'gzip',
            all_varchar = true
        )
        LIMIT {SAMPLE_SIZE}
    """, [str(FILE_PATH)])

    # 1. Nombre de lignes
    total = con.execute(
        "SELECT COUNT(*) FROM sample"
    ).fetchone()[0]

    print(f"\nNombre d'événements : {total:,}")

    # 2. Structure et types
    print("\n=== COLONNES ===")

    columns = con.execute(
        "PRAGMA table_info('sample')"
    ).fetchall()

    for col in columns:
        print(f"{col[1]} : {col[2]}")

    # 3. Types d'événements
    print("\n=== TYPES D'EVENEMENTS ===")

    events = con.execute("""
        SELECT
            event_type,
            COUNT(*) AS total
        FROM sample
        GROUP BY event_type
        ORDER BY total DESC
    """).fetchall()

    for event_type, count in events:
        print(f"{event_type} : {count:,}")

    # 4. Nombre de clients différents
    clients = con.execute("""
        SELECT COUNT(DISTINCT user_id)
        FROM sample
    """).fetchone()[0]

    print(f"\nClients distincts : {clients:,}")

    # 5. Valeurs manquantes des identifiants
    missing = con.execute("""
        SELECT COUNT(*)
        FROM sample
        WHERE user_id IS NULL
           OR TRIM(user_id) = ''
    """).fetchone()[0]

    print(f"Identifiants clients manquants : {missing:,}")

    # 6. Aperçu des données
    print("\n=== APERCU DES 5 PREMIERES LIGNES ===")

    preview = con.execute("""
        SELECT
            event_time,
            event_type,
            product_id,
            category_code,
            price
        FROM sample
        LIMIT 5
    """).fetchall()

    for row in preview:
        print(row)

    con.close()

    print("\nDATA PROFILING TERMINE !")


if __name__ == "__main__":
    profile_data()
