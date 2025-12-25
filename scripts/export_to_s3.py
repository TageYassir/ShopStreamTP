"""
export_to_s3.py
Exporte les tables PostgreSQL vers S3 au format CSV
Emplacement : ShopStreamTP/scripts/export_to_s3.py
"""

import os
from datetime import datetime
import psycopg2
import pandas as pd
import boto3
from io import StringIO

# CONFIGURATION A MODIFIER
DB_CONFIG = {
    'host': 'localhost',
    'database': 'shopstream',
    'user': 'postgres',
    'password': '1234'  # VOTRE mot de passe PostgreSQL
}

S3_CONFIG = {
    'bucket': 'shopstream-datalake-yassir',  # VOTRE nom de bucket S3
    'region': 'eu-north-1'
}

# Tables à exporter
TABLES = ['users', 'products', 'orders', 'order_items', 'crm_contacts']


def get_db_connection():
    """Connexion PostgreSQL"""
    print("Connexion à PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connexion PostgreSQL réussie")
        return conn
    except Exception as e:
        print(f"Erreur de connexion PostgreSQL : {e}")
        exit(1)


def get_s3_client():
    """Client S3"""
    print("Connexion à AWS S3...")
    try:
        s3_client = boto3.client('s3', region_name=S3_CONFIG['region'])
        # Test de connexion
        s3_client.head_bucket(Bucket=S3_CONFIG['bucket'])
        print("Connexion S3 réussie")
        return s3_client
    except Exception as e:
        print(f"Erreur de connexion S3 : {e}")
        print("Vérifiez votre nom de bucket et vos credentials AWS")
        exit(1)


def export_table_to_s3(table_name, date_partition):
    """
    Exporte une table PostgreSQL vers S3 au format CSV

    Args:
        table_name: Nom de la table PostgreSQL
        date_partition: Date de partition (format YYYY-MM-DD)
    """
    print(f"\nExport de la table '{table_name}'...")

    # Connexion DB
    conn = get_db_connection()

    # Lecture de la table dans un DataFrame Pandas
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql(query, conn)
        print(f"   {len(df)} lignes extraites de PostgreSQL")
    except Exception as e:
        print(f"   Erreur lecture table : {e}")
        conn.close()
        return

    conn.close()

    if df.empty:
        print(f"   Table '{table_name}' vide, pas d'export")
        return

    # Conversion en CSV (en mémoire)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Chemin S3
    date_folder = date_partition
    s3_key = f"raw/postgres/{table_name}/{date_folder}/{table_name}_{date_partition.replace('-', '')}.csv"

    # Upload vers S3
    s3_client = get_s3_client()
    try:
        s3_client.put_object(
            Bucket=S3_CONFIG['bucket'],
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"   Uploadé vers s3://{S3_CONFIG['bucket']}/{s3_key}")
    except Exception as e:
        print(f"   Erreur upload S3 : {e}")


def export_events_to_s3(date_partition):
    """Exporte les événements au format JSON"""
    print(f"\nExport de la table 'events'...")

    conn = get_db_connection()

    query = "SELECT id, user_id, event_type, event_ts, metadata FROM events"
    try:
        df = pd.read_sql(query, conn)
        print(f"   {len(df)} lignes extraites de PostgreSQL")
    except Exception as e:
        print(f"   Erreur lecture events : {e}")
        conn.close()
        return

    conn.close()

    if df.empty:
        print(f"   Table 'events' vide, pas d'export")
        return

    # Conversion en JSON
    json_data = df.to_json(orient='records', date_format='iso')

    # Chemin S3
    s3_key = f"raw/events/{date_partition}/events_{date_partition.replace('-', '')}.json"

    # Upload
    s3_client = get_s3_client()
    try:
        s3_client.put_object(
            Bucket=S3_CONFIG['bucket'],
            Key=s3_key,
            Body=json_data
        )
        print(f"   Uploadé vers s3://{S3_CONFIG['bucket']}/{s3_key}")
    except Exception as e:
        print(f"   Erreur upload S3 : {e}")


def main():
    """Point d'entrée principal"""
    print("=" * 70)
    print("EXPORT POSTGRESQL vers S3")
    print("=" * 70)

    # Date du jour (ou passée en argument)
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\nDate de partition : {today}")

    try:
        # Export des tables relationnelles
        for table in TABLES:
            export_table_to_s3(table, today)

        # Export des événements (JSON)
        export_events_to_s3(today)

        print("\n" + "=" * 70)
        print("EXPORT TERMINE AVEC SUCCES")
        print("=" * 70)
        print(f"\nVérifiez dans S3 : https://s3.console.aws. amazon.com/s3/buckets/{S3_CONFIG['bucket']}")

    except Exception as e:
        print(f"\nERREUR GLOBALE : {e}")


if __name__ == "__main__":
    main()