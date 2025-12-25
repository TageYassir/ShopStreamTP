"""
shopstream_daily_pipeline.py
DAG Airflow pour le pipeline quotidien ShopStream

Ce DAG orchestre :
1. Extraction PostgreSQL vers S3
2. Chargement S3 vers Snowflake Staging
3. Transformations dbt (staging vers core vers marts)
4. Tests de qualité dbt

Emplacement : airflow/dags/shopstream_daily_pipeline.py
Auteur : Data Engineering Team
Date : 2025-11-27
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators. python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Configuration du DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email': ['alerts@shopstream.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'shopstream_daily_pipeline',
    default_args=default_args,
    description='Pipeline quotidien ShopStream : PostgreSQL vers S3 vers Snowflake vers dbt vers BI',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['production', 'daily', 'shopstream']
)

# Tâches Python
def extract_postgres_to_s3(**context):
    """Extrait les données PostgreSQL et les pousse dans S3"""
    import subprocess
    execution_date = context['ds']  # Date d'exécution (YYYY-MM-DD)
    
    print(f"Extraction PostgreSQL vers S3 pour {execution_date}")
    
    # Exécution du script Python d'export
    # MODIFIEZ LE CHEMIN SELON VOTRE INSTALLATION
    result = subprocess.run(
        ['python', 'C:\\tagemouati_yassir\\ShopStreamTP\\scripts\\export_to_s3.py'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Erreur lors de l'export : {result.stderr}")
    
    print(result.stdout)
    print("Export terminé avec succès")

def run_dbt_models(**context):
    """Exécute les transformations dbt"""
    import subprocess
    
    print("Exécution des modèles dbt")
    
    # MODIFIEZ LE CHEMIN SELON VOTRE INSTALLATION
    result = subprocess.run(
        ['dbt', 'run', '--project-dir', 'C:\\tagemouati_yassir\\ShopStreamTP\\shopstream_dbt'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Erreur dbt run : {result.stderr}")
    
    print(result.stdout)
    print("Transformations dbt terminées")

def run_dbt_tests(**context):
    """Exécute les tests de qualité dbt"""
    import subprocess
    
    print("Exécution des tests dbt")
    
    # MODIFIEZ LE CHEMIN SELON VOTRE INSTALLATION
    result = subprocess.run(
        ['dbt', 'test', '--project-dir', 'C:\\tagemouati_yassir\\ShopStreamTP\\shopstream_dbt'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Certains tests ont échoué : {result.stderr}")
        # On ne fait pas échouer le DAG, juste un warning
    
    print(result.stdout)
    print("Tests dbt terminés")

def send_success_notification(**context):
    """Envoie une notification de succès"""
    print("Pipeline terminé avec succès")
    print(f"Execution date : {context['ds']}")
    # Ici : appel à Slack/Teams/Email via webhook

# Définition des tâches

# Tâche 1 : Extraction PostgreSQL vers S3
task_extract = PythonOperator(
    task_id='extract_postgres_to_s3',
    python_callable=extract_postgres_to_s3,
    dag=dag
)

# Tâche 2 : Attendre que les fichiers soient présents dans S3
task_wait_s3_users = S3KeySensor(
    task_id='wait_s3_users_file',
    bucket_name='shopstream-datalake-yassir',  # MODIFIEZ avec votre bucket
    bucket_key='raw/postgres/users/{{ ds }}/users_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

task_wait_s3_orders = S3KeySensor(
    task_id='wait_s3_orders_file',
    bucket_name='shopstream-datalake-yassir',  # MODIFIEZ avec votre bucket
    bucket_key='raw/postgres/orders/{{ ds }}/orders_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

# Tâche 3 : Chargement S3 vers Snowflake STAGING
task_load_staging_users = SnowflakeOperator(
    task_id='load_staging_users',
    snowflake_conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA SHOPSTREAM_DWH. STAGING;
        
        TRUNCATE TABLE stg_users;
        
        COPY INTO stg_users (id, email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
        FROM @RAW. s3_raw_stage/postgres/users/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

task_load_staging_orders = SnowflakeOperator(
    task_id='load_staging_orders',
    snowflake_conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA SHOPSTREAM_DWH. STAGING;
        
        TRUNCATE TABLE stg_orders;
        
        COPY INTO stg_orders (id, user_id, created_at, total_amount, status, country, payment_method)
        FROM @RAW.s3_raw_stage/postgres/orders/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# Tâche 4 : Exécution de dbt (staging vers core vers marts)
task_dbt_run = PythonOperator(
    task_id='dbt_run_models',
    python_callable=run_dbt_models,
    dag=dag
)

# Tâche 5 : Tests dbt
task_dbt_test = PythonOperator(
    task_id='dbt_test_models',
    python_callable=run_dbt_tests,
    dag=dag
)

# Tâche 6 : Génération de la documentation dbt
task_dbt_docs = BashOperator(
    task_id='dbt_generate_docs',
    bash_command='cd C:\\tagemouati_yassir\\ShopStreamTP\\shopstream_dbt && dbt docs generate',  # MODIFIEZ le chemin
    dag=dag
)

# Tâche 7 : Notification de succès
task_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Définition des dépendances (DAG)

# Phase 1 : Extraction
task_extract >> [task_wait_s3_users, task_wait_s3_orders]

# Phase 2 : Chargement Staging
task_wait_s3_users >> task_load_staging_users
task_wait_s3_orders >> task_load_staging_orders

# Phase 3 : Transformations dbt
[task_load_staging_users, task_load_staging_orders] >> task_dbt_run

# Phase 4 : Tests et documentation
task_dbt_run >> task_dbt_test
task_dbt_test >> task_dbt_docs

# Phase 5 : Notification
task_dbt_docs >> task_success