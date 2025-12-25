"""
shopstream_daily_pipeline.py
DAG Airflow pour le pipeline quotidien ShopStream

Ce DAG orchestre :
1. Extraction PostgreSQL vers S3
2. Chargement S3 vers Snowflake Staging
3. Transformations dbt (staging vers core vers marts)
4. Tests de qualité dbt
"""
from datetime import datetime, timedelta
import subprocess
import sys
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# We do NOT import SnowflakeOperator directly to avoid ImportError when provider is missing.
# We'll attempt to use SnowflakeHook if available; otherwise fall back to snowflake-connector-python.

# For retrieving connection info if SnowflakeHook is not available:
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)


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
    schedule='0 2 * * *',  # <-- changed from schedule_interval to schedule (Airflow v2.2+/3.x)
    catchup=False,
    tags=['production', 'daily', 'shopstream']
)

# --- Utility to run SQL on Snowflake ---
def run_sql_on_snowflake(sql, snowflake_conn_id='snowflake_default'):
    """
    Attempt to run SQL against Snowflake.
    Tries SnowflakeHook first (preferred). If not available, tries snowflake-connector-python
    using connection information retrieved from Airflow BaseHook.
    """
    # Try Hook first
    try:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        log.info("Using SnowflakeHook to run SQL")
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        hook.run(sql)
        return
    except Exception as e:
        log.info("SnowflakeHook not available or failed: %s", e)

    # Fallback to snowflake-connector-python
    try:
        import snowflake.connector
    except Exception as e:
        raise RuntimeError(
            "Neither SnowflakeHook nor snowflake-connector-python are available in the runtime. "
            "Install apache-airflow-providers-snowflake in your Airflow image or install "
            "snowflake-connector-python. Original error: %s" % e
        )

    # Get connection info from Airflow connection
    conn = BaseHook.get_connection(snowflake_conn_id)
    extra = conn.extra_dejson or {}

    account = extra.get('account') or conn.host
    user = conn.login
    password = conn.password
    warehouse = extra.get('warehouse')
    database = extra.get('database') or conn.schema
    role = extra.get('role')
    schema = conn.schema or extra.get('schema')

    conn_kwargs = {
        'user': user,
        'password': password,
        'account': account,
    }
    if warehouse:
        conn_kwargs['warehouse'] = warehouse
    if database:
        conn_kwargs['database'] = database
    if schema:
        conn_kwargs['schema'] = schema
    if role:
        conn_kwargs['role'] = role

    log.info("Connecting to Snowflake with account=%s user=%s warehouse=%s database=%s schema=%s",
             account, user, warehouse, database, schema)

    sf_conn = snowflake.connector.connect(**conn_kwargs)
    try:
        cur = sf_conn.cursor()
        # Split SQL into statements and run them sequentially
        statements = [s.strip() for s in sql.strip().split(';') if s.strip()]
        for stmt in statements:
            log.debug("Executing Snowflake statement: %s", stmt[:200])
            cur.execute(stmt)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            sf_conn.close()
        except Exception:
            pass

# --- Python callable tasks ---

def extract_postgres_to_s3(**context):
    """Extrait les données PostgreSQL et les pousse dans S3"""
    execution_date = context.get('ds')  # Date d'exécution (YYYY-MM-DD)
    log.info("Extraction PostgreSQL vers S3 pour %s", execution_date)

    # Use the same Python interpreter running Airflow inside container
    # new
    script_path = '/opt/airflow/scripts/export_to_s3.py'
    cmd = [sys.executable, script_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    log.info("stdout: %s", proc.stdout)
    if proc.returncode != 0:
        log.error("stderr: %s", proc.stderr)
        raise RuntimeError(f"Erreur lors de l'export (code {proc.returncode})")

    log.info("Export terminé avec succès")

def run_dbt_models(**context):
    """Exécute les transformations dbt"""
    log.info("Exécution des modèles dbt")
    project_dir = '/opt/airflow/shopstream_dbt'  # adjust/mount this path into container
    cmd = ['dbt', 'run', '--project-dir', project_dir]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    log.info("dbt run stdout: %s", proc.stdout)
    if proc.returncode != 0:
        log.error("dbt run stderr: %s", proc.stderr)
        raise RuntimeError("Erreur dbt run")
    log.info("Transformations dbt terminées")

def run_dbt_tests(**context):
    """Exécute les tests de qualité dbt"""
    log.info("Exécution des tests dbt")
    project_dir = '/opt/airflow/shopstream_dbt'  # adjust/mount this path into container
    cmd = ['dbt', 'test', '--project-dir', project_dir]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    log.info("dbt test stdout: %s", proc.stdout)
    if proc.returncode != 0:
        log.warning("Certains tests dbt ont échoué: %s", proc.stderr)
    log.info("Tests dbt terminés")

def send_success_notification(**context):
    """Envoie une notification de succès"""
    log.info("Pipeline terminé avec succès")
    log.info("Execution date : %s", context.get('ds'))
    # Add webhook or notification logic here

# Load staging - using PythonOperator that calls run_sql_on_snowflake

def load_staging_users_callable(**context):
    ds = context.get('ds')
    sql = f"""
    USE WAREHOUSE LOADING_WH;
    USE SCHEMA SHOPSTREAM_DWH.STAGING;
    TRUNCATE TABLE stg_users;
    COPY INTO stg_users (id, email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
    FROM @RAW.s3_raw_stage/postgres/users/{ds}/
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """
    run_sql_on_snowflake(sql)

def load_staging_orders_callable(**context):
    ds = context.get('ds')
    sql = f"""
    USE WAREHOUSE LOADING_WH;
    USE SCHEMA SHOPSTREAM_DWH.STAGING;
    TRUNCATE TABLE stg_orders;
    COPY INTO stg_orders (id, user_id, created_at, total_amount, status, country, payment_method)
    FROM @RAW.s3_raw_stage/postgres/orders/{ds}/
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE';
    """
    run_sql_on_snowflake(sql)

# --- Définition des tâches ---

task_extract = PythonOperator(
    task_id='extract_postgres_to_s3',
    python_callable=extract_postgres_to_s3,
    dag=dag
)

task_wait_s3_users = S3KeySensor(
    task_id='wait_s3_users_file',
    bucket_name='shopstream-datalake-yassir',  # consider parameterizing via Variables
    bucket_key='raw/postgres/users/{{ ds }}/users_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

task_wait_s3_orders = S3KeySensor(
    task_id='wait_s3_orders_file',
    bucket_name='shopstream-datalake-yassir',
    bucket_key='raw/postgres/orders/{{ ds }}/orders_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

task_load_staging_users = PythonOperator(
    task_id='load_staging_users',
    python_callable=load_staging_users_callable,
    dag=dag
)

task_load_staging_orders = PythonOperator(
    task_id='load_staging_orders',
    python_callable=load_staging_orders_callable,
    dag=dag
)

task_dbt_run = PythonOperator(
    task_id='dbt_run_models',
    python_callable=run_dbt_models,
    dag=dag
)

task_dbt_test = PythonOperator(
    task_id='dbt_test_models',
    python_callable=run_dbt_tests,
    dag=dag
)

task_dbt_docs = BashOperator(
    task_id='dbt_generate_docs',
    bash_command='cd /opt/airflow/shopstream_dbt && dbt docs generate',  # adjust path
    dag=dag
)

task_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Définition des dépendances (DAG)
task_extract >> [task_wait_s3_users, task_wait_s3_orders]
task_wait_s3_users >> task_load_staging_users
task_wait_s3_orders >> task_load_staging_orders
[task_load_staging_users, task_load_staging_orders] >> task_dbt_run
task_dbt_run >> task_dbt_test
task_dbt_test >> task_dbt_docs
task_dbt_docs >> task_success