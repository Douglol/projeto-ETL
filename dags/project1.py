from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession

AZURE_CONN_ID = "azure_blob_storage"
CONTAINER_NAME = "dgcont"
BLOB_NAME = "/opt/airflow/data/Churn.csv"
SYNAPSE_CONN_ID = "azure_synapse_connection"

# Credenciais do Azure Synapse
SYNAPSE_SERVER = "dgwork.sql.azuresynapse.net"
SYNAPSE_DATABASE = "dgpool"
SYNAPSE_USER = "sqladminuser"
SYNAPSE_PASSWORD = "!74@08Do"
SYNAPSE_TABLE = "ChurnTable"

# Função para upload do arquivo ao Azure Blob Storage
def upload_file_to_blob():
    hook = WasbHook(wasb_conn_id=AZURE_CONN_ID)
    blob_name = BLOB_NAME.split('/')[-1] # Usando apenas o nome do arquivo, não o caminho completo
    hook.load_file(BLOB_NAME, container_name=CONTAINER_NAME, blob_name=blob_name, overwrite=True)

# Função para processar e carregar dados no Azure Synapse
def process_and_load_to_synapse():

    spark = SparkSession.builder \
        .appName("TransformarDados") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1,com.microsoft.azure:azure-storage:8.6.5,com.microsoft.sqlserver:mssql-jdbc:10.2.0.jre8") \
        .getOrCreate()
    
    # Configurar credenciais do Azure Blob Storage
    spark.conf.set("fs.azure.account.key.dgsbxsstorage.blob.core.windows.net", "LrUrJGlQIkX7dca28q0xdbpT/onacDcHPzLj0LPUJmuMLqFH+bLAviX1gC5+1LVmum5D3+X+zV+y+ASt+Sh6hg==")

    file_path = f"wasbs://{CONTAINER_NAME}@dgsbxsstorage.blob.core.windows.net/Churn.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")

    transformed_df = df.select("X10", "X11")

    jdbc_url = f"jdbc:sqlserver://{SYNAPSE_SERVER}:1433;database={SYNAPSE_DATABASE};user={SYNAPSE_USER};password={SYNAPSE_PASSWORD};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

    transformed_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", SYNAPSE_TABLE) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("overwrite") \
        .save()

    spark.stop()

with DAG(
    dag_id="blob_to_synapse_pyspark",
    default_args={"owner": "airflow"},
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    upload_task = PythonOperator(
        task_id="upload_para_blob",
        python_callable=upload_file_to_blob,
    )

    transform_and_load_task = PythonOperator(
        task_id="transform_and_load_to_synapse",
        python_callable=process_and_load_to_synapse,
    )

    upload_task >> transform_and_load_task
