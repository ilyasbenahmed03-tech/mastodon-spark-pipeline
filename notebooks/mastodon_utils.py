import pyspark
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "kafka:9092"      #lenom du service kafka dans docker-compose
TOPIC = "mastodon_stream"
DATA_DIR = "/home/jovyan/work/data"
CHECKPOINT_DIR = "/home/jovyan/checkpoints"
JDBC_URL = "jdbc:postgresql://postgres:5432/mastodon"
JDBC_PROPS = {"user": "spark", "password": "spark", "driver": "org.postgresql.Driver"}

def _packages():
    # le connecteur de Kafka pour Spark+driver JDBC PostgreSQL
    v = pyspark.__version__
    scala = "2.13" if v.startswith("4") else "2.12"
    return ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{v}",
        "org.postgresql:postgresql:42.7.4",
    ])

def get_spark(app_name, shuffle_partitions=8):
#SparkSession en local
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", _packages())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    port = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    print(f"Spark {spark.version} prêt - Spark UI : http://localhost:{port}")
    return spark

def read_table(spark, table):
 # lit une table PostgreSQL et la renvoie en DataFrame
 return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)

def write_table(df, table, mode="append"):
    #ecrit un DataFrame dans PostgreSQL
    #truncate=true: vide la table au lieu de la supprimer
    (df.write.mode(mode).option("truncate", "true")
       .jdbc(JDBC_URL, table, properties=JDBC_PROPS))