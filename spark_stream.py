import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace créé avec succès")
    except Exception as e:
        logging.error(f"Erreur lors de la création du keyspace: {e}")
        raise

def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
        """)
        logging.info("Table créée avec succès")
    except Exception as e:
        logging.error(f"Erreur lors de la création de la table: {e}")
        raise

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("KafkaCassandraIntegration") \
            .config("spark.jars.packages", 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logging.info("Connexion Spark établie")
        return spark
    except Exception as e:
        logging.error(f"Échec de la connexion Spark: {e}")
        return None

def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logging.info("Connexion Kafka réussie")
        return df
    except Exception as e:
        logging.error(f"Échec de la connexion Kafka: {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        session = cluster.connect()
        logging.info("Connexion Cassandra établie")
        return session
    except Exception as e:
        logging.error(f"Échec de la connexion Cassandra: {e}")
        return None

def create_selection_df(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    return spark_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

if __name__ == "__main__":
    # 1. Connexion Spark
    spark_conn = create_spark_connection()
    if not spark_conn:
        exit(1)

    # 2. Connexion Kafka
    spark_df = connect_to_kafka(spark_conn)
    if not spark_df:
        exit(1)

    # 3. Préparation des données
    selection_df = create_selection_df(spark_df)
    
    # 4. Connexion Cassandra
    cass_session = create_cassandra_connection()
    if not cass_session:
        exit(1)

    # 5. Initialisation de la base
    try:
        create_keyspace(cass_session)
        create_table(cass_session)
    except Exception as e:
        logging.error(f"Initialisation Cassandra échouée: {e}")
        exit(1)

    # 6. Écriture du stream
    logging.info("Démarrage du traitement stream...")
    query = selection_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .option("keyspace", "spark_streams") \
        .option("table", "created_users") \
        .start()

    query.awaitTermination()