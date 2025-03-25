import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# Configuration améliorée du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def create_spark_connection():
    """
    Crée et retourne une session Spark avec une configuration robuste.
    """
    try:
        spark = SparkSession.builder \
            .appName("KafkaCassandraIntegration") \
            .master("local[*]") \
            .config("spark.jars.packages", 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "300s") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Connexion Spark établie avec succès")
        return spark
    except Exception as e:
        logger.error(f"Échec de la création de la session Spark: {e}", exc_info=True)
        return None

class SpringBootConnector:
    """Gère la connexion à l'API Spring Boot avec gestion des erreurs améliorée"""
    def __init__(self, spring_boot_url: str):
        self.spring_boot_url = spring_boot_url
        self.headers = {'Content-Type': 'application/json'}
        self.session = requests.Session()
        self.timeout = 30  # timeout en secondes
    
    def send_transactions(self, transactions: List[Dict[str, Any]]) -> bool:
        """Envoie les transactions à Spring Boot avec gestion robuste des erreurs"""
        if not transactions:
            logger.warning("Aucune transaction à envoyer")
            return False
            
        try:
            # Conversion des dates en format ISO
            formatted_transactions = []
            for transaction in transactions:
                formatted = {
                    k: v.isoformat() if isinstance(v, datetime) else v
                    for k, v in transaction.items()
                }
                formatted_transactions.append(formatted)
            
            response = self.session.post(
                self.spring_boot_url,
                json=formatted_transactions,
                headers=self.headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            logger.info(f"Successfully sent {len(transactions)} transactions")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur de requête vers l'API Spring Boot: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'envoi des transactions: {str(e)}", exc_info=True)
            return False

def get_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    """Crée un stream Kafka avec gestion des erreurs"""
    try:
        logger.info(f"Connexion à Kafka: {bootstrap_servers}, topic: {topic}")
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
    except Exception as e:
        logger.error(f"Échec de création du stream Kafka: {str(e)}", exc_info=True)
        raise

def get_transaction_schema() -> StructType:
    """Définit le schéma des transactions"""
    return StructType([
        StructField("transactionid", StringType(), False),  # Changed from transaction_id
        StructField("accountid", StringType(), False),      # Changed from account_id
        StructField("transactionamount", DoubleType(), False),
        StructField("transactiondate", TimestampType(), False),
        StructField("transactiontype", StringType(), False),
        StructField("location", StringType(), False),
        StructField("deviceid", StringType(), False),
        StructField("ipaddress", StringType(), False),
        StructField("merchantid", StringType(), False),
        StructField("accountbalance", DoubleType(), False),
        StructField("previoustransactiondate", TimestampType(), False),
        StructField("channel", StringType(), False),
        StructField("customerage", IntegerType(), False),
        StructField("customeroccupation", StringType(), False),
        StructField("transactionduration", IntegerType(), False),
        StructField("loginattempts", IntegerType(), False),
        StructField("firstname", StringType(), False),
        StructField("lastname", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("picture", StringType(), False)
    ])
def process_stream(kafka_df, schema: StructType, spring_connector: SpringBootConnector):
    """Traite le stream et envoie les données avec gestion des erreurs"""
    transactions_df = kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    def process_batch(df, batch_id):
        try:
            if df.rdd.isEmpty():
                logger.info("Batch vide reçu")
                return
                
            records = [row.asDict() for row in df.collect()]
            logger.info(f"Traitement du batch {batch_id} avec {len(records)} transactions")
            
            if not spring_connector.send_transactions(records):
                logger.warning(f"Échec partiel pour le batch {batch_id}")
                
        except Exception as e:
            logger.error(f"Erreur lors du traitement du batch {batch_id}: {str(e)}", exc_info=True)
    
    return transactions_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

def main():
    """Point d'entrée principal avec gestion améliorée des erreurs"""
    # Configuration
    KAFKA_CONFIG = {
        "bootstrap_servers": "localhost:9092",
        "topic": "bank_transactions"
    }
    
    SPRING_CONFIG = {
        "url": "http://localhost:8087/api/transactions"  # For POSTing transactions
    }
    
    spark = None
    try:
        # Vérification de l'API Spring Boot avant de démarrer
        try:
            # Use direct actuator health endpoint instead of modifying transactions URL
            health_url = "http://localhost:8087/actuator/health"
            test_response = requests.get(health_url, timeout=5)
            if test_response.status_code != 200:
                logger.error("Spring Boot API n'est pas disponible")
                return
        except Exception as e:
            logger.error(f"Impossible de se connecter à Spring Boot: {str(e)}")
            return

        # Rest of your main() remains the same...
        spark = create_spark_connection()
        if not spark:
            logger.error("Arrêt de l'application - Impossible de créer la session Spark")
            return
        
        spring_connector = SpringBootConnector(SPRING_CONFIG["url"])
        
        # Stream Kafka
        kafka_df = get_kafka_stream(spark, KAFKA_CONFIG["bootstrap_servers"], KAFKA_CONFIG["topic"])
        
        # Traitement
        query = process_stream(
            kafka_df,
            get_transaction_schema(),
            spring_connector
        )
        
        logger.info("Démarrage du traitement du stream...")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.error(f"Échec critique de l'application: {str(e)}", exc_info=True)
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Session Spark arrêtée avec succès")
            except Exception as e:
                logger.error(f"Erreur lors de l'arrêt de Spark: {str(e)}")
        logger.info("Application terminée")
if __name__ == "__main__":
    main()