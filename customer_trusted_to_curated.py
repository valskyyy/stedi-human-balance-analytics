import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lire directement depuis S3
customer_trusted_df = spark.read.parquet("s3://stedi-lakehouse-vl/customer/trusted/")
accelerometer_trusted_df = spark.read.parquet("s3://stedi-lakehouse-vl/accelerometer/trusted/")

# Créer des vues temporaires
customer_trusted_df.createOrReplaceTempView("customer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")

# Inner join sur email - garder uniquement les colonnes customer
# DISTINCT pour éviter les doublons si un customer a plusieurs lectures
customer_curated_df = spark.sql("""
    SELECT DISTINCT c.*
    FROM customer_trusted c
    INNER JOIN accelerometer_trusted a ON c.email = a.user
""")

# Reconvertir en DynamicFrame
customer_curated = DynamicFrame.fromDF(
    customer_curated_df,
    glueContext,
    "customer_curated"
)

# Écrire dans la Curated Zone sur S3 et mettre à jour le Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-vl/customer/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_curated"
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(customer_curated)

job.commit()
