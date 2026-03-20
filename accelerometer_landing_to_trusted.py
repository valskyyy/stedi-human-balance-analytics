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
accelerometer_landing_df = spark.read.json("s3://stedi-lakehouse-vl/accelerometer/landing/")
customer_trusted_df = spark.read.parquet("s3://stedi-lakehouse-vl/customer/trusted/")

# Créer des vues temporaires
accelerometer_landing_df.createOrReplaceTempView("accelerometer_landing")
customer_trusted_df.createOrReplaceTempView("customer_trusted")

# Inner join sur email/user - garder uniquement les colonnes accelerometer
accelerometer_trusted_df = spark.sql("""
    SELECT a.timestamp, a.user, a.x, a.y, a.z
    FROM accelerometer_landing a
    INNER JOIN customer_trusted c ON a.user = c.email
""")

# Reconvertir en DynamicFrame
accelerometer_trusted = DynamicFrame.fromDF(
    accelerometer_trusted_df,
    glueContext,
    "accelerometer_trusted"
)

# Écrire dans la Trusted Zone sur S3 et mettre à jour le Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-vl/accelerometer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="accelerometer_trusted"
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(accelerometer_trusted)

job.commit()
