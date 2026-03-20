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
step_trainer_landing_df = spark.read.json("s3://stedi-lakehouse-vl/step_trainer/landing/")
customer_curated_df = spark.read.parquet("s3://stedi-lakehouse-vl/customer/curated/")

# Créer des vues temporaires
step_trainer_landing_df.createOrReplaceTempView("step_trainer_landing")
customer_curated_df.createOrReplaceTempView("customer_curated")

# SELECT IN plutôt que JOIN pour éviter les doublons
# dus aux serialnumbers non-uniques
step_trainer_trusted_df = spark.sql("""
    SELECT DISTINCT s.*
    FROM step_trainer_landing s
    WHERE s.serialnumber IN (
        SELECT serialnumber FROM customer_curated
    )
""")

# Reconvertir en DynamicFrame
step_trainer_trusted = DynamicFrame.fromDF(
    step_trainer_trusted_df,
    glueContext,
    "step_trainer_trusted"
)

# Écrire dans la Trusted Zone sur S3 et mettre à jour le Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-vl/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="step_trainer_trusted"
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(step_trainer_trusted)

job.commit()
