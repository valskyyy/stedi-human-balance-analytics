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
step_trainer_trusted_df = spark.read.parquet("s3://stedi-lakehouse-vl/step_trainer/trusted/")
accelerometer_trusted_df = spark.read.parquet("s3://stedi-lakehouse-vl/accelerometer/trusted/")

# Créer des vues temporaires
step_trainer_trusted_df.createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_df.createOrReplaceTempView("accelerometer_trusted")

# Joindre sur timestamp = sensorreadingtime
machine_learning_curated_df = spark.sql("""
    SELECT s.sensorreadingtime, s.serialnumber, s.distancefromobject,
           a.user, a.x, a.y, a.z
    FROM step_trainer_trusted s
    INNER JOIN accelerometer_trusted a ON s.sensorreadingtime = a.timestamp
""")

# Reconvertir en DynamicFrame
machine_learning_curated = DynamicFrame.fromDF(
    machine_learning_curated_df,
    glueContext,
    "machine_learning_curated"
)

# Écrire dans la Curated Zone sur S3 et mettre à jour le Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-vl/machine_learning/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated"
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(machine_learning_curated)

job.commit()
