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

# Lire directement depuis S3 en JSON (plus fiable que via Data Catalog)
customer_landing_df = spark.read.json("s3://stedi-lakehouse-vl/customer/landing/")

# Créer une vue temporaire
customer_landing_df.createOrReplaceTempView("customer_landing")

# Filtrer via SQL : ne garder que les clients ayant consenti
customer_trusted_df = spark.sql("""
    SELECT * FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
    AND sharewithresearchasofdate > 0
""")

# Reconvertir en DynamicFrame
customer_trusted = DynamicFrame.fromDF(
    customer_trusted_df,
    glueContext,
    "customer_trusted"
)

# Écrire dans la Trusted Zone sur S3 et mettre à jour le Data Catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-lakehouse-vl/customer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_trusted"
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(customer_trusted)

job.commit()
