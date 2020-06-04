import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, row_number, desc
from pyspark.sql.window import Window

# SETUP FROM GLUE BOILER PLATE
# Pull in job name to execute later
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Used in glue job auto create:
sc = SparkContext()

# From Paul and Jon:
# sc = SparkContext.getOrCreate()

# Create glue context
glueContext = GlueContext(sc)

# May not be necessary
spark = glueContext.spark_session

# Create job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Get dynamic frame from catalog table
# Used in glue job auto create:
## @type: DataSource
## @args: [database = "cdc-demo-database", table_name = "raw_events", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
df = glueContext.create_dynamic_frame.from_catalog(
        database = "cdc-demo-database",
        table_name = "raw_events",
        transformation_ctx = "datasource0")

# create_dynamic_frame.from_path instead of from catalog to go straight from the bucket
# skip the crawl step and go straight to raw bucket
# will be helpful if we have all 7 tables pushing into one bucket

# From Paul and Jon
# df = glueContext.create_dynamic_frame_from_catalog(
#         database='cdc-demo-database',
#         table_name='raw_events',
#         transformation_ctx='source')

# OUR SCRIPT
# Isolate payload
df = df.select_fields(['payload'])

# Flatten structure
df = df.unnest()

# Convert to pyspark DF
df = df.toDF()

# replace . with _ in DF
new_column_name_list= list(map(lambda x: x.replace(".", "_"), df.columns))
df = df.toDF(*new_column_name_list)

# Add new payload column True or False depending if after is null or not
df = df.withColumn("payload_delete", when(col("payload_after_ID").isNull(), 'True').otherwise('False'))

# Copy over payload ID to after, if after ID is null (for later deletion)
df = df.withColumn("payload_after_ID", when(col("payload_after_ID").isNull(), col("payload_before_ID")).otherwise(col("payload_after_ID")))

# Isolate important data
df = df.select(['payload_before_ID', 'payload_before_NAME', 'payload_after_ID', 'payload_after_NAME', 'payload_delete', 'payload_source_ts_ms'])

# Group by payload ID and order by time stamp
window = Window.partitionBy('payload_after_ID').orderBy(desc('payload_source_ts_ms'))

# Select the most recent within a group ID
# After_ID | Delete
# 1        | True (deleted)
# 1        | False (created) << will be removed
# 2        | False (updated)
# 2        | False (created) << will be removed
df = df.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

# Filter out where payload delete is true
# After_ID | Delete
# 1        | True (deleted) << will be removed
# 2        | False (updated)
df = df.filter(col("payload_delete") == "False")

# Isolate important data
df = df.select(['payload_after_ID', 'payload_after_NAME'])



####~~!!!-- Need to work out saving the above via process copied from generated AWS Script:
## @type: ApplyMapping
## @args: [mapping = [("payload.before.ID", "string", "`before.ID`", "string"), ("payload.before.NAME", "string", "`before.NAME`", "string"), ("payload.after.ID", "int", "`after.ID`", "int"), ("payload.after.NAME", "string", "`after.NAME`", "string"), ("payload.source.db", "string", "`source.db`", "string"), ("payload.source.schema", "string", "`source.schema`", "string"), ("payload.source.table", "string", "`source.table`", "string"), ("payload.ts_ms", "long", "ts_ms", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("payload.before.NAME", "string", "`before.NAME`", "string"), ("payload.before.ID", "string", "`before.ID`", "string"), ("payload.after.ID", "int", "`after.ID`", "int"), ("payload.after.NAME", "string", "`after.NAME`", "string"), ("payload.source.db", "string", "`source.db`", "string"), ("payload.source.schema", "string", "`source.schema`", "string"), ("payload.source.table", "string", "`source.table`", "string"), ("payload.ts_ms", "long", "ts_ms", "long")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://database.db2inst1.example-table"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]

datasink2 = glueContext.write_dynamic_frame.from_options(
  frame = df,
  connection_type = "s3",
  connection_options = {"path": "s3://database.db2inst1.example-table"},
  format = "csv",
  transformation_ctx = "datasink2")

job.commit()