import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "cdc-demo-database", table_name = "raw_events", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "cdc-demo-database", table_name = "raw_events", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("payload.before.ID", "string", "`before.ID`", "string"), ("payload.before.NAME", "string", "`before.NAME`", "string"), ("payload.after.ID", "int", "`after.ID`", "int"), ("payload.after.NAME", "string", "`after.NAME`", "string"), ("payload.source.db", "string", "`source.db`", "string"), ("payload.source.schema", "string", "`source.schema`", "string"), ("payload.source.table", "string", "`source.table`", "string"), ("payload.ts_ms", "long", "ts_ms", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("payload.before.NAME", "string", "`before.NAME`", "string"), ("payload.before.ID", "string", "`before.ID`", "string"), ("payload.after.ID", "int", "`after.ID`", "int"), ("payload.after.NAME", "string", "`after.NAME`", "string"), ("payload.source.db", "string", "`source.db`", "string"), ("payload.source.schema", "string", "`source.schema`", "string"), ("payload.source.table", "string", "`source.table`", "string"), ("payload.ts_ms", "long", "ts_ms", "long")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://database.db2inst1.example-table"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://database.db2inst1.example-table"}, format = "csv", transformation_ctx = "datasink2")
job.commit()