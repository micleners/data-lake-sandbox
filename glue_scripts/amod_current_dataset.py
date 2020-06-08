import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when, col, row_number, desc
from pyspark.sql.window import Window

log = logging.getLogger(__name__)

def __load_staging_data(args, glueContext):
    # # Replace with this approach by passing in parameters:
    # db_name = args["database_name"]
    # source_table = args["source_table"]
    # return glueContext.create_dynamic_frame_from_catalog(
    #     database=db_name,
    #     table_name=source_table,
    #     transformation_ctx="source")
    # Get dynamic frame from catalog table
    # Used in glue job auto create:
    ## @type: DataSource
    ## @args: [database = "cdc-demo-database", table_name = "raw_events", transformation_ctx = "datasource0"]
    ## @return: df
    ## @inputs: []
    return glueContext.create_dynamic_frame_from_catalog(
            database = "cdc-demo-database",
            table_name = "raw_events"
        )

def __reduce_data(data_source):
    # OUR SCRIPT
    # Isolate payload, flatten structure, and convert to pyspark dataframe
    df = data_source.select_fields(['payload']) \
        .unnest() \
        .toDF()

    # replace . with _ in DF
    new_column_name_list= list(map(lambda x: x.replace(".", "_"), df.columns))
    df = df.toDF(*new_column_name_list)

    # Add new payload column True or False depending if after is null or not
    df = df.withColumn("payload_delete", when(col("payload_after_ID").isNull(), 'True').otherwise('False'))

    # Copy over payload ID to after, if after ID is null (for later deletion)
    df = df.withColumn("payload_after_ID", when(col("payload_after_ID").isNull(), col("payload_before_ID")).otherwise(col("payload_after_ID")))

    # Isolate important data
    df = df.select(['payload_before_ID', 'payload_before_NAME', 'payload_after_ID', 'payload_after_NAME', 'payload_delete', 'payload_source_ts_ms'])

    # Specify window (grouping) by payload ID and order by time stamp
    window = Window.partitionBy('payload_after_ID').orderBy(desc('payload_source_ts_ms'))

    # Select the most recent within an ID group, and filter out the rest
    # After_ID | Delete
    # 1        | True (deleted)
    # 1        | False (created) << will be removed
    # 2        | False (updated)
    # 2        | False (created) << will be removed
    df = df.withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn") \

    # Filter out where payload delete is true
    # After_ID | Delete
    # 1        | True (deleted) << will be removed
    # 2        | False (updated)
    # (This command could potentially be chained to the above commands)
    df = df.filter(col("payload_delete") == "False")

    # Isolate important data, and repartition the dataframe so that output is in one file
    df = df.select(['payload_after_ID', 'payload_after_NAME']) \
            .repartition(1)

    return df


def __rename_and_store(args, glueContext, df):
    print('df coming into __rename_and_store')
    print(df.show())
    # Convert pyspark dataframe to AWS dynamic frame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, 'final_frame')

    # Apply mapping to rename columns
    apply_mapping = ApplyMapping.apply(frame = dynamic_frame, mappings = [("payload_after_ID", "int", "ID", "int"), ("payload_after_NAME", "string", "Name", "string")], transformation_ctx = "applymapping")

    # Write dynamic frame to CSV in S3 bucket
    print('apply_mapping')
    print(apply_mapping.show())
    data_sink = glueContext.write_dynamic_frame_from_options(
        frame = apply_mapping,
        connection_type = "s3",
        connection_options = {"path": "s3://amod-cdc-current-dataset-tested-bucket/"},
        format = "csv",
        transformation_ctx = "datasink"
    )


def main(argv, glueContext, job):
    # SETUP FROM GLUE BOILER PLATE
    # Pull in job name to execute later
    ## @params: [JOB_NAME]
    args = getResolvedOptions(argv, ['JOB_NAME'])

    job.init(args['JOB_NAME'], args)


    staging_df = __load_staging_data(args, glueContext)
    reduced_df = __reduce_data(staging_df)
    __rename_and_store(args, glueContext, reduced_df)
    # mapped_staging = __map_staging_data(staging_df)
    # unioned_data = __union_with_existing_data(mapped_staging, args, glueContext)
    # merged_result = __merge_rows(unioned_data)

    # __repartition_and_store(merged_result, args, glueContext)

    # This may be unnecessary since we are not using job bookmarking
    job.commit()

if __name__ == '__main__':
    # Create glue context
    glueContext = GlueContext(SparkContext.getOrCreate())

    # Create job
    job = Job(glueContext)

    main(sys.argv, glueContext, job)