import unittest
from unittest.mock import MagicMock, patch
import logging
import os
from datetime import date, datetime
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
from awsglue.context import GlueContext, DynamicFrame
from pyspark.sql.types import StructField, StructType, StringType, DateType, TimestampType, IntegerType
import glue_scripts.amod_current_dataset as merge_into_customer_dim
# import glue_scripts.merge_into_customer_dim as merge_into_customer_dim
from glue_scripts.tests.pyspark_test_helper import PySparkTest
from awsglue.job import Job

class TestMergeIntoCustomerDim(PySparkTest):
    output_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("Name", StringType(), True),
    ])

    argv = ["",
        "--JOB_NAME", "ut_job",
        "--database_name", "db_name",
        "--source_table", "ut_source",
        "--target_path", "s3://ut_target_path"]

    def __mock_staging(self, glue_context, rows):
        input_df = self.spark.createDataFrame(rows)
        print('input df')
        print(input_df.show())
        dynamic_df = DynamicFrame.fromDF(input_df, glue_context, 'staging_df')
        print('dynamic df')
        print(dynamic_df.show())
        glue_context.create_dynamic_frame_from_catalog = MagicMock(return_value=dynamic_df)

    def test_sanity(self):
        assert 1 == 1

    @patch('awsglue.job.Job')
    def test_no_existing_output(self, mock_job):
        glue_context = GlueContext(self.spark)
        self.__mock_staging(glue_context, [
                {
                    "payload": {
                        "before": {"ID": 158, "NAME": "6tsetadbmal" },
                        "after": {"ID": 158, "NAME": "7tsetadbmal" },
                        "source": {
                        "version": "1.2.0.Beta2",
                        "connector": "db2",
                        "name": "database",
                        "ts_ms": 1591035016774,
                        "snapshot": "false",
                        "db": "testdb",
                        "schema": "DB2INST1",
                        "table": "EXAMPLE_TABLE",
                        "change_lsn": "00000000:00000000:0000000006f4b31f",
                        "commit_lsn": "00000000:000102b5:000000000007bf55"
                        },
                        "op": "u",
                        "ts_ms": 1591017016750
                    }
                }
            ])
        # self.__mock_existing_target(glue_context, [])
        glue_context.write_dynamic_frame_from_options = MagicMock()
        glue_context.purge_s3_path = MagicMock()

        merge_into_customer_dim.main(self.argv, glue_context, mock_job)

        expected_df = input_df = self.spark.createDataFrame([
                [158, "7tsetadbmal"]
            ], schema=self.output_schema)

        write_args, write_kargs = glue_context.write_dynamic_frame_from_options.call_args
        print('write_kargs')
        print(write_kargs['frame'].toDF().show())
        print(write_kargs['frame'])
        self.assert_dataframe_equal(write_kargs['frame'].toDF(), expected_df, ["ID"])

    # @patch('awsglue.job.Job')
    # def test_the_target_path_is_purged(self, mock_job):
    #     glue_context = GlueContext(self.spark)
    #     self.__mock_staging(glue_context, [
    #             {
    #                 "id": "01",
    #                 "firstname": "John",
    #                 "lastname": "Smith",
    #                 "birthdate": "1990-01-01",
    #                 "zipcode": "12345",
    #                 "modifieddate": "2019-01-01T00:40:32Z",
    #             }
    #         ])
    #     self.__mock_existing_target(glue_context, [])
    #     glue_context.write_dynamic_frame_from_options = MagicMock()
    #     glue_context.purge_s3_path = MagicMock()

    #     merge_into_customer_dim.main(self.argv, glue_context, mock_job)

    #     glue_context.purge_s3_path.assert_called_with(
    #         s3_path = "s3://ut_target_path",
    #         options = {
    #             "retentionPeriod": 0
    #         }
    #     )

    # @patch('awsglue.job.Job')
    # def test_duplicate_rows_in_target_are_deduped(self, mock_job):
    #     glue_context = GlueContext(self.spark)
    #     self.__mock_staging(glue_context, [
    #         {
    #             "id": "02",
    #             "firstname": "Bob from staging",
    #             "modifieddate": "2019-01-02T00:40:32Z"
    #         }
    #     ])
    #     self.__mock_existing_target(glue_context, [
    #         {
    #             "id": "01",
    #             "first_name": "John",
    #             "modified_date": datetime.fromisoformat("2019-01-01T00:40:32+00:00")
    #         },
    #         {
    #             "id": "02",
    #             "first_name": "Bob",
    #             "modified_date": datetime.fromisoformat("2019-01-01T00:40:32+00:00")
    #         },
    #         {
    #             "id": "01",
    #             "first_name": "Bill",
    #             "modified_date": datetime.fromisoformat("2019-01-02T00:40:32+00:00")
    #         }
    #     ])
    #     glue_context.write_dynamic_frame_from_options = MagicMock()
    #     glue_context.purge_s3_path = MagicMock()

    #     merge_into_customer_dim.main(self.argv, glue_context, mock_job)

    #     expected_df = input_df = self.spark.createDataFrame([
    #             ["01", "Bill", None, None, None, datetime.fromisoformat("2019-01-02T00:40:32+00:00")],
    #             ["02", "Bob from staging", None, None, None, datetime.fromisoformat("2019-01-02T00:40:32+00:00")]
    #         ], schema=self.output_schema)

    #     write_args, write_kargs = glue_context.write_dynamic_frame_from_options.call_args
    #     self.assert_dataframe_equal(write_kargs['frame'].toDF(), expected_df, ["id"])