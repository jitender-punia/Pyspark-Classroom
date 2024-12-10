import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, IntegerType, DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def load_pps_report_rs():
    """
    Main function to run: initialize class for data preparation, prepare data and write it to db
    """
    data_prep = Preparation()
    result = data_prep.prepare_full_data()
    data_prep.write_to_db(result)
    print("process completed")

class Data:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv,['TempDir', 'JOB_NAME', 'env', 'redshiftuser', 'redshiftpass', "redshifthost"])
        self.ENV = self.args['env']
        self.glue_database = f'enterprise-reporting-catalog-db-{self.ENV}'
        self.grant_stat = self._grant_rights()
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.args['JOB_NAME'], self.args)
        self.logger = self.glue_context.get_logger()
        

    def _grant_rights(self):
        if self.ENV == 'prod':
            group_env_name = 'prod_reporting'
        else:
            group_env_name = 'stage_reporting'
        return group_env_name


class Preparation(Data):
    def __init__(self):
        super(Preparation, self).__init__()

    def prepare_full_data(self):
       
        pps_visa_daily_datasource = self.glue_context.create_dynamic_frame.from_catalog(database=self.glue_database,
                                                                            table_name="pps_visa_daily_snapshot",
                                                                            transformation_ctx="pps_visa_daily_datasource")
        df_pps = pps_visa_daily_datasource.toDF()
        print("table read completed")
        return df_pps

    def write_to_db(self, data):
        # Write as dynamic frame
        redshift_output_table = 'public.pps_visa_reporting_daily_snapshot'
        show_data = DynamicFrame.fromDF(dataframe=data, glue_ctx=self.glue_context, name="show_data")
        data_sink = self.glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=show_data,
            catalog_connection="enterprise-reporting-redshift-connection-" + self.ENV,
            connection_options={"dbtable": redshift_output_table,
                                "postactions": f"GRANT ALL on {redshift_output_table} to GROUP {self.grant_stat}",
                                "database": "enterprisereporting_db_" + self.ENV},
            redshift_tmp_dir=self.args["TempDir"],
            transformation_ctx="data_sink"
        )
        self.job.commit()

# call the main function
load_pps_report_rs()
