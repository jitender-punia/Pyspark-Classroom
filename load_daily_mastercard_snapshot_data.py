import sys
import boto3
from datetime import date, datetime, timedelta
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.types import StringType, IntegerType, DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from common_datasource_functions import get_lkp_fxrate_df,get_lkp_bin_ica_apply_mapping

def load_pps_report_rs():
    """
    Main function to run: initialize class for data preparation, prepare data and write it to db
    """
    data_prep = Preparation()
    result_visa = data_prep.prepare_full_visa_data()
    result_mc = data_prep.prepare_full_mc_data()
    print("Visa Table load has started")
    data_prep.write_to_db_visa(result_visa)
    print("****Visa Table load Ended")
    print("MC Table load has started")
    data_prep.write_to_db_mc(result_mc)
    print("***MC Table load Ended")
    print("****process completed******")

class Data:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv,['TempDir', 'JOB_NAME', 'env', 'redshiftuser', 'redshiftpass', 'redshifthost','run_date'])
        self.ENV = self.args['env']
        self.run_date = self.args['run_date']
        self.glue_database = f'enterprise-reporting-catalog-db-{self.ENV}'
        self.grant_stat = self._grant_rights()
        self.sns_grant_stat = self._sns_env()
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
    
    def _sns_env(self):
        if self.ENV == 'prod':
            sns_env_name = '271481108287'
        else:
            sns_env_name = '392685408529'
        return sns_env_name

class Preparation(Data):
    def __init__(self):
        super(Preparation, self).__init__()

    def prepare_full_visa_data(self):       
        try:
            #yesterday_dt = '2022-08-09'
            if self.run_date != 'NULL':
                yesterday_dt=self.run_date
            else:
                yesterday_dt=datetime.strftime(datetime.now() - timedelta(days=1),'%Y-%m-%d')
            print("Visa Process running with Yesterday date: ",yesterday_dt)
            pps_visa_daily_datasource = self.glue_context.create_dynamic_frame.from_catalog(database=self.glue_database,
                                                                            table_name="pps_visa_daily_snapshot",
                                                                            transformation_ctx="pps_visa_daily_datasource")
            df_visa_pps = pps_visa_daily_datasource.toDF()
            print("Removing Dupicates from Visa file")
            df_visa_pps=df_visa_pps.dropDuplicates(['report_date','transaction_type','bin','Account_Funding_Source','purchase_date','acquirer_settlement_currency',
                                                    'domestic_international','settlement_flag','transaction_code_qualifier','transaction_count','transaction_amount'])
            #df_visa_pps=df_visa_pps.withColumn('report_date',func.to_date(func.col('report_date'),"yyyy-MM-dd"))
            df_visa_pps_max=df_visa_pps.agg(func.max('report_date')).collect()[0][0]
            print("Latest Visa Data Available in S3: ", df_visa_pps_max)
            if (df_visa_pps_max >= yesterday_dt):
                print("Visa table read completed*****")
                return df_visa_pps
            else:
                print("Visa file for date ", yesterday_dt," is missing. Aborting the Visa Run...!!")
                sys.exit(1)

        except Exception as ex:
            print(ex)    

    def prepare_full_mc_data(self):
        try:
            #yesterday_dt = '2022-08-09'
            if self.run_date != 'NULL':
                yesterday_dt=self.run_date
            else:
                yesterday_dt=datetime.strftime(datetime.now() - timedelta(days=1),'%Y-%m-%d')
            print("MC Process running with yesterday date: ",yesterday_dt)
            pps_mastercard_daily_datasource = self.glue_context.create_dynamic_frame.from_catalog(database=self.glue_database,
                                                                            table_name="pps_mastercard_daily_snapshot",
                                                                            transformation_ctx="pps_mastercard_daily_datasource")

            df_pps = pps_mastercard_daily_datasource.toDF()
            print("Removing Dupicates from MC file")
            df_pps=df_pps.dropDuplicates(['Date','ICA','Currency','Description','Recon_Amt','RecAmt_Sign',
                                          'Transaction_Fee','TFee_Sign','Orig','File_ID','Business_Service_ID','Acceptance_Brand','Count','Gross_Amount'])
            #getting the currecny from pps mastercard glue table and keep prg name so that later it can be populated from bin ica table
            df_pps=df_pps.withColumn("currency",func.split(func.col("currency"),"-").getItem(1))
            
            #Reading fx rate glue table
            df_lkp_fxrate = get_lkp_fxrate_df(self.glue_context, self.glue_database)
            df_lkp_fxrate = df_lkp_fxrate.withColumnRenamed("From", "currency")
            df_lkp_fxrate_all_cad = df_lkp_fxrate.filter(df_lkp_fxrate['to'] == "CAD")
            df_lkp_fxrate_all_cad = df_lkp_fxrate_all_cad.drop("To")
            
            #left join pps mastercard glue table with fx rate table
            df_mc_pps = df_pps.join(df_lkp_fxrate_all_cad,['date', "currency"],how='left')
            df_mc_pps=df_mc_pps.withColumn('rate', func.coalesce(func.col("rate"),func.lit(1)))
            df_mc_pps=df_mc_pps.withColumn("converted_gross_amount",func.col("gross_amount")*func.col("rate"))
        
            df_mc_pps_max=df_mc_pps.agg(func.max('date')).collect()[0][0]
            print("Latest MC Data Available in S3: ", df_mc_pps_max)
            if (df_mc_pps_max >= yesterday_dt):
                print("MC table read completed")
                return df_mc_pps
            else:
                print("Mastercard file for date ", yesterday_dt," is missing. Aborting the Mastercard Run...!!")
                sys.exit(1) 

        except Exception as ex:
            print(ex)    
            
    def write_to_db_visa(self, data):
        try:
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
        except Exception as ex:
            print(ex)  

    def write_to_db_mc(self, data):
        try:
            # Write as dynamic frame
            redshift_output_table = 'public.pps_mastercard_reporting_daily_snapshot'
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
        except Exception as ex:
            print(ex)
        self.job.commit()


# call the main function
load_pps_report_rs()
