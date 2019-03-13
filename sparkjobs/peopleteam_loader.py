#!/usr/bin/env python


# Imports the Google Cloud client library
# Google's docs on configuring logging are total garbage
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client, name='peopleteam_loader')
logger = logging.getLogger('cloudLogger')
logger.setLevel(logging.INFO) # defaults to WARN
logger.addHandler(handler)

#from google.cloud import logging_v2
#import json
#
#client = logging_v2.LoggingServiceV2Client()
##entries = [{'severity':'INFO','trace':'FAKETRACEID','text_payload':"test log entry"}]
#entries = [{'severity':'INFO','trace':'FAKETRACEID','proto_payload':json.dumps({'@type':'type.googleapis.com/google.protobuf.Struct','script':'peopleteam_loader','something':'else'})}]
#response = client.write_log_entries(entries,resource={'type': 'global'},log_name='projects/imposing-union-227917/logs/peopleteam_loader')

import sys, os, re
import datetime
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format

script_id = 'peopleteam_loader'

def main():
  csv_file_full_path = sys.argv[1]
  out_dir            = sys.argv[2]
  
  
  csv_file = os.path.basename(csv_file_full_path)
  
  logger.info("[%s] starting. csv_file=%s" % (script_id,csv_file))
  
  #current_date       = datetime.datetime.now().strftime("%Y-%m-%d")
  #
  ## get the pull date for posterity ... ?
  #match     = re.search('pull_date_([0-9-]+)_', csv_file)
  #pull_date = match.group(1)
  
  if re.search('^hire', csv_file):
    file_type = 'HIRES'
    script_id += ':hires'
    table     = 'workday_hires_monthly'
    expected_columns = ['Employee_ID', 'Gender', 'Female_Y_N', 'Male_Y_N',
                        'Gender_Decline_Y_N', 'Gender_Other_Y_N', 'Gender_Blank_Y_N',
                        'Race_Ethnicity__Locale_Sensitive_', 'Asian_Y_N',
                        'Black_or_African_American_Y_N', 'Hispanic_or_Latino_Y_N',
                        'Hawaiian_or_Other_Pacific_Islander_Y_N',
                        'American_Indian_or_Alaska_Native_Y_N', 'Two_or_More_Races_Y_N',
                        'Race_is_Other_Y_N', 'White_Y_N', 'Race_Decline_Y_N', 'Race_Blank_Y_N',
                        'Original_Hire_Date', 'continuous_service_date', 'Company_Service_Date',
                        'Benefits_Service_Date', 'Seniority_Date', 'Cost_Center',
                        'Cost_Center_Hierarchy', 'Functional_Group', 'Steering_Committee',
                        'SCVP', 'Management_Chain_-_Level_01', 'Management_Chain_-_Level_02',
                        'Management_Chain_-_Level_03', 'Management_Chain_-_Level_04',
                        'Management_Chain_-_Level_05', 'Management_Chain_-_Level_06',
                        'Worker_Type', 'Position_Worker_Type', 'Job_Family', 'Job_Level',
                        'Is_Manager', 'Engineering_Non-Engineering', 'Hire_Date',
                        'Office_or_Remote_Status', 'Work_Address_-_City',
                        'Work_Address_-_Postal_Code', 'location', 'Location_Country',
                        'Manager_Name', 'Worker_Status', 'People_Partner', 'snapshot_date']
  elif re.search('^term', csv_file):
    file_type = 'TERMINATIONS'
    script_id += ':terminations'
    table     = 'workday_terminations_monthly'
    expected_columns = ['Employee_ID', 'Gender', 'Female_Y_N', 'Male_Y_N', 'Gender_Decline_Y_N',
                        'Gender_Other_Y_N', 'Gender_Blank_Y_N', 'Race_Ethnicity__Locale_Sensitive_',
                        'Asian_Y_N', 'Black_or_African_American_Y_N', 'Hispanic_or_Latino_Y_N',
                        'Hawaiian_or_Other_Pacific_Islander_Y_N', 'American_Indian_or_Alaska_Native_Y_N',
                        'Two_or_More_Races_Y_N', 'Race_is_Other_Y_N', 'White_Y_N', 'Race_Decline_Y_N',
                        'Race_Blank_Y_N', 'Original_Hire_Date', 'continuous_service_date',
                        'Company_Service_Date', 'Benefits_Service_Date', 'Seniority_Date', 'Hire_Date',
                        'Cost_Center', 'Cost_Center_Hierarchy', 'Functional_Group', 'Steering_Committee',
                        'SCVP', 'Management_Chain_-_Level_01', 'Management_Chain_-_Level_02',
                        'Management_Chain_-_Level_03', 'Management_Chain_-_Level_04',
                        'Management_Chain_-_Level_05', 'Management_Chain_-_Level_06', 'Worker_Type',
                        'Position_Worker_Type', 'Job_Family', 'Job_Level', 'Is_Manager',
                        'Engineering_Non-Engineering', 'Office_or_Remote_Status', 'Work_Address_-_City',
                        'Work_Address_-_Postal_Code', 'location', 'Location_Country', 'Manager_Name',
                        'Worker_Status', 'People_Partner', 'Termination_Date', 'Termination_Category',
                        'Primary_Termination_Reason', 'snapshot_date']
  elif re.search('^promo', csv_file):
    file_type = 'PROMOTIONS'
    script_id += ':promotions'
    table     = 'workday_promotions_monthly'
    # NOTE! These are not the right columns for the promos report!
    expected_columns = ['Employee_ID', 'Gender', 'Female_Y_N', 'Male_Y_N', 'Gender_Decline_Y_N',
      'Gender_Other_Y_N', 'Gender_Blank_Y_N', 'Race_Ethnicity__Locale_Sensitive_', 'Asian_Y_N',
      'Black_or_African_American_Y_N', 'Hispanic_or_Latino_Y_N', 'Hawaiian_or_Other_Pacific_Islander_Y_N',
      'American_Indian_or_Alaska_Native_Y_N', 'Two_or_More_Races_Y_N', 'Race_is_Other_Y_N', 'White_Y_N',
      'Race_Decline_Y_N', 'Race_Blank_Y_N', 'Original_Hire_Date', 'continuous_service_date',
      'Company_Service_Date', 'Benefits_Service_Date', 'Seniority_Date', 'Hire_Date', 'Cost_Center',
      'Cost_Center_Hierarchy', 'Functional_Group', 'Steering_Committee', 'SCVP',
      'Management_Chain_-_Level_01', 'Management_Chain_-_Level_02', 'Management_Chain_-_Level_03',
      'Management_Chain_-_Level_04', 'Management_Chain_-_Level_05', 'Management_Chain_-_Level_06',
      'Worker_Type', 'Position_Worker_Type', 'Job_Family', 'Job_Level', 'Is_Manager',
      'Engineering_Non-Engineering', 'Office_or_Remote_Status', 'Work_Address_-_City',
      'Work_Address_-_Postal_Code', 'location', 'Location_Country', 'Manager_Name', 'Worker_Status',
      'People_Partner', 'Termination_Date', 'Termination_Category', 'Primary_Termination_Reason',
      'snapshot_date']
  elif re.search('^head', csv_file):
    file_type        = 'HEADCOUNT'
    script_id       += ':headcount'
    table            = 'workday_employees_monthly'
    expected_columns = ['Employee_ID', 'Cost_Center', 'Cost_Center_Hierarchy', 'Functional_Group',
      'Steering_Committee', 'SCVP', 'Management_Chain_-_Level_01', 'Management_Chain_-_Level_02',
      'Management_Chain_-_Level_03', 'Management_Chain_-_Level_04', 'Management_Chain_-_Level_05',
      'Management_Chain_-_Level_06', 'Worker_Type', 'Position_Worker_Type', 'Job_Family', 'Job_Level',
      'Is_Manager', 'Engineering_Non-Engineering', 'Gender', 'Female_Y_N', 'Male_Y_N', 'Gender_Decline_Y_N',
      'Gender_Other_Y_N', 'Gender_Blank_Y_N', 'Race_Ethnicity__Locale_Sensitive_', 'Asian_Y_N',
      'Black_or_African_American_Y_N', 'Hispanic_or_Latino_Y_N', 'Hawaiian_or_Other_Pacific_Islander_Y_N',
      'American_Indian_or_Alaska_Native_Y_N', 'Two_or_More_Races_Y_N', 'Race_is_Other_Y_N', 'White_Y_N',
      'Race_Decline_Y_N', 'Race_Blank_Y_N', 'Original_Hire_Date', 'continuous_service_date',
      'Company_Service_Date', 'Benefits_Service_Date', 'Seniority_Date', 'Hire_Date',
      'Office_or_Remote_Status', 'Work_Address_-_City', 'Work_Address_-_Postal_Code', 'location',
      'Location_Country', 'Manager_Name', 'Worker_Status', 'People_Partner','snapshot_date']
  else:
    error = 'filename not recognized!'
    logger.error("[%s] %s",script_id,error)
    raise(ValueError(error))
  
  parquet_outdir_name = file_type
  
  print("file_type '%s' detected" % file_type)
  logger.info("[%s] file_type '%s' detected" ,script_id, file_type)
  spark = SparkSession.builder.appName('csv_parser').getOrCreate()
  
  try:
    csv_data = spark.read.csv(csv_file_full_path,header=True,inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache()
  
    # make sure the columns are in the order we expect, and remove any extra columns
    csv_data = csv_data.select(expected_columns)
  except Exception as e:
    logger.error("[%s] Caught exception: '%s'",script_id,str(e))
    raise e
  
  # apparently hive can't handle GCS's representation of Spark's timestamps for partitions
  # "snapshot_date=2016-09-30 00%3A00%3A00/"
  # So let's convert snapshot_date into a string representation of a date
  #
  csv_data = csv_data.withColumn('snapshot_date', date_format(csv_data.snapshot_date, 'yyyy-MM-dd'))
  
  snapdate        = csv_data.select(csv_data["snapshot_date"].alias('sd')).distinct().collect()
  csv_outdir_name = file_type + '_' + str(snapdate[0]['sd'])
  csv_outdir_name = csv_outdir_name.replace(" ", "_")
  csv_outdir_name = csv_outdir_name.replace(":", "_")
  
  # the schema used by tableau involves having a "date" field with different meanings :(
  if file_type == 'HEADCOUNT':
    new_csv = csv_data.withColumn('date_for_tableau', csv_data.snapshot_date)
  elif file_type == 'HIRES':
    new_csv  = csv_data.withColumn('date_for_tableau', date_format(csv_data.Hire_Date, 'yyyy-MM-dd'))
  elif file_type == 'TERMINATIONS':
    new_csv = csv_data.withColumn('date_for_tableau', date_format(csv_data.Termination_Date, 'yyyy-MM-dd'))
  elif file_type == 'PROMOTIONS':
    new_csv = csv_data.withColumn('date_for_tableau', date_format(csv_data.Promotion_Date, 'yyyy-MM-dd'))
  
  print("writing out transformed CSV to %s" % out_dir + '/csv/' + csv_outdir_name)
  logger.info("[%s] writing out transformed CSV to %s",script_id,  out_dir + '/csv/' + csv_outdir_name)
  new_csv.write.option('escape', '"').save(out_dir + '/csv/' + csv_outdir_name, mode='overwrite', format='csv')
  
  # so we can overwrite (only one) partition:
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","DYNAMIC")
  logger.info("[%s] writing out transformed PARQUET to %s",script_id,  out_dir + '/parquet/' + parquet_outdir_name)
  new_csv.write.partitionBy('snapshot_date').save(out_dir + '/parquet/' + parquet_outdir_name, mode='overwrite')
  
  logger.info("[%s] executing bq DELETE for snapshot_date=%s",script_id, str(snapdate[0]['sd']))
  project = 'imposing-union-227917'
  dataset = 'workday'
  subprocess.check_call([
      'bq','query','--use_legacy_sql=FALSE',
      'DELETE FROM %s.%s ' % (dataset, table) + \
      "WHERE snapshot_date='%s'" % str(snapdate[0]['sd'])
  ])
  
  logger.info("[%s] loading CSV data into BQ", script_id)
  # Shell out to bq CLI to perform BigQuery import.
  subprocess.check_call(
      'bq load --source_format CSV '
      '--noreplace '
     '{dataset}.{table} {files}'.format(
          dataset=dataset, table=table, files=out_dir + '/csv/' + csv_outdir_name + '/part*'
      ).split())
  
  logger.info("[%s] script finished", script_id)

try:
  main()
except Exception as e:
  logger.error("[%s] Caught exception: '%s'",script_id,str(e))
  raise e
