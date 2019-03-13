#!/usr/bin/env python


# Imports the Google Cloud client library
# Google's docs on configuring logging are total garbage
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client, name='sfdc_loader')
logger = logging.getLogger('cloudLogger')
logger.setLevel(logging.INFO) # defaults to WARN
logger.addHandler(handler)

import sys, os, re
import datetime
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format, col, to_timestamp, to_date

script_id = 'sfdc_loader'

def main():
  csv_indir  = sys.argv[1]
  filename   = sys.argv[2]
  out_dir    = sys.argv[3]
  
  date_to_process = os.path.basename(csv_indir)
  
  script_id = 'sfdc_loader:' + filename
  
  logger.info("[%s] starting. csv_indir=%s" % (script_id,csv_indir))
  
  spark = SparkSession.builder.appName('csv_parser').getOrCreate()
  
  try:
    logger.info("[%s] reading %s" % (script_id,csv_indir + '/output.csv'))
    csv_data = spark.read.csv(csv_indir + '/output.csv', header=True, inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache()
  
    if filename == 'contact_donor_count':
      wanted_columns = [ 'Opportunity Name', 'Amount', 'Contact ID' ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumnRenamed('Opportunity Name', 'Opportunity_Name') \
        .withColumnRenamed('Contact ID', 'Contact_ID') \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'contact_history':
      wanted_columns = [ 'Field', 'ContactId', 'CreatedDate', 'NewValue', 'OldValue' ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('CreatedDate', to_timestamp(csv_data.CreatedDate, "yyyy-MM-dd'T'H:m:s.SSS+0000")) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'contacts':
      wanted_columns = [
        'Id','CreatedDate','Email_Format__c','Email_Language__c','Signup_Source_URL__c',
        'Confirmation_MITI_Subscriber__c','Sub_Apps_And_Hacks__c','Sub_Firefox_And_You__c',
        'Sub_Firefox_Accounts_Journey__c','Sub_Mozilla_Foundation__c','Sub_MITI_Subscriber__c',
        'Sub_Mozilla_Leadership_Network__c','Sub_Mozilla_Learning_Network__c','Sub_Webmaker__c',
        'Sub_Mozillians_NDA__c','Sub_Open_Innovation_Subscriber__c','Subscriber__c',
        'Sub_Test_Flight__c','Sub_Test_Pilot__c','Sub_View_Source_Global__c',
        'Sub_View_Source_NAmerica__c','Double_Opt_In__c','Unengaged__c','HasOptedOutOfEmail',
        'MailingCountry','Sub_Common_Voice__c','Sub_Hubs__c','Sub_Mixed_Reality__c',
        'Sub_Mozilla_Tech__c','AMO_Email_Opt_In__c'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('CreatedDate', to_timestamp(csv_data.CreatedDate, "yyyy-MM-dd'T'H:m:s.SSS+0000")) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'donation_record_count':
      wanted_columns = [
        'Opportunity Name', 'Type', 'Lead Source', 'Amount', 'Close Date', 'Next Step',
        'Stage', 'Probability (%)', 'Fiscal Period', 'Age', 'Created Date', 'Opportunity Owner',
        'Owner Role', 'Account Name'
      ]
      # ugh. parquet no like spaces
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('Close Date', to_date(csv_data['Close Date'], 'M/d/yyyy')) \
        .withColumn('Created Date', to_date(csv_data['Created Date'], 'M/d/yyyy')) \
        .withColumnRenamed('Close Date', 'Close_Date') \
        .withColumnRenamed('Created Date', 'Created_Date') \
        .withColumnRenamed('Opportunity Name', 'Opportunity_Name') \
        .withColumnRenamed('Lead Source', 'Lead_Source') \
        .withColumnRenamed('Next Step', 'Next_Step') \
        .withColumnRenamed('Probability (%)', 'Probability_Percent') \
        .withColumnRenamed('Fiscal Period', 'Fiscal_Period') \
        .withColumnRenamed('Opportunity Owner', 'Opportunity_Owner') \
        .withColumnRenamed('Owner Role', 'Owner_Role') \
        .withColumnRenamed('Account Name', 'Account_Name') \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'foundation_signups':
      wanted_columns = [ 'Id', 'CreatedDate' ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('CreatedDate', to_timestamp(csv_data.CreatedDate, "yyyy-MM-dd'T'H:m:s.SSS+0000")) \
        .withColumn('snapshot_date', lit(date_to_process))
        #.withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
  
    elif filename == 'petition_signups':
      wanted_columns = [ 'ContactId', 'Member_First_Associated_Date__c' ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    else:
      error = 'filename "%s" not recognized!' % filename
      logger.error("[%s] %s",script_id,error)
      raise(ValueError(error))
  
    
  except Exception as e:
    logger.error("[%s] Caught exception: '%s'",script_id,str(e))
    raise e
  
  parquet_outdir_name = filename
  csv_outdir_name     = filename + '_' + date_to_process
  
  # Write out the CSV
  # adding the ".option('escape', '"')" option so the output CSV is RFC compliant
  # (and so bigquery will accept it)
  #
  print("writing out transformed CSV to %s" % out_dir + '/csv/' + csv_outdir_name)
  logger.info("[%s] writing out transformed CSV to %s", script_id,  out_dir + '/csv/' + csv_outdir_name)
  csv_data.write.option('escape', '"').save(out_dir + '/csv/' + csv_outdir_name, mode='overwrite', format='csv')
  
  # Write out the Parquet file
  #
  # so we can overwrite (only one) partition:
  #
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
  print("writing out transformed PARQUET to %s" % out_dir + '/parquet/' + parquet_outdir_name)
  logger.info("[%s] writing out transformed PARQUET to %s", script_id,  out_dir + '/parquet/' + parquet_outdir_name)
  csv_data.write.partitionBy('snapshot_date').save(out_dir + '/parquet/' + parquet_outdir_name, mode='overwrite')
  
  # Delete duplicate info from bigquery
  #
  logger.info("[%s] executing bq DELETE for snapshot_date=%s", script_id, date_to_process)
  project = 'imposing-union-227917'
  dataset = 'sfdc'
  table   = filename.lower()
  subprocess.check_call([
      'bq','query','--use_legacy_sql=FALSE',
      'DELETE FROM %s.%s ' % (dataset, table) + \
      "WHERE snapshot_date='%s'" % date_to_process
  ])

  # oops, not necessary
  #wait_for_csv(script_id, out_dir + '/csv/' + csv_outdir_name + '/_SUCCESS')

  #
  # Insert into bigquery
  #
  logger.info("[%s] loading CSV data into BQ", script_id)
  # Shell out to bq CLI to perform BigQuery import.
  subprocess.check_call(
      'bq load --source_format CSV '
      '--noreplace '
     '{dataset}.{table} {files}'.format(
          dataset=dataset, table=table, files=out_dir + '/csv/' + csv_outdir_name + '/part*'
      ).split())
  #
  logger.info("[%s] script finished", script_id)

def wait_for_csv(script_id, location):
  while True:
    try:
      print("Trying 'gsutil','ls',%s" % location)
      result = subprocess.check_call(['gsutil','ls',location])
      print("One assumes the CSV is done writing")
      logger.info("[%s] One assumes the CSV is done writing",script_id)
      return True
    except subprocess.CalledProcessError as e:
      if e.returncode == 1:
        print("One assumes the CSV is not finished. Sleeping for 30 seconds...")
        logger.info("[%s] One assumes the CSV is not finished. Sleeping for 30 seconds...",script_id)
        sleep(30)
  
try:
  main()
except Exception as e:
  logger.error("[%s] Caught exception: '%s'",script_id,str(e))
  raise e
