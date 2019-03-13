#!/usr/bin/env python


# Imports the Google Cloud client library
# Google's docs on configuring logging are total garbage
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client, name='sfmc_loader')
logger = logging.getLogger('cloudLogger')
logger.setLevel(logging.INFO) # defaults to WARN
logger.addHandler(handler)

import sys, os, re
import datetime
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format, col, to_timestamp

script_id = 'sfmc_loader'

def main():
  csv_indir  = sys.argv[1]
  filename   = sys.argv[2]
  out_dir    = sys.argv[3]
  
  date_to_process = os.path.basename(csv_indir)
  
  script_id = 'sfmc_loader:' + filename
  
  logger.info("[%s] starting. csv_indir=%s" % (script_id,csv_indir))
  
  spark = SparkSession.builder.appName('csv_parser').getOrCreate()
  
  try:
    logger.info("[%s] reading %s" % (script_id,csv_indir + '/' + filename + '.csv'))
    csv_data = spark.read.csv(csv_indir + '/' + filename + '.csv', header=True, sep="\t", inferSchema=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True).cache()
  
    # arguably I'm doing too much transforming here. Maybe should just load straight across?
    if filename == 'Bounces':
      wanted_columns = [
        'SendID', 'SubscriberID', 'ListID', 'EventDate', 'EventType', 'BounceCategory', 
        'SMTPCode', 'BounceReason', 'BatchID', 'TriggeredSendExternalKey'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'Clicks':
      # remove PII
      wanted_columns = [
        'SendID', 'SubscriberID', 'ListID', 'EventDate', 'EventType', 'SendURLID', 'URLID', 
        'URL', 'Alias', 'BatchID', 'TriggeredSendExternalKey'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'Opens':
      wanted_columns = [
        'SendID', 'SubscriberID', 'ListID', 'EventDate', 'EventType', 'BatchID', 
        'TriggeredSendExternalKey'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'SendJobs':
      wanted_columns = [
        'SendID', 'FromName', 'FromEmail', 'SchedTime', 'SentTime', 'Subject', 
        'EmailName', 'TriggeredSendExternalKey', 'SendDefinitionExternalKey', 'JobStatus',
        'PreviewURL', 'IsMultipart', 'Additional'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('SchedTime', to_timestamp(csv_data.SchedTime, 'M/d/yyyy h:m:s a')) \
        .withColumn('SentTime', to_timestamp(csv_data.SentTime, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'Sent':
      wanted_columns = [
        'SendID', 'SubscriberID', 'ListID', 'EventDate', 'EventType', 'BatchID',
        'TriggeredSendExternalKey'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'Subscribers':
      wanted_columns = [
        'SubscriberID', 'Status', 'DateHeld', 'DateCreated', 'DateUnsubscribed'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('DateHeld', to_timestamp(csv_data.DateHeld, 'M/d/yyyy h:m:s a')) \
        .withColumn('DateCreated', to_timestamp(csv_data.DateCreated, 'M/d/yyyy h:m:s a')) \
        .withColumn('DateUnsubscribed', to_timestamp(csv_data.DateUnsubscribed, 'M/d/yyyy h:m:s a')) \
        .withColumn('snapshot_date', lit(date_to_process))
  
    elif filename == 'Unsubs':
      wanted_columns = [
        'SendID', 'SubscriberID', 'ListID', 'EventDate', 'EventType', 'BatchID',
        'TriggeredSendExternalKey'
      ]
      csv_data = csv_data.select(wanted_columns) \
        .withColumn('EventDate', to_timestamp(csv_data.EventDate, 'M/d/yyyy h:m:s a')) \
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
  dataset = 'sfmc'
  table   = filename.lower()
  subprocess.check_call([
      'bq','query','--use_legacy_sql=FALSE',
      'DELETE FROM %s.%s ' % (dataset, table) + \
      "WHERE snapshot_date='%s'" % date_to_process
  ])
  
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
  
  logger.info("[%s] script finished", script_id)

try:
  main()
except Exception as e:
  logger.error("[%s] Caught exception: '%s'",script_id,str(e))
  raise e
