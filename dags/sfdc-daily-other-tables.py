import datetime, re, os

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable


#PROJECT          = Variable.get('gcp_project')
PROJECT          = os.environ.get('gcp_project')
LATEST_TAG       = Variable.get('latest_di_image_tag')
INCOMING_BUCKET  = PROJECT + '-data-incoming'
SPARKJOBS_BUCKET = PROJECT + '-data-sparkjobs'
OUTPUT_BUCKET    = PROJECT + '-data-sfdc'

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/' + PROJECT + '/data-integrations:' + LATEST_TAG

sf_secret = secret.Secret( deploy_type='volume',
    deploy_target='/config',
    secret='salesforce-fetcher-config',
    key='salesforce_fetcher_config.yaml')

default_args = {
          'retries': 3,
          'retry_delay': datetime.timedelta(seconds=10),
          'execution_timeout': datetime.timedelta(minutes=30),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,19),
          'start_date': datetime.datetime(2019,4,18),
          'catchup_by_default': False,
          # TODO: email stuff
}

dag = DAG(dag_id='sfdc-daily-other-tables',
          schedule_interval=datetime.timedelta(days=1),
          catchup=False,
          default_args=default_args
         )

fetch_op = {}
clean_op = {}
load_op  = {}
merge_op = {}

for report in ['contact_donor_count','donation_record_count','foundation_signups','petition_signups']:

  # stupid airflow not sanitizing input
  report_name_for_k8s = re.sub('_', '-', report)

  fetch_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-fetcher-' + report_name_for_k8s,
        name='sfdc-daily-fetcher-'+ report_name_for_k8s,
        namespace='default',
        image=IMAGE,
        secrets=[sf_secret],
        cmds=['sh', '-c', 'export LC_ALL=C.UTF-8; export LANG=C.UTF-8; /usr/local/bin/salesforce-fetcher --config-file /config/salesforce_fetcher_config.yaml --fetch-only ' + report + ' && gsutil cp -r /tmp/salesforce_fetcher/* gs://'+INCOMING_BUCKET+'/sfdc/'],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag)

  load_op[report] = BashOperator(
                  task_id='sfdc-daily-load-' + report_name_for_k8s,
                  bash_command='bq load --location=US --source_format CSV --skip_leading_rows=1 --autodetect --replace sfdc.'+ report + ' gs://' + INCOMING_BUCKET + '/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}/output.csv',
                  dag=dag)

  load_op[report].set_upstream(fetch_op[report])

#  # Do we need to cleanup? No PII here.
#  clean_op[report] = BashOperator(
#               task_id='sfdc-daily-load-cleanup-' + report_name_for_k8s,
#               #bash_command='gsutil rm gs://'+INCOMING_BUCKET+'/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}',
#               bash_command='echo "NOOP"',
#               dag=dag)
#
#  clean_op[report].set_upstream(load_op[report])

#
# contact_history is a date-based query, so shouldn't be overwritten daily
# BUT, the query specifies "YESTERDAY", so we can't run the fetcher on 
# historical data, either.
#
for report in ['contact_history']:
  # stupid airflow not sanitizing input
  report_name_for_k8s = re.sub('_', '-', report)

  fetch_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-fetcher-' + report_name_for_k8s,
        name='sfdc-daily-fetcher-'+ report_name_for_k8s,
        namespace='default',
        image=IMAGE,
        secrets=[sf_secret],
        cmds=['sh', '-c', 'export LC_ALL=C.UTF-8; export LANG=C.UTF-8; /usr/local/bin/salesforce-fetcher --config-file /config/salesforce_fetcher_config.yaml --fetch-only ' + report + ' && gsutil cp -r /tmp/salesforce_fetcher/* gs://'+INCOMING_BUCKET+'/sfdc/'],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag)

  load_op[report] = BashOperator(
                  task_id='sfdc-daily-load-' + report_name_for_k8s,
                  bash_command='bq load --location=US --source_format CSV --skip_leading_rows=1 --replace sfdc_staging.'+ report + ' gs://' + INCOMING_BUCKET + '/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}/output.csv',
                  dag=dag)

  load_op[report].set_upstream(fetch_op[report])

  merge_query = ''
  if report == 'contact_history':
    merge_query = """
      MERGE sfdc.contact_history P
      USING sfdc_staging.contact_history S
      ON P.CreatedDate=PARSE_DATETIME('%Y-%m-%dT%T.000+0000', S.CreatedDate)
        AND P.Field=S.Field AND P.ContactId=S.ContactId
      WHEN NOT MATCHED THEN
        INSERT (Field,ContactId,CreatedDate,NewValue,OldValue)
        VALUES (Field,ContactId,PARSE_DATETIME('%Y-%m-%dT%T.000+0000', CreatedDate),NewValue,OldValue);
    """

  merge_query = re.sub("\n| {2,}", ' ', merge_query)
  if re.search('"', merge_query):
    raise Exception("You must not use double quotes in your query!")

  merge_op[report] = BashOperator(
                  task_id='sfdc-daily-merge-' + report_name_for_k8s,
                  bash_command='bq query --use_legacy_sql=FALSE "%s"' % merge_query,
                  dag=dag)

  merge_op[report].set_upstream(load_op[report])


#
#  del_op[report] = BashOperator(
#                  task_id='sfdc-daily-delete-' + report_name_for_k8s,
#                  bash_command='bq query "DELETE FROM sfdc.' + report + ' WHERE snapshot_date={{ macros.ds_add(ds, 1) }}"',
#                  dag=dag)
#
#  del_op[report].set_upstream(load_op[report])
#
#INSERT INTO sfdc.contact_donor_count
#(Opportunity_Name, Amount , Contact_ID, snapshot_date)
#SELECT Opportunity_Name, Amount , Contact_ID, {{ macros.ds_add(ds, 1) }}
#FROM sfdc_staging.contact_donor_count;
#
#  ins_op[report] = BashOperator(
#                  task_id='sfdc-daily-insert-' + report_name_for_k8s,
#                  bash_command='bq query "dc.' + report + ' WHERE snapshot_date={{ macros.ds_add(ds, 2) }}"',
#                  dag=dag)
#
#  ins_op[report].set_upstream(del_op[report])

