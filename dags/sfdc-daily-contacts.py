import datetime, re

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/imposing-union-227917/data-integrations:20190307'

sf_secret = secret.Secret( deploy_type='volume',
    deploy_target='/config',
    secret='salesforce-fetcher-config',
    key='salesforce_fetcher_config.yaml')

default_args = {
          'retries': 5,
          'retry_delay': datetime.timedelta(seconds=10),
          'retry_exponential_backoff': True,
          'max_retry_delay': datetime.timedelta(minutes=10),
          #'end_date': datetime.datetime(2019,2,19),
          'start_date': datetime.datetime(2019,2,26),
          'catchup_by_default': False,
          # TODO: email stuff
}

dag = DAG(dag_id='sfdc-daily-contacts',
          schedule_interval=datetime.timedelta(days=1),
          catchup=False,
          default_args=default_args
         )

fetch_op = {}
clean_op = {}
bash_op  = {}

for report in ['contacts']:

  # stupid airflow not sanitizing input
  report_name_for_k8s = re.sub('_', '-', report)

  fetch_op[report] = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='sfdc-daily-fetcher-' + report_name_for_k8s,
        name='sfdc-daily-fetcher-'+ report_name_for_k8s,
        namespace='default',
        image=IMAGE,
        secrets=[sf_secret],
        cmds=['sh', '-c', 'export LC_ALL=C.UTF-8; export LANG=C.UTF-8; /usr/local/bin/salesforce-fetcher --config-file /config/salesforce_fetcher_config.yaml --fetch-only ' + report + ' && gsutil cp -r /tmp/salesforce_fetcher/* gs://moz-it-data-dp2-incoming-dev/sfdc/'],
        #cmds=['sh', '-c', 'sleep 3600'],
        dag=dag)

  # The fetcher seems to report failure even when everything succeeds. Perhaps because airflow loses
  # its connection to the Pod? Shrug. Anyway, setting the trigger_rule for the next task to 'all_done'
  # will cause it to run even if the fetcher reports "failure"
  # Ah, known issue: https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator#task_fails_despite_pod_success

  bash_op[report] = BashOperator(
                  trigger_rule='all_done',
                  task_id='sfdc-daily-load-' + report_name_for_k8s,
                  bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/sfdc_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }} ' + report + ' gs://moz-it-data-dp2-sfdc-dev',
                  dag=dag)

  bash_op[report].set_upstream(fetch_op[report])

  # Do we need to cleanup? No PII here.
  clean_op[report] = BashOperator(
               task_id='sfdc-daily-load-cleanup-' + report_name_for_k8s,
               #bash_command='gsutil rm gs://moz-it-data-dp2-incoming-dev/sfdc/' + report + '/{{ macros.ds_add(ds, 1) }}',
               bash_command='echo "NOOP"',
               dag=dag)

  clean_op[report].set_upstream(bash_op[report])


query = "DELETE FROM sfdc.summary WHERE rollup_name NOT LIKE '%Unsubscribes' AND date=CURRENT_DATE()"
delete_query_unsub = BashOperator(
                        task_id='sfdc-daily-bq-delete-unsubscribes',
                        bash_command='bq query --use_legacy_sql=FALSE "%s"' % query,
                        dag=dag)
delete_query_unsub.set_upstream(bash_op['contacts'])

query = "DELETE FROM sfdc.summary WHERE rollup_name LIKE '%Unsubscribes' AND date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY)"
delete_query_sub = BashOperator(
                        task_id='sfdc-daily-bq-delete-subscribes',
                        bash_command='bq query --use_legacy_sql=FALSE "%s"' % query,
                        dag=dag)
delete_query_sub.set_upstream(bash_op['contacts'])

#
# Now insert into summary table
# I do not like this one bit.
#
table_name    = "sfdc.summary"
insert_fields = "rollup_name, rollup_value, date, mailing_country, email_language, email_format"
select_fields = "snapshot_date, MailingCountry, Email_Language__c, Email_Format__c"
addtl_where   = "AND Double_Opt_In__c = TRUE AND HasOptedOutOfEmail = FALSE AND Subscriber__c = TRUE"

summary_subscribe_queries = {
  'unique-contacts': "INSERT INTO %s (%s) SELECT 'Unique Contacts', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),

  'opted-in': "INSERT INTO %s (%s) SELECT 'Opted In', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND Subscriber__c = TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'opted-out': "INSERT INTO %s (%s) SELECT 'Opted Out', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() AND (Double_Opt_In__c = FALSE OR HasOptedOutOfEmail = TRUE OR Subscriber__c = FALSE) GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'moz-sub': "INSERT INTO %s (%s) SELECT 'Mozilla Subscriber', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND moz_subscriber=TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'dev-sub': "INSERT INTO %s (%s) SELECT 'Developer Subscriber', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND dev_subscriber=TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'fx-sub': "INSERT INTO %s (%s) SELECT 'Firefox Subscriber', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND fx_subscriber=TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'other-sub': "INSERT INTO %s (%s) SELECT 'Other Subscriber', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND other_subscriber=TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),

  'moz-labs-sub': "INSERT INTO %s (%s) SELECT 'Mozilla Labs Subscriber', COUNT(*), %s FROM sfdc.contacts_vw WHERE snapshot_date=CURRENT_DATE() %s AND moz_labs_subscriber=TRUE GROUP BY %s" % (table_name, insert_fields, select_fields, addtl_where, select_fields),
}

select_fields = "DATE(contact_history_vw.CreatedDate), MailingCountry, Email_Language__c, Email_Format__c"

summary_unsubscribe_queries = {
  'moz-unsubs': "INSERT INTO %s (%s) SELECT 'Mozilla Unsubscribes', COUNT(*), %s FROM sfdc.contact_history_vw LEFT JOIN sfdc.contacts_vw ON (ContactId=Id) WHERE DATE(contact_history_vw.CreatedDate)=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND contacts_vw.snapshot_date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND LOWER(OldValue)='true' AND LOWER(NewValue)='false' AND Field='Mozilla' GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),

  'fx-unsubs': "INSERT INTO %s (%s) SELECT 'Firefox Unsubscribes', COUNT(*), %s FROM sfdc.contact_history_vw LEFT JOIN sfdc.contacts_vw ON (ContactId=Id) WHERE DATE(contact_history_vw.CreatedDate)=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND contacts_vw.snapshot_date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND LOWER(OldValue)='true' AND LOWER(NewValue)='false' AND Field='Firefox' GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),

  'other-unsubs': "INSERT INTO %s (%s) SELECT 'Other Unsubscribes', COUNT(*), %s FROM sfdc.contact_history_vw LEFT JOIN sfdc.contacts_vw ON (ContactId=Id) WHERE DATE(contact_history_vw.CreatedDate)=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND contacts_vw.snapshot_date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND LOWER(OldValue)='true' AND LOWER(NewValue)='false' AND Field='Other' GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),

  'dev-unsubs': "INSERT INTO %s (%s) SELECT 'Developer Unsubscribes', COUNT(*), %s FROM sfdc.contact_history_vw LEFT JOIN sfdc.contacts_vw ON (ContactId=Id) WHERE DATE(contact_history_vw.CreatedDate)=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND contacts_vw.snapshot_date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND LOWER(OldValue)='true' AND LOWER(NewValue)='false' AND Field='Developer' GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),

  'moz-labs-unsubs': "INSERT INTO %s (%s) SELECT 'Mozilla Labs Unsubscribes', COUNT(*), %s FROM sfdc.contact_history_vw LEFT JOIN sfdc.contacts_vw ON (ContactId=Id) WHERE DATE(contact_history_vw.CreatedDate)=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND contacts_vw.snapshot_date=DATE_SUB( CURRENT_DATE(), INTERVAL 1 DAY) AND LOWER(OldValue)='true' AND LOWER(NewValue)='false' AND Field='Mozilla_Labs' GROUP BY %s" % (table_name, insert_fields, select_fields, select_fields),
}

sub_ops = {}
previous_job = None
for query_name in summary_subscribe_queries:
  sub_ops[query_name] = BashOperator(
                        task_id='sfdc-daily-bq-insert-' + query_name,
                        bash_command='bq query --use_legacy_sql=FALSE "%s"' % summary_subscribe_queries[query_name],
                        dag=dag)
  #
  # make each job dependent on the last so they don't all run at once,
  # otherwise bigquery has a sad
  #
  if previous_job:
    sub_ops[query_name].set_upstream(previous_job)
  else:
    sub_ops[query_name].set_upstream(delete_query_sub)

  previous_job = sub_ops[query_name]

unsub_ops = {}
previous_job = None
for query_name in summary_unsubscribe_queries:
  unsub_ops[query_name] = BashOperator(
                        task_id='sfdc-daily-bq-insert-' + query_name,
                        bash_command='bq query --use_legacy_sql=FALSE "%s"' % summary_unsubscribe_queries[query_name],
                        dag=dag)
  if previous_job:
    unsub_ops[query_name].set_upstream(previous_job)
  else:
    unsub_ops[query_name].set_upstream(delete_query_unsub)

  previous_job = unsub_ops[query_name]
