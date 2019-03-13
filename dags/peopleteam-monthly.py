#
# TODO: A lot of hardcoded paths (eg- bucket names) to be parameterized
#
import datetime
from dateutil.relativedelta import *

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/imposing-union-227917/data-integrations:20190307'

wd_u_secret = secret.Secret(
    deploy_type='env',
    deploy_target='HR_DASHBOARD_WORKDAY_USERNAME',
    secret='di-secrets',
    key='HR_DASHBOARD_WORKDAY_USERNAME')
wd_p_secret = secret.Secret(
    deploy_type='env',
    deploy_target='HR_DASHBOARD_WORKDAY_PASSWORD',
    secret='di-secrets',
    key='HR_DASHBOARD_WORKDAY_PASSWORD')

# consider putting *all* the variables (urls, etc) in "secrets" and get rid of configmap altogether
config_volume = volume.Volume(
            name = 'configs',
            configs = {
              'configMap': {
                'name': 'data-integrations-dev',
                'items': [
                  { 'key': 'secrets_workday.py', 'path': 'secrets_workday.py', },
                  { 'key': 'secrets_util.py',     'path': 'secrets_util.py',     },
                ],
              },
            }
          )
config_mount = volume_mount.VolumeMount(
    name = 'configs',
    mount_path = '/configs',
    sub_path=None,
    read_only = True)

# Having this dynamic is not a good idea because retrying subtasks
# at a later date will fail
pull_date = datetime.datetime.now().strftime('%Y-%m-%d')

default_args = {
          'retries': 5,
          'retry_delay': datetime.timedelta(minutes=5),
          #'end_date': datetime.datetime(2016,10,1),
          'start_date': datetime.datetime(2016,9,1)
          # TODO: email stuff
          #'catchup': False,
}

dag = DAG(dag_id='peopleteam-monthly',
          default_args=default_args,
          schedule_interval='0 16 1 * *'
)

# Because airflow won't run 2019-02-01's monthly job until 2019-03-01, we need to
# pass next month's exeution datetime (minus 1 day) to our scripts like:
# {{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}
# I think the newest airflow will let you do:
# {{ macros.ds_add(next_ds, -1) }}
# but Google's not using that version yet
#
peopleteam_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='peopleteam-monthly-fetcher',
        name='peopleteam-monthly-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[wd_u_secret,wd_p_secret],
        # TODO: move the copying stuff to the di module itself
        cmds=['sh', '-c', '/usr/local/bin/get_people_dashboard_data.py --monthly -o /tmp/ --date {{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }} && gsutil cp /tmp/*_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv gs://moz-it-data-dp2-incoming-dev/peopleteam_dashboard_monthly/pull_date_' + pull_date + '/'],
        #cmds=['sh', '-c', 'sleep 7200'],
        volumes=[config_volume],
        volume_mounts=[config_mount],
        dag=dag)

peopleteam_hires_load = BashOperator(
        task_id='peopleteam-monthly-hires-load',

        # TODO: figure out a better way to pass in cluster name (airflow variables? does that help?)
        # maybe make a dependencies dir under "dags" and import a module that is just vars? see:
        #    https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies
        bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/peopleteam_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/peopleteam_dashboard_monthly/pull_date_' + pull_date + '/hires_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv gs://moz-it-data-dp2-peopleteam-dev/peopleteam_dashboard_monthly',
        dag=dag)

peopleteam_terms_load = BashOperator(
        task_id='peopleteam-monthly-terminations-load',
        bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/peopleteam_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/peopleteam_dashboard_monthly/pull_date_' + pull_date + '/terminations_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv gs://moz-it-data-dp2-peopleteam-dev/peopleteam_dashboard_monthly',
        dag=dag)

peopleteam_hdcnt_load = BashOperator(
        task_id='peopleteam-monthly-headcount-load',
        bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/peopleteam_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/peopleteam_dashboard_monthly/pull_date_' + pull_date + '/headcount_{{ macros.ds_add(next_execution_date.strftime("%Y-%m-%d"), -1) }}.csv gs://moz-it-data-dp2-peopleteam-dev/peopleteam_dashboard_monthly',
        dag=dag)

peopleteam_hires_load.set_upstream(peopleteam_fetch)
peopleteam_terms_load.set_upstream(peopleteam_fetch)
peopleteam_hdcnt_load.set_upstream(peopleteam_fetch)
