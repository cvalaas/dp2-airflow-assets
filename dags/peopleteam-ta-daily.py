#
# TODO: A lot of hardcoded paths (eg- bucket names) to be parameterized
#
import datetime
import os
from dateutil.relativedelta import *

from airflow import DAG
from airflow.contrib.kubernetes import pod
from airflow.contrib.kubernetes import secret, volume, volume_mount
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

#PROJECT=os.environ.get('gcp_project')

PROJECT=Variable.get('gcp_project')
LATEST_TAG=Variable.get('latest_di_image_tag')

# TODO: put the latest SHA hash in a bucket somewhere and pull from there
IMAGE='gcr.io/' + PROJECT + '/data-integrations:' + LATEST_TAG


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

# Having this dynamic is not a good idea because retrying subtasks
# at a later date will fail
pull_date = datetime.datetime.now().strftime('%Y-%m-%d')

default_args = {
          'retries': 5,
          'retry_delay': datetime.timedelta(minutes=5),
          #'end_date': datetime.datetime(2016,10,1),
          'start_date': datetime.datetime(2019,4,15)
          # TODO: email stuff
          #'catchup': False,
}

dag = DAG(dag_id='peopleteam-ta-daily',
          default_args=default_args,
          schedule_interval='0 16 * * *'
)

peopleteam_fetch = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='peopleteam-ta-daily-fetcher',
        name='peopleteam-ta-daily-fetcher',
        namespace='default',
        image=IMAGE,
        secrets=[wd_u_secret,wd_p_secret],
        # TODO: move the copying stuff to the di module itself
        cmds=['sh', '-c', '/usr/local/bin/get_people_dashboard_data.py --ta-dashboard -o /tmp/ --date {{ ds }} && gsutil cp /tmp/*_{{ ds }}.csv gs://' + PROJECT + '-data-incoming/ta_dashboard/pull_date_' + pull_date + '/'],
        #cmds=['sh', '-c', 'sleep 7200'],
        dag=dag)

peopleteam_hires_load = BashOperator(
        task_id='peopleteam-ta-dashboard-hires-load',

        # TODO: figure out a better way to pass in cluster name (airflow variables? does that help?)
        # maybe make a dependencies dir under "dags" and import a module that is just vars? see:
        #    https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies
        bash_command='bq load --source_format CSV --autodetect --skip_leading_rows=1 --replace workday.workday_ta_hires gs://' + PROJECT + '-data-incoming/ta_dashboard/pull_date_' + pull_date + '/hires_{{ ds }}.csv',
        #bash_command='gcloud dataproc jobs submit pyspark gs://moz-it-data-dp2-sparkjobs-dev/etl/peopleteam_loader.py --cluster=etl-cluster --region us-central1 -- gs://moz-it-data-dp2-incoming-dev/ta_dashboard/pull_date_' + pull_date + '/hires_{{ ds }}.csv gs://moz-it-data-dp2-peopleteam-dev/ta_dashabord',
        dag=dag)

peopleteam_hires_load.set_upstream(peopleteam_fetch)
