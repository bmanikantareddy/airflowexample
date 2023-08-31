import json
import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator, PythonOperator
from airflow.utils.dates import days_ago

def check_smoke_exist(**kwargs):
    smokeexists = kwargs['dag_run'].conf.get('smokeexists', False)
    return 'smoke_test' if smokeexists else 'skip_create_cluster'

def check_remote_exist(**kwargs):
    remoteexists = kwargs['dag_run'].conf.get('remoteexists', False)
    return 'remote_dag_smoke_test' if remoteexists else 'skip_remote_dag_result'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    'branching_with_annotation',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    params={
        "smokeexists": False,
        "remoteexists": False
    }
)

def _get_cluster_branch(**kwargs):
    return "create_cluster"

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=_get_cluster_branch,
    provide_context=True,  # Add this line
    dag=dag,
)

get_info = DummyOperator(task_id='get_info', dag=dag)
create_image = DummyOperator(task_id='create_image', dag=dag)
create_cluster = DummyOperator(task_id='create_cluster', dag=dag)
skip_create_cluster = DummyOperator(task_id='skip_create_cluster', dag=dag)

#gcloud_test_start = DummyOperator(task_id='gcloud_test_start',trigger_rule='one_success', dag=dag)

def do_branching(**kwargs):
    smokeexists = kwargs['dag_run'].conf.get('smokeexists', False)
    remoteexists = kwargs['dag_run'].conf.get('remoteexists', False)
    print("smokeexists:",smokeexists)
    print("remoteexists:",remoteexists)
    if smokeexists and remoteexists:
        result = ["smoke_test", "remote_dag_smoke_test"]
    elif smokeexists and not remoteexists:
        result = "smoke_test"
    elif not smokeexists and remoteexists:
        result = "remote_dag_smoke_test"
    else:
        result = "skip_remote_and_smoke_test"
    print("result",result)
    return result

gcloud_test_start = BranchPythonOperator(
    task_id='gcloud_test_start',
    provide_context=True,
    python_callable=do_branching,
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

smoke_test = DummyOperator(task_id='smoke_test', dag=dag)
remote_dag_smoke_test = DummyOperator(task_id='remote_dag_smoke_test', dag=dag)

branch_remote_dag = BranchPythonOperator(
    task_id='branch_remote_dag',
    provide_context=True,
    python_callable=check_remote_exist,
    dag=dag,
)

def _check_smoke_test_exists(**kwargs):
    smokeexists = kwargs['dag_run'].conf.get('smokeexists', False)
    print()
    return True if smokeexists else False

def _check_remote_dag_exists(**kwargs):
    remoteexists = kwargs['dag_run'].conf.get('remoteexists', False)
    return True if remoteexists else False

check_smoke_test_exists = ShortCircuitOperator(
    task_id='check_smoke_test_exists',
    python_callable=_check_smoke_test_exists,
    provide_context=True,
    dag=dag,
)

check_remote_dag_exists = ShortCircuitOperator(
    task_id='check_remote_dag_exists',
    python_callable=_check_remote_dag_exists,
    provide_context=True,
    dag=dag,
)

def _check_remote_and_smoke_exists(**kwargs):
    smokeexists = kwargs['dag_run'].conf.get('smokeexists', False)
    remoteexists = kwargs['dag_run'].conf.get('remoteexists', False)
    print("smokeexists:",smokeexists)
    print("remoteexists:",remoteexists)
    return not (smokeexists or remoteexists)

# check_remote_and_smoke_exists = ShortCircuitOperator(
#     task_id='check_remote_and_smoke_exists',
#     python_callable=_check_remote_and_smoke_exists,
#     provide_context=True,
#     dag=dag,
# )
skip_remote_and_smoke_test = DummyOperator(task_id='skip_remote_and_smoke_test', dag=dag)

remote_dag_result = DummyOperator(task_id='remote_dag_result', dag=dag)
skip_remote_dag_result = DummyOperator(task_id='skip_remote_dag_result', dag=dag)

labels = DummyOperator(task_id='labels',trigger_rule='none_failed_min_one_success',dag=dag)
promote_image = DummyOperator(task_id='promote_image',trigger_rule='none_failed_min_one_success', dag=dag)
promote_common_image = DummyOperator(task_id='promote_common_image', trigger_rule='none_failed_min_one_success', dag=dag)

# # Set up dependencies
# get_info >> create_image >> branching >> [create_cluster, skip_create_cluster]
# [create_cluster, skip_create_cluster] >> gcloud_test_start
# gcloud_test_start >> [smoke_test, remote_dag_smoke_test]
# remote_dag_smoke_test >> branch_remote_dag
# branch_remote_dag >> [remote_dag_result, skip_remote_dag_result]
# [smoke_test, remote_dag_smoke_test] >> labels
# [remote_dag_result, skip_remote_dag_result] >> labels
# labels >> [promote_image, promote_common_image]


# # Set up dependencies
# get_info >> create_image >> branching >> [create_cluster, skip_create_cluster] >> gcloud_test_start
# #gcloud_test_start << [create_cluster, skip_create_cluster]  # Reversed the direction of dependency
# gcloud_test_start >> [check_smoke_test_exists, check_remote_dag_exists, check_remote_and_smoke_exists]
# # gcloud_test_start >> [check_smoke_test_exists, check_remote_dag_exists]
# check_smoke_test_exists >> smoke_test >> labels
# check_remote_dag_exists >> remote_dag_smoke_test >> remote_dag_result >> labels
# # remote_dag_smoke_test >> branch_remote_dag
# # branch_remote_dag >> [remote_dag_result, skip_remote_dag_result]
# check_remote_and_smoke_exists >> labels
# #gcloud_test_start >> labels
# # smoke_test >> labels
# # [remote_dag_result, skip_remote_dag_result] >> labels
# labels >> [promote_image, promote_common_image]

get_info >> create_image >> branching >> [create_cluster, skip_create_cluster] >> gcloud_test_start
gcloud_test_start >> smoke_test >> labels
gcloud_test_start >> remote_dag_smoke_test >> remote_dag_result >> labels
gcloud_test_start >> skip_remote_and_smoke_test >> labels
labels >> [promote_image, promote_common_image]
