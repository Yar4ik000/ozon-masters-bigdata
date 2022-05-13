from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.sensors.filesystem import FileSensor
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

dsenv = '/opt/conda/envs/dsenv/bin/python'
base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'
train = '/datasets/amazon/all_reviews_5_core_train_extra_small_sentiment.json'
train_out = 'user/Yar4ik000/Yar4ik000_train_out.parquet'
test = '/datasets/amazon/all_reviews_5_core_test_extra_small_features.json'
test_out = 'user/Yar4ik000/Yar4ik000_test_out.parquet'
prediction = 'Yar4ik000_hw6_prediction'


with DAG(
        'Yar4ik000_dag',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2022, 5, 13)
        ) as dag:
    feature_eng_task_train = SparkSubmitOperator(
            task_id='train_engineering',
            env_vars={'PYSPARK_PYTHON': dsenv},
            application=f'{base_dir}/engineering.py',
            application_args=[train, train_out],
            spark_binary='/usr/bin/spark-submit')
    
    feature_eng_task_test = SparkSubmitOperator(
            task_id='test_engineering',
            env_vars={'PYSPARK_PYTHON': dsenv},
            application=f'{base_dir}/engineering.py',
            application_args=[test, test_out],
            spark_binary='/usr/bin/spark-submit')    
    
    train_download_command = f'hdfs dfs -get {train_out} {base_dir}/Yar4ik000_train_out_local.parquet'
    train_download_task = BashOperator(
            task_id='train_download',
            bash_command=train_download_command)

    train_task_command = f'{dsenv} {base_dir}/model.py {base_dir}/Yar4ik000_train_out_local.parquet {base_dir}/6.joblib' 

    train_task = BashOperator(
            task_id='train_task',
            bash_command=train_task_command)

    model_sensor = FileSensor(
            task_id='model_sensor',
            filepath=f'{base_dir}/6.joblib')

    predict_task = SparkSubmitOperator(
            task_id='predict_task',
            env_vars={'PYSPARK_PYTHON': dsenv},
            application=f'{base_dir}/predict.py',
            application_args=[test_out, prediction, f'{base_dir}/6.joblib'],
            spark_binary='/usr/bin/spark-submit')

    feature_eng_task_train >> feature_eng_task_test >> train_download_task >> train_task >> model_sensor >> predict_task

