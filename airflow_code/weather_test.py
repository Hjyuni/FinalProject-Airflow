import os
import requests
import pendulum
import pandas as pd
from anyio import TASK_STATUS_IGNORED
from numpy import apply_over_axes
from bs4 import BeautifulSoup as bs
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from mod.slackbot import Slack

# 서울 시간대로 맞추기
kst = pendulum.timezone('Asia/Seoul')
sm = Slack('#msg')
"""
depends_on_past: 이전 날짜의 task 인스턴스 중에서 동일한 task인스턴스가 실패한 경우 실행되지 않고 대기
wait_for_downstream: 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스 중 하나라도 실패한 경우 해당 DAG는 실행되지 않고 대기
"""

# 기본 인수
default_args = {
    "owner" : "admin",
    "depends_on_past" : False,
    "wait_for_downstream" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}

dag = DAG(
    dag_id='getWeather',
    default_args=default_args,
    # 매일 1시부터 2시간씩 날씨 가져오기
    schedule_interval='0 1-23/2 * * *',
    start_date=datetime(2022, 5, 10, tzinfo=kst),
    end_date=datetime(2022, 5, 14, tzinfo=kst),
    catchup=False
)

def getTime(hour):
    """시간"""
    # hour = int(hour)
    # am 1시부터 2시간 간격 시작
    if hour == '01':
        temp_hour ='00'
    elif hour == '03':
        temp_hour = '02'
    elif hour == '05':
        temp_hour = '04'
    elif hour == '07':
        temp_hour = '06'
    elif hour == '09':
        temp_hour = '08'
    elif hour == '11':
        temp_hour = '10'
    elif hour == '13':
        temp_hour = '12'
    elif hour == '15':
        temp_hour = '14'
    elif hour == '17':
        temp_hour = '16'
    elif hour == '19':
        temp_hour = '18'
    elif hour == '21':
        temp_hour = '20'
    elif hour == '23':
        temp_hour = '22'        
    
    return temp_hour + '30'

def getWeather():
    """날씨 데이터"""
    try:
        key = ""
        now = datetime.now()
        now_hour = now.strftime('%H')
        base_date = now.strftime('%Y%m%d')
        base_time = getTime(now_hour)
        # 서울(nx, ny)
        nx=60
        ny=127

        url="https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst?serviceKey={}&pageNo=1&numOfRows=1000&dataType=JSON&base_date={}&base_time={}&nx={}&ny={}".format(key, base_date, base_time, nx, ny)

        # T1H: 기온
        # SKY: 하늘 -> (1)맑음, (2)구름많음, (3)흐림
        # PTY: 강수형태 -> (0)없음, (1)비, (2)비/눈, (3)눈, (5)빗방울->비, (6)빗방울눈날림->비/눈, (7)눈날림->눈

        resp = requests.get(url,verify=False)
        resp.encoding = "UTF-8"
        weather_df = pd.DataFrame(resp.json()['response']['body']['items']['item'])
        weather_df=weather_df[weather_df['category'].isin(['PTY','SKY','T1H'])]
        weather_df=weather_df[['category','fcstDate','fcstTime','fcstValue']]

        # 강수형태 df
        rain_df=pd.DataFrame({'fcstValue':[0,1,2,3,5,6,7],'rain':['비/눈 없음','비','비/눈','눈','비','비/눈','눈']})
        # 구름 df
        cloud_df=pd.DataFrame({'fcstValue':[1,3,4],'cloud':['맑음','구름많음','흐림']})

        # csv로 저장하기
        weather_df.to_csv('/home/hjyoon/final_data/weather_df.csv',mode='w',index=False)
        rain_df.to_csv('rain_df.csv',mode='w',index=False)
        cloud_df.to_csv('/home/hjyoon/final_data/cloud_df.csv',mode='w',index=False)
        sm.dbgout("getWeather DONE!")
    except Exception as ex:
        sm.dbgout(f'getWeather FAIL!->{str(ex)}')

def hdfsmsg():
    try:
        # aws에 맞는 경로로 바꿔야 함
        os.system('hdfs dfs -copyFromLocal -f /home/hjyoon/final_data /home')
        sm.dbgout("HDFS PUT DONE")
    except Exception as ex:
        sm.dbgout(f"HDFS PUT FAIL! -> {str(ex)}")

#=================================================
#                      Python                    #
#=================================================

getWeather = PythonOperator(
    task_id='getWeather',
    python_callable=getWeather,
    dag=dag
)

#=================================================
#                      Hadoop                    #
#=================================================

checkJPS = BashOperator(
    task_id='checkJPS',
    bash_command="""
                    if [ -z $(jps | grep "ResourceManager") ] || [ -z $(jps | grep NodeManager) ]
                    then echo "Not Running"; start-yarn.sh
                    else echo "Running"
                    fi

                    if [ -z $(jps | grep "NameNode") ] || [ -z $(jps | grep "DataNode") ]
                    then echo "Not Running"; start-dfs.sh
                    else echo "Running"
                    fi
                """
)

toHDFS = PythonOperator(
    task_id='toHDFS',
    python_callable=hdfsmsg,
    dag=dag
)

#=================================================
#                      Spark                     #
#=================================================

Weather = SparkSubmitOperator(
    task_id='getWeather',
    # aws 경로로 바꿔줘야 함
    application='/home/hjyoon/airflow/dags/spark_code/getWeather.py',
    conn_id='spark_default',
    dag=dag
)

#=================================================
#                      Slack                     #
#=================================================

slack = PythonOperator(
    task_id='sendmsg',
    python_callable=sm.dbgout,
    op_args=['ALL DONE!'],
    dag=dag
)

getWeather >> checkJPS >> toHDFS >> Weather >> slack
