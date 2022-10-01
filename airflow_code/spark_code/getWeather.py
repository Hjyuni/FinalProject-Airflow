import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

myToken = ''

def post_message(token, channel, text) :
    response = requests.post('https://slack.com/api/chat.postMessage',
                            headers = {'Authorization' : 'Bearer '+token},
                            data = {'channel' : channel, 'text' : text})
    print(response)

# 오류메세지
def dbgout(message):
    strbuf = datetime.now().strftime('[%m/%d %H:%M:%S] ') + message
    post_message(myToken, '#msg', strbuf)

try:
    spark = SparkSession.builder.master("yarn").appName("weather").getOrCreate()

    weather_df=spark.read.option('header','True').csv('/home/final_data/weather_df.csv')
    weather_df.createOrReplaceTempView("weather_df")

    rain_df=spark.read.option('header','True').csv('/home/final_data/rain_df.csv')
    rain_df.createOrReplaceTempView("rain_df")
    rain_df = spark.sql("select fcstDate,fcstTime,rain from weather_df w JOIN rain_df r ON(w.fcstValue=r.fcstValue) where category='PTY'")
    rain_df.createOrReplaceTempView("rain_df")

    cloud_df=spark.read.option('header','True').csv('/home/final_data/cloud_df.csv')
    cloud_df.createOrReplaceTempView("cloud_df")
    cloud_df = spark.sql("select fcstDate,fcstTime,cloud from weather_df w JOIN cloud_df c ON(w.fcstValue=c.fcstValue) where category='SKY'")
    cloud_df.createOrReplaceTempView("cloud_df")

    temp_df=spark.sql("select fcstDate,fcstTime,fcstValue as temp from weather_df where category='T1H'")
    temp_df.createOrReplaceTempView("temp_df")

    weather_df = spark.sql("select t.fcstDate,t.fcstTime,t.temp,c.cloud,r.rain from temp_df t JOIN cloud_df c ON(t.fcstTime=c.fcstTime) JOIN rain_df r ON(c.fcstTime=r.fcstTime)")
    weather_df.createOrReplaceTempView("weather_df")
    weather_df = weather_df.withColumn("id",monotonically_increasing_id())
    weather_df.createOrReplaceTempView("weather_df")
    

    #  리뷰 테이블 mysql에 넣기
    user="root"
    password="1234"
    url="jdbc:mysql://localhost:3306/meok4"
    driver="com.mysql.cj.jdbc.Driver"
    dbtable="store_weather"

    weather_df.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})

    dbgout("weather SPARK SUBMIT SUCCESS")

except Exception as ex:
    dbgout(f"total_review SPARK SUBMIT FAIL -> {str(ex)}")
