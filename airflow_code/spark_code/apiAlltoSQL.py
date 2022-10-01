import requests
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

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
    '''
    json_names = ['Seoul', 'Gyeonggi', 'Incheon']

    def save_mysql():
        devColumns = [
            StructField("id", IntegerType()),
            StructField("s_name", StringType()),
            StructField("s_add", StringType()),
            StructField("s_road", StringType()),
            StructField("s_kind", StringType()),
            StructField("lat", FloatType()),
            StructField("lot", FloatType()),
            StructField("s_status", StringType())
        ]
        devSchema = StructType(devColumns)

        result = pd.DataFrame()

        for loca in json_names:
            load_loca = f"/data/devSchema/{loca}/"
            df = spark.read.schema(devSchema).option("header", "true").csv(load_loca + "part-00000*")
            local = df.toPandas()
            result = pd.concat([result, local])  # df 합치기
            print(f'Success {loca}')
        print(f'Success Total')
        df = spark.createDataFrame(result)

        df = df.drop(df.id)
        df = df.toPandas()

        df2 = df.rename_axis('id').reset_index()
        df2 = df2.astype(object).where(pd.notnull(df2), None)
        df3 = spark.createDataFrame(df2)

        devColumns = [
            StructField("id", IntegerType()),
            StructField("s_name", StringType()),
            StructField("s_add", StringType()),
            StructField("s_road", StringType()),
            StructField("s_kind", StringType()),
            StructField("lat", FloatType()),
            StructField("lot", FloatType()),
            StructField("s_status", StringType()),
            StructField("modification_time", TimestampType())
        ]
        devSchema = StructType(devColumns)
        df3 = df3.withColumn("modification_time", current_timestamp())

        save_loca = f"/data/total_rest"
        df3.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true"). \
            option("encoding", "UTF8").save(save_loca)
        print(f'Success {loca} Save')

        df_data = spark.read.schema(devSchema).option("header", "true").csv(save_loca + "/part-00000*")

        user = "root"
        password = "1234"
        url="jdbc:mysql://localhost:3306/meok4"
        driver = "com.mysql.cj.jdbc.Driver"
        dbtable = 'store_store'

        df_data.write.jdbc(url, dbtable, "overwrite", properties={"driver": driver, "user": user, "password": password})

    if __name__ =='__main__':
        spark = SparkSession.builder.master('yarn').appName('jsonToMysql').getOrCreate()
        save_mysql()
    '''
    
    dbgout("apiAlltoSQL SUCCESS")
except Exception as ex:
    dbgout(f"apiAlltoSQL FAIL -> {str(ex)}")
