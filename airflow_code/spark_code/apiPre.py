import requests
from datetime import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

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
    """
    def addId(df_addId):
        df_addId.createOrReplaceTempView('addIdDf')
        sql_query = '''SELECT s_name, s_add, s_road, s_kind, lat, lot, s_status FROM addIdDf
                            WHERE lat is not null'''
        df = spark.sql(sql_query).toPandas()
        df2 = df.rename_axis('id').reset_index()
        df3 = spark.createDataFrame(df2)

    return df3

    def repBracket(s_name):
        op_Bracket = s_name.find('(')           # 여는 괄호 위치
        cl_Bracket = s_name.find(')')           # 닫는 괄호 위치
        if op_Bracket == -1:                    # 여는 괄호가 없다면
            op_Bracket = 0
        result = s_name[:op_Bracket]+s_name[cl_Bracket+1:]
        return result

    def addressSlice(s_add):
        if s_add == None:
            return s_add
        s_spl = s_add.split(' ')
        if len(s_spl) > 2:
            try:
                result = f'{s_spl[0]} {s_spl[1]} {s_spl[2]} {s_spl[3]} {s_spl[4]}'
            except:
                result = f'{s_spl[0]} {s_spl[1]} {s_spl[2]} {s_spl[3]}'
        else:
            result = s_add
        return result


    def strProcess(loca):
        if loca == 'Incheon':
            load_loca = f"/data/{loca}"
            df = spark.read.option("header", "true").csv(load_loca + ".csv")
        else:
            load_loca = f"/data/toCsv/{loca}/"
            df = spark.read.option("header", "true").csv(load_loca + "part-00000*")

        repBracket_udf = udf(repBracket, StringType())
        addressSlice_udf = udf(addressSlice, StringType())
        df = df.withColumn('s_name', repBracket_udf(df.s_name)).withColumn('s_add', addressSlice_udf(df.s_add))
        if loca == 'Incheon':
            df = df.withColumn('s_road', repBracket_udf(df.s_road))
        print(f'Success s_name')
        print(f'Success s_add')

        if loca != 'Incheon':
            df = addId(df)
            print(f'Success addId')

        save_loca = f"/data/strProcess/{loca}"
        df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true"). \
            option("encoding", "UTF8").save(save_loca)
        print(f'Success {loca} Save')



    if __name__ == '__main__':
        spark = SparkSession.builder.master('yarn').appName('strProcess').getOrCreate()
        json_names = ['Seoul', 'Gyeonggi', 'Incheon']
        for name in json_names:
            strProcess(name)
    
    """
    
    dbgout("apiPre SUCCESS")
except Exception as ex:
    dbgout(f"apiPre FAIL -> {str(ex)}")
