import requests
from datetime import datetime
import pandas as pd
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
    '''
    def jsonProcess(loca):
        if loca == 'Seoul':
            chk = 0
            location = '/data/apiSeoul/'         # hadoop에서 가지고올 파일 경로
            file_name = 'api_seoul'         # 위의 location안에 파일 이름
            sql_query = """SELECT BPLCNM as s_name, SITEWHLADDR as s_add, RDNWHLADDR as s_road, UPTAENM as s_kind,
                            X as lat, Y as lot, TRDSTATENM as s_status FROM local
                            WHERE TRDSTATEGBN = '01' AND (SITEWHLADDR is not null OR RDNWHLADDR is not null)"""
        elif loca == 'Gyeonggi':
            chk = 1
            location = '/data/apiGyeonggi/'      # hadoop에서 가지고올 파일 경로
            file_name = 'api_gyeonggi'      # 위의 location안에 파일 이름
            sql_query = """SELECT BIZPLC_NM as s_name, REFINE_LOTNO_ADDR as s_add, REFINE_ROADNM_ADDR as s_road, 
                            BIZCOND_DIV_NM_INFO as s_kind, REFINE_WGS84_LAT as lat, REFINE_WGS84_LOGT as lot, 
                            BSN_STATE_NM as s_status FROM local
                            WHERE BSN_STATE_DIV_CD = '01' AND (REFINE_LOTNO_ADDR is not null OR REFINE_ROADNM_ADDR is not null)"""
        else:
            print("Wrong value. Please enter 'Seoul' or 'Gyeonggi'")

        result = pd.DataFrame()             # 저장할 DF
        # 파일개수만큼 반복(5개라 5)
        for i in range(5):
            # json 읽기
            df_json = spark.read.json(location+file_name+str(i)+'.json', encoding='utf8')
            df_json.createOrReplaceTempView('local')
            local = spark.sql(sql_query).toPandas()
            result = pd.concat([result, local])             # df 합치기
        print(f'Success {loca} Total')
        df_data = spark.createDataFrame(result)             # pyspark df로 변경

        save_loca = f"/data/toCsv/{loca}"
        df_data.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true"). \
            option("encoding", "UTF8").save(save_loca)
        print(f'Success {loca} Save')


    if __name__ == '__main__':
        spark = SparkSession.builder.master('yarn').appName('jsonProcess').getOrCreate()
        json_names = ['Seoul', 'Gyeonggi']
        for name in json_names:
            jsonProcess(name)
            '''
    dbgout("apiFileMerge SUCCESS")
except Exception as ex:
    dbgout(f"apiFileMerge FAIL -> {str(ex)}")
