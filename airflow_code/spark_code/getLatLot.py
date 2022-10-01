import requests
from datetime import datetime
import pandas as pd

# from pyproj import Transformer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
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
    # api => https://developers.kakao.com/product/map
    # api 주소, 키
    api_keys = ['3ede87edc2f779bef86eca021e732474', '61f8ef2ccb4625e5add4ebc5caa78817']
    api_add = 'https://dapi.kakao.com/v2/local/search/address.json'
    transformer = Transformer.from_crs("epsg:2097", "epsg:4326")


    # kakao API를 이용해 주소의 lot, lat, b_code를 dict의 형태로 retrun
    def kakao_location(add):
        # api 설정
        url = api_add# + '?query=' + add
        headers = {"Authorization": f"KakaoAK {api_keys[1]}"}
        query = {'query': add}

        # api result
        result_json = json.loads(str(requests.get(url, headers=headers, params=query).text))

        result_json = dict(result_json)

        # api로 주소의 data를 받았는데 data가 없는 경우
        if len(result_json['documents']) == 0:
            # '충청남도 공주시 금흥동 ....'이라면
            # '공주시 금흥동' 으로 검색하도록
            add1 = add.split(' ')
            if len(add1) < 3:
                dict_fail = [None, None]
                return dict_fail
            qu = f'{add1[0]} {add1[1]} {add1[2]} {add1[3]}'
            query = {'query': qu}
            result_json = json.loads(str(requests.get(url, headers=headers, params=query).text))

        # 위의 두 방법 모두 실패했을 때
        # kakao API로 검색할 수 없는 주소라고 판단하고 좌표값과 법정동코드를 NULL값으로
        if len(result_json['documents']) == 0:
            dict_fail = [None, None]
            return dict_fail

        addr = result_json['documents'][0]['address']

        # 검색이 제대로 된경우 좌표값 반환
        result = [addr['y'], addr['x']]
        return result

    # Tm좌표계 epsg:2097을 위경도 epsg:4326으로 변경해주는 함수
    def tmTrans(x, y):
        loca = transformer.transform(y, x)

        lat = round(float(loca[0]) - 0.00007, 6)
        lot = round(float(loca[1]) + 0.00285, 6)
        result = [lat, lot]
        return result


    def addCoordinate(loca):
        load_loca = f"/data/strProcess/{loca}/"
        df = spark.read.option("header", "true").csv(load_loca+"part-00000*")
        df_pd = df.toPandas()

        trans_result = [None, None]
        list_id, list_lat, list_lot, list_road = list(), list(), list(), list()
        list_num, list_X, list_Y, list_add, list_road = df_pd.id, df_pd.lat, df_pd.lot, df_pd.s_add, df_pd.s_road
        for i in range(len(list_num)):
            if list_X[i] != None:
                if float(list_X[i]) > 40.0:
                    trans_result = tmTrans(list_X[i], list_Y[i])
                else:
                    trans_result = [list_X[i], list_Y[i]]
            else:
                if list_add[i] != None:
                    trans_result = kakao_location(list_add[i])
                elif list_road[i] != None:
                    trans_result = kakao_location(list_road[i])
            if i % 100 == 0:
                print(loca + str(i))

            list_lat.append(trans_result[0])
            list_lot.append(trans_result[1])
            list_id.append(list_num[i])

        print(len(list_id), len(list_lat), len(list_lot))
        td = {'id': list_id, 'lat': list_lat, 'lot': list_lot}
        pd_df = pd.DataFrame(td)
        df_data = spark.createDataFrame(pd_df)

        df.createOrReplaceTempView('df')
        df_data.createOrReplaceTempView('df_data')
        df2 = spark.sql("""select cast(df.id as int) as id, df.s_name, df.s_add, df.s_road, df.s_kind, 
                            df_data.lat, df_data.lot, df.s_status 
                            from df join df_data on (df.id = df_data.id) order by cast(id as int) asc""")

        save_loca = f"/data/addCoordinate/{loca}"
        df2.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true"). \
            option("encoding", "UTF8").save(save_loca)
        print(f'Success {loca} Save')

    def devSchema(loca):
        devColumns = [
            StructField("id", IntegerType()),
            StructField("s_name", StringType()),
            StructField("s_add", StringType()),
            StructField("s_road", StringType()),
            StructField("s_kind", StringType()),
            StructField("lat", FloatType()),
            StructField("lot", FloatType()),
            StructField("s_status", StringType()),
        ]
        devSchema = StructType(devColumns)

        load_loca = f"/data/addCoordinate/{loca}/"
        df = spark.read.schema(devSchema).option("header", "true").csv(load_loca + "part-00000*")

        save_loca = f"/data/devSchema/{loca}"
        df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true"). \
            option("encoding", "UTF8").save(save_loca)

    if __name__ == '__main__':
        spark = SparkSession.builder.master('local[1]').appName('getLocation').getOrCreate()
        json_names = ['Seoul', 'Gyeonggi', 'Incheon']
        for name in json_names:
            addCoordinate(name)
            devSchema(name)
    '''
    
    dbgout("getLatLot SUCCESS")
except Exception as ex:
    dbgout(f"getLatLot FAIL -> {str(ex)}")
