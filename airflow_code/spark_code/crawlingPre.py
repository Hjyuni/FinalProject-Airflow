import os
import json
import requests
from datetime import datetime
import pandas as pd
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
    """
    # location = './data/'
    location = '/home/ubuntu/git/Final_Team04/data/crawling/'

    # json_file
    def replaceGap(json_file):
        json_str = json.dumps(json_file, ensure_ascii=False)
        new_str = json_str.replace('\\""', '"')
        new_str = new_str.replace('""', "null")
        new_str = json.loads(new_str)
        return new_str


    def change():
        result = dict()
        for i in range(124500):
            result[i] = dict()

        print(result[0])
        # csv_text = pd.read_csv('./data/id/mix_id.csv')
        load_loca = "/mix_id_all.csv"

        devColumns = [
            StructField("total_id", IntegerType()),
            StructField("s_id", IntegerType()),
            StructField("s_tel", StringType()),
        ]
        devSchema = StructType(devColumns)

        csv_text = spark.read.schema(devSchema).option("header", "true").csv(load_loca).toPandas()

        site_name = ['dining', 'mango', 'naver']
        for file_name in site_name:
            file_list = os.listdir(location + file_name)
            print(len(file_list))
            cnt = 0
            for file in file_list:
                fn = f'{location}{file_name}/'
                with open(fn+file, 'r', encoding='utf8') as f:
                    json_file = json.load(f)

                if file_name == 'mango':
                    json_file = json_file['store']
                elif file_name == 'dining':
                    json_file = replaceGap(json_file)

                for id in json_file:
                    menus = list()
                    prices = list()
                    # print(id['s_menu'])
                    try:
                        if id['s_menu'] != None:
                            for menu in id['s_menu']:
                                key = list(menu.keys())
                                menus.append(key[0])
                                prices.append(menu[key[0]])
                    except KeyError:
                        pass
                    except AttributeError:
                        menus = id['s_menu']
                        prices = id['s_price']

                    id_int = int(id['id'])

                    try:
                        if file_name == 'mango':
                            cnt += 1
                            if cnt % 1000 == 0:
                                print(cnt)
                            str_expr = f's_id == {id_int}'
                            df_q = csv_text.query(str_expr)
                            id_int = int(df_q.iloc[0]['total_id'])
                    except IndexError:
                        continue

                    id_di = result[id_int]
                    id_di['id'] = id_int
                    try:
                        if file_name == 'dining':
                            if id['s_tel'] != None:
                                id_di['s_name'] = id['s_name']
                        else:
                            id_di['s_name'] = id['s_name']
                    except KeyError:
                        id_di['s_name'] = None
                    try:
                        id_di['s_tel'] = id['s_tel']
                    except KeyError:
                        id_di['s_tel'] = None
                    try:
                        id_di['s_etc'] = id['s_etc']
                    except KeyError:
                        id_di['s_etc'] = None
                    try:
                        id_di['s_hour'] = id['s_hour']
                    except KeyError:
                        id_di['s_hour'] = None
                    try:
                        content_dict = dict()
                        content_dict['content'] = id['s_photo']
                        id_di['s_photo'] = content_dict
                    except KeyError:
                        id_di['s_photo'] = None

                    if len(menus) != 0:
                        menu_dict, prices_dict = dict(), dict()
                        menu_dict['content'] = menus
                        prices_dict['content'] = prices
                        id_di['s_menu'] = menu_dict
                        id_di['s_price'] = prices_dict
                    else:
                        id_di['s_menu'] = None
                        id_di['s_price'] = None


                print('success '+file)

        result_list = list()
        for i in range(124500):
            result_list.append(result[i])

        # with open('./data/process/'+'total'+'_pro.json', 'w', encoding='utf8') as f:
        #     json.dump(result_list, f, ensure_ascii=False)

        devColumns = [
            StructField("id", IntegerType()),
            StructField("s_name", StringType()),
            StructField("s_tel", StringType()),
            StructField("s_photo", MapType(StringType(), ArrayType(StringType()))),
            StructField("s_hour", StringType()),
            StructField("s_etc", StringType()),
            StructField("s_menu", MapType(StringType(), ArrayType(StringType()))),
            StructField("s_price", MapType(StringType(), ArrayType(StringType())))
        ]
        devSchema = StructType(devColumns)

        df_data = spark.createDataFrame(result_list, schema=devSchema)
        save_local = f'/crawling/add/total'
        df_data.coalesce(1).write.format("json").mode("overwrite").json(save_local)

        print('save '+location+'total_pro.json')


    def dropNa():
        devColumns = [
            StructField("id", IntegerType()),
            StructField("s_name", StringType()),
            StructField("s_tel", StringType()),
            StructField("s_photo", StringType()),
            StructField("s_hour", StringType()),
            StructField("s_etc", StringType()),
            StructField("s_menu", StringType()),
            StructField("s_price", StringType()),
        ]
        devSchema = StructType(devColumns)

        load_local = f'/crawling/add/total/part-0000*'
        df = spark.read.schema(devSchema).json(load_local).dropna(subset='s_name')

        save_local  = f'/crawling/drop/total'
        df.coalesce(1).write.format("json").mode("overwrite").json(save_local)
        print('save total')

        user = "root"
        password = "1234"
        url = "jdbc:mysql://localhost:3306/meok4"
        driver = "com.mysql.cj.jdbc.Driver"
        dbtable = 'rest_detail'

        df.write.jdbc(url, dbtable, "overwrite", properties={"driver": driver, "user": user, "password": password})

    if __name__ == '__main__':
        spark = SparkSession.builder.master('yarn').appName('strProcess').getOrCreate()
        # site_list = ['naver', 'mango', 'dining']
        # for site in site_list:
        #     change(site)
        #     dropNa(site)
        # replaceGap()
        change()
        dropNa()
        """
    dbgout("crawlingPro SUCCESS!")
except Exception as ex:
    dbgout(f"crawlingPro FAIL -> {str(ex)}")
