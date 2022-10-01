import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext
# from pymongo import MongoClient, GEOSPHERE

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
    client = MongoClient('localhost', 27017)
    db = client['test']

    def makeMongoSet():
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

        load_loca = "/data/total_rest"
        df = spark.read.schema(devSchema).option("header", "true").csv(load_loca + "/part-00000*")
        df.createOrReplaceTempView('df')

        sql = f'select id, s_name, s_add, s_road, lat, lot from df where lat is not NULL or lat != ""'
        df2 = spark.sql(sql)

        df3 = df2.collect()

        df_list = list()
        for row in df3:
            df_list2 = list()
            df_list2.append(row['lot'])
            df_list2.append(row['lat'])
            df_dict = {'id': (row['id']), 's_name': row['s_name'], 's_add': row['s_add'], 's_road': row['s_road'],
                    'location': {'type': 'Point', 'coordinates': df_list2}}
            df_list.append(df_dict)
            if row['id'] % 100 == 0:
                print(row['id'])

        rest_mongo = db['rest']
        rest_mongo.drop()
        rest_mongo = db['rest']
        rest_mongo.create_index([("location", GEOSPHERE)])

        rest_mongo.insert_many(df_list)

    if __name__ == '__main__':
        spark = SparkSession.builder.master('local[1]').appName('addMongodb').getOrCreate()
        makeMongoSet()
    '''
    
    dbgout("apiAlltoMongo SUCCESS")
except Exception as ex:
    dbgout(f"apiAlltoSMongo FAIL -> {str(ex)}")
