#-*- coding: utf-8 -*-
import re
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from functools import reduce
from pyspark.sql.functions import array

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

## siteName: kakao, naver, mango, dining
spark = SparkSession.builder.master("yarn").appName("review").getOrCreate()
def reviewPre(siteName):
	# 파일 읽어오기
	s_site = spark.read.json(f"/home/ubuntu/final_data/{siteName}_pro.json")
	s_site.createOrReplaceTempView(f"s_{siteName}")
	# s_review (array->str) 바꾸고 s_review null값 제거
	s_site = spark.sql(f"select id,cast(s_review as string) s_review from s_{siteName} where s_review is not null")
	s_site.createOrReplaceTempView(f"s_{siteName}")
	if siteName == "dining":
		s_site = spark.sql(f"select i.s_id, d.dining_review dining_review from id_file i JOIN s_{siteName} d ON (i.total_id=d.id)")
		s_site.createOrReplaceTempView(f"s_{siteName}")
	else:
		pass

	# s_review에 []제거
	regex_str = "\[|\]"
	s_site = f"s_{siteName}".select(col("id"),regexp_replace(col("s_review"), regex_str, "").alias("s_review"))
	s_site.createOrReplaceTempView(f"s_{siteName}")

	# s_review 전처리
	store = []
	for i in f"s_{siteName}".collect():
		# 한글, 숫자, 띄어쓰기, 탭, 줄바꿈, ' 제외-> " " 
		a = re.sub(r"[^0-9가-힣\s\']"," ",i["s_review"])
		a = re.sub("\s+", " ",a)
		a = a.strip()
		store.append((i["id"],a))

	# 전처리 된 값으로 df만들기
	s_site = spark.createDataFrame(store,("id",f"{siteName}_review"))
	s_site.createOrReplaceTempView(f"s_{siteName}")

	# 빈값, None값 없애기
	columns = set(f"s_{siteName}".columns) - set(['id'])
	cond = map(lambda x: (col(x).isNotNull()) & (col(x) != ""), columns)
	cond = reduce((lambda x, y: x & y), cond)
	s_site = f"s_{siteName}".filter(cond)
	s_site.createOrReplaceTempView(f"s_{siteName}")

	# seoul.csv랑 id 맞추기
	id_file = spark.read.option("header","true").csv("/home/ubuntu/final_data/mix_id.csv")
	id_file.createOrReplaceTempView("id_file")

	s_site = spark.sql(f"select i.s_id, {siteName}.{siteName}_review {siteName}_review from id_file i JOIN s_{siteName} {siteName} ON (i.total_id={siteName}.id)")
	s_site.createOrReplaceTempView(f"s_{siteName}")

	# 최종 각 사이트 리뷰데이터 저장
	# s_site.write.format("json").mode("overwrite").save(f"/home/ubuntu/final_data/review/{siteName}")
	# hdfs dfs -getmerge /home/ubuntu/final_data/review/kakao /home/ubuntu/final_data/kakao_review.json
	# hdfs dfs -getmerge /home/ubuntu/final_data/review/naver /home/ubuntu/final_data/naver_review.json
	# hdfs dfs -getmerge /home/ubuntu/final_data/review/mango /home/ubuntu/final_data/mango_review.json
	# hdfs dfs -getmerge /home/ubuntu/final_data/review/dining /home/ubuntu/final_data/dining_review.json
try:
	'''
	for site in ['kakao','naver','dining','mango']:
		reviewPre(site)
	## total
	id = spark.range(122759).toDF("id")
	id.createOrReplaceTempView("id")

	tot_review = spark.sql("select distinct(i.id),k.kakao_review,d.dining_review,n.naver_review,m.mango_review from id i LEFT OUTER JOIN s_kakao k ON (int(k.s_id)=int(i.id)) LEFT OUTER JOIN s_naver n ON (int(i.id) = int(n.s_id)) LEFT OUTER JOIN s_dining d ON (int(i.id) = int(d.s_id)) LEFT OUTER JOIN s_mango m ON (int(i.id) = int(m.id)) order by i.id")
	tot_review.createOrReplaceTempView("tot_review")

	tot_review = tot_review.withColumn("s_review", array(tot_review.kakao_review, tot_review.naver_review, tot_review.dining_review, tot_review.mango_review))
	tot_review.createOrReplaceTempView("tot_review")
	tot_review = spark.sql("select id,cast(s_review as string) s_review from tot_review")
	tot_review.createOrReplaceTempView("tot_review")
	tot_review = spark.sql("select * from tot_review where s_review<>'[null, null, null, null]'")
	tot_review.createOrReplaceTempView("tot_review")

	# tot_review.write.format("json").mode("overwrite").save("/home/ubuntu/final_data/review/total")

	#  리뷰 테이블 mysql에 넣기
	user="root"
	password="1234"
	url="jdbc:mysql://localhost:3306/mukjalal"
	driver="com.mysql.cj.jdbc.Driver"
	dbtable="s_review"

	tot_review.write.mode("overwrite").option("truncate","true").jdbc(url, dbtable, properties={"driver": driver, "user": user, "password": password})
	'''
	pass
	dbgout(f"s_review SPARK-SUBMIT SUCCESS")
except Exception as ex:
	dbgout(f"s_review SPARK-SUBMIT FAIL -> {str(ex)}")
