import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import date_format, to_timestamp

spark = SparkSession.builder.appName("sample_test").getOrCreate()
df = spark.read.csv("./log_data_with_groupid_100k.csv", header=True, inferSchema=True)
# df.printSchema()
# df.show()

# - **목적**: 플랫폼 전체 성과를 파악하고 성장률 분석.
# 1 - 월별 생성된 모임 수.
# response : fail 제거 / time -> yyyy-MM / time별
df_fail = df.filter(df.response != "{'result': failure}")

df_trformed = df_fail.withColumn("time", date_format(to_timestamp("time"), "yyyy-MM"))
# df_trformed.show()

# 새로 생성된 모임 : post 중 /api/v1/group
df_create = df_trformed.filter((df.method == "POST") & (df.url == "/api/v1/group"))

df_mon_create = df_create.groupBy("time").agg(F.countDistinct("groupId").alias("new_group_count"))
# df_mon_create.show()

# 2 - 월별 평균 참여자 수 = 가입된 총 멤버 수 -> 평균
# groupId별 / method : delete 제거 / count (groupId별, time별)
df_del = df_trformed.filter(df_trformed.method != "DELETE")
# df_del.show()

df_mon_count = df_del.groupBy("groupId", "time").count()
df_mon_avg = df_mon_count.groupBy("time").agg(F.avg("count").alias("mon_avg"))


