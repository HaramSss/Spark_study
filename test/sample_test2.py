import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import date_format, to_timestamp, datediff, to_date

spark = SparkSession.builder.appName("sample_test2").getOrCreate()
df = spark.read.csv("./Updated_Group_Activity_Data.csv", header=True, inferSchema=True)
# df.printSchema()
# df.show()

# - **목적**: 플랫폼의 사용자 유지율을 파악하고 개선 방안 도출.
# - 신규 사용자(가입 1개월) 중 모임 참여율.
# response : fail 제거, time -> yyyy-MM <공통> / 모임 가입 비율
df_del = df.filter(df.response != "{'result': 'failure'}")
df_trans = (
    df_del
    .withColumn("time", date_format(to_timestamp("time"), "yyyy-MM"))
    .withColumn("signupTime", date_format(to_timestamp("signupTime"), "yyyy-MM"))
    .withColumn("participationDate", date_format(to_timestamp("participationDate"), "yyyy-MM"))
)

# 가입한 지 1개월 이내 사용자 필터링
df_new_member = df_trans.withColumn("new_signup", datediff(to_date(F.col("time")), to_date(F.col("signupTime"))))
df_new_member.show()

# - 기존 사용자(가입 6개월 이상) 활동(최근 2개월) 비율.


# - 특정 기간(3개월) 동안 모임 참여를 계속한 사용자 비율.

