import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import date_format, to_timestamp, datediff, to_date, lit, sum as Fsum, count as Fcount
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("sample_test2").getOrCreate()
df = spark.read.csv("./Updated_Group_Activity_Data.csv", header=True, inferSchema=True)
# df.printSchema()
# df.show()

# - **목적**: 플랫폼의 사용자 유지율을 파악하고 개선 방안 도출.
# 1. - 신규 사용자(가입 1개월) 중 모임 참여율.
# response : fail 제거, time -> yyyy-MM <공통> / 모임 가입 비율
df_del = df.filter(df.response != "{'result': 'failure'}")
df_trans = (
    df_del
    .withColumn("time", date_format(to_timestamp("time"), "yyyy-MM"))
    .withColumn("signupTime", date_format(to_timestamp("signupTime"), "yyyy-MM"))
    .withColumn("participationDate", date_format(to_timestamp("participationDate"), "yyyy-MM"))
)

# 가입한 지 1개월(new_signup) 이내 사용자 필터링
df_new_member = df_trans.withColumn(
    "new_signup",
    datediff(to_date(F.col("time")), to_date(F.col("signupTime")))
).filter(F.col("new_signup") <= 30)

# 모임 참여율 : 모임 참여 수 (participation)를 특정 그룹(groupId)의 전체 사용자 수로 나눈 값
df_participation_rate = (
    df_new_member
    .groupBy("groupId")
    .agg(
        Fsum(F.col("participation")).alias("new_participation"),
        Fcount(F.col("memberId")).alias("new_member"),
    )
    .withColumn("participation_rate", F.col("new_participation") / F.col("new_member"))
)

# 2 - 특정 일 기준 1개월 이내 사용자 필터링
# 기준 날짜 지정
reference_date = "2024-05-01"
df_specific_period = df_trans.withColumn(
    "days_since_signup",
    datediff(to_date(F.lit(reference_date)), to_date(F.col("signupTime")))
).filter((F.col("days_since_signup") >= 0 ) & (F.col("days_since_signup") <= 30 ))

df_participation_rate_specific = (
    df_specific_period
    .groupBy("groupId")
    .agg(
        Fsum("participation").alias("new_participation"),
        Fcount(F.col("memberId")).alias("new_members"),
    )
    .withColumn("participation_rate", F.col("new_participation") / F.col("new_members"))
)


# 3 - 기존 사용자(가입 6개월 이상) 활동(최근 2개월) 비율.
df_old_member = df_trans.withColumn(
    "old_signup",
    datediff(to_date(F.col("time")), to_date(F.col("signupTime")))
).filter(F.col("old_signup") >= 180 )

# 최근 2개월 활동 사용자 필터링
recent_month_limit = 2
df_recent_activity = df_old_member.withColumn(
    "activity_month",
        datediff(to_date(F.col("time")), to_date(F.col("participationDate"))) / 30
).filter(F.col("activity_month") <= recent_month_limit)
# df_recent_activity.show()

# 모임 참여율 (groupId별)
df_recent_rate = (
    df_recent_activity
    .groupBy("groupId")
    .agg(
        Fsum(F.col("participation")).alias("total_recent_participation"),
        Fcount(F.col("memberId")).alias("active_old_members"),
    )
    .withColumn(
        "recent_rate",
        F.col("total_recent_participation") / F.col("active_old_members"))
)
# df_recent_rate.show()


# 4 - 특정 기간(3개월) 동안 모임 참여를 계속한 사용자 비율.
# 참여 월 추출
df_trans = df_trans.withColumn(
    "participation_month",
    F.date_format(F.to_date("participationDate"), "yyyy-MM")
)

# 사용자별(memberId) 그룹참여(groupId) 월 리스트 생성
df_member_mon = df_trans.groupBy("memberId", "groupId").agg(
    F.collect_list("participation_month").alias("participation_months")
)

# 연속 참여 여부
def consecutive_months(months, period) :
    from datetime import datetime, timedelta
    if len(months) < period:
        return False
    months = sorted(set(datetime.strptime(m, "%Y-%m") for m in months))
    for i in range(len(months) - period + 1):
        if all((months[i + j] - months[i]).days == 30 * j for j in range(period)):
            return True
    return False

# UDF 등록
consecutive_months_udf = F.udf(lambda x: consecutive_months(x, 3), BooleanType())

# 연속 참여 여부 칼럼 추가
df_member_consecutive = df_member_mon.withColumn(
    "consecutive_participation",
    consecutive_months_udf("participation_months")
)

# 그룹별 연속 참여 사용자 수 및 전체 사용자 수 계산
df_group_stats = df_member_consecutive.groupBy("groupId").agg(
    F.count("memberId").alias("total_members"),
    F.sum(F.when(F.col("consecutive_participation"), 1).otherwise(0)).alias("consecutive_members")
)

# 그룹별 연속 참여 사용자 비율 계산
df_group_stats = df_group_stats.withColumn(
    "consecutive_participation_rate",
    F.col("consecutive_members") / F.col("total_members")
)

df_group_stats.show()