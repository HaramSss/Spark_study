import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import count
from twisted.scripts.htmlizer import header

# Session 생성
spark = SparkSession.builder.appName("financial_transactions").getOrCreate()

# csv 파일을 읽어 데이터프레임으로 변환
df = spark.read.csv("./Financial_Transactions_with_Reduced_Missing_Values.csv", header=True)
# df.show()

# 결측치 제거
df_na = df.dropna()
# df_na.show()

# amount를 interger타입으로 변경
df = df_na.withColumn("amount", df_na.amount.cast("integer"))
# df.printSchema()

# 1. 거래 분석
# 거래 유형별(transaction_type) 거래 수 집계
df1 = df.groupBy("transaction_type").agg(f.sum("amount").alias("amount"))
# df1.show()

# amount의 최대값, 최소값, 평균 계산
df.agg(f.min("amount").alias("min"), f.max("amount").alias("max"), f.avg("amount").alias("avg"))

# 2. 사용자별 거래 금액
# sender_account 기준으로 총 거래 금액(amount)을 계산
df_sum = df.groupBy("sender_account").agg(f.sum("amount").alias("total_amount"))

# 가장 많은 거래 금액을 보낸 상위 5개 계좌 출력
df_limit = df_sum.orderBy(f.desc("total_amount")).limit(5)

# 3. 결과 출력
# 결과를 화면에 출력하거나 간단히 csv 저장
# df_limit.write.csv("./Financial_answer", header=True)

# 심화과제
# 1. 월별 거래 분석
# transaction_date를 기준으로 월별(YYYY-MM)총 거래 금액 계산

# transaction_date에서 월별 데이터 추출
df_with_month = df.withColumn("transaction_month", f.date_format("transaction_date", "yyyy-MM"))

# 월별 총 거래 금액 계산
df_mon_sum = (
    df_with_month.groupBy("transaction_month").agg(f.sum("amount").alias("total_amount")).orderBy("transaction_month"))

# 월별 가장 거래 금액이 높은 월 출력
df_mon_sum.orderBy(f.desc("total_amount")).limit(1)

#  2. 거래 금액 분포 분석
# amount를 기준으로 1,000 단위로 구간을 나눠 각 구간별 거래 수를 계산
# 1,000단위로 구간을 나눔
df_section = df.withColumn(
    "amount_bin",
    f.when(f.col("amount") < 1000, "< 1000")
    .when((f.col("amount") >= 1000) & (f.col("amount") < 2000), "1000 - 1999")
    .when((f.col("amount") >= 2000) & (f.col("amount") < 3000), "2000 - 2999")
    .when((f.col("amount") >= 3000) & (f.col("amount") < 4000), "3000 - 3999")
    .when((f.col("amount") >= 4000) & (f.col("amount") < 5000), "4000 - 4999")
    .when((f.col("amount") >= 5000),">= 5000"))

# 각 구간별 거래 수 계산
df_section_total = df_section.groupBy("amount_bin").agg(f.sum("amount").alias("total_amount"))

# 3. 결과 출력
# df_section_total.write.csv("./Financial_answer2", header=True)
df_section_total.write.parquet("./Financial_answer2.1")

df2 = spark.read.parquet("./Financial_answer2.1")
df2.show()