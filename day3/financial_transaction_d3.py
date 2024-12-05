import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. 데이터 준비
# 1-1 spark를 사용하여 금융 데이터를 로드
spark = SparkSession.builder.appName("financial_transaction_d3").getOrCreate()
df = spark.read.csv("././Financial_Transactions_with_Reduced_Missing_Values.csv", header =True)

# 1-2 데이터 스키마를 출력하고, 결측값 처리(삭제, 평균 대체)
# df.printSchema()
# df.show()
df_na = df.dropna()
# df_na.show()

# 1-3 데이터의 transaction_date를 날짜 형식으로 변환
df_transformed = (
    df_na
    .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
    .withColumn("amount", F.col("amount").cast("double"))
)

# df_transformed.printSchema()

# 2. 거래 분석
# 2-1 사용자의 월별 총 거래 금액 계산
df_with_month = df_transformed.withColumn("year_month", F.date_format(F.col("transaction_date"), "yyyy-MM")) # 연도, 월 칼럼 추가
df_mon_sum = (
    df_with_month
    .groupBy("transaction_id", "year_month").agg(F.sum("amount").alias("total_amount"))
    .orderBy("transaction_id", "year_month")
)

# 2-2 거래 유형(transaction_type)별 총 거래 금액 집계
df_type_sum = (
    df_with_month.groupBy("transaction_type").agg(F.sum("amount").alias("total_amount"))
)

# 3. 이상 거래 탐지
# 3-1 사용자별 평균 거래 금액과 표준 편차를 계산
# df_id_avg = df_with_month.groupBy("transaction_id").agg(F.avg("amount").alias("avg_amount"))
# df_id_stddev = df_with_month.groupBy("transaction_id").agg(F.stddev("amount").alias("std_amount"))
df_id_analyze = (
    df_with_month.groupBy("transaction_id")
    .agg(F.avg("amount").alias("avg_amount"),
         F.stddev("amount").alias("std_amount"))
)

# 3-2 평균 거래 금액의 3배를 초과하는 거래를 이상 거래로 간주하고 해당 거래를 출력
df_over = df_id_analyze.filter
