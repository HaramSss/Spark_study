import pyspark
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f

# Session 생성
spark = SparkSession.builder.appName("Ecommerce").getOrCreate()

# 1. csv 파일을 읽어서 price * quantity로 주문의 총 매출을 계산
# 파일 읽기
df = spark.read.csv("./ecommerce_data.csv", header=True)
# df.show()
# df.printSchema()

# 총 매출 계산
# dataframe에 total_sales 컬럼 추가
df_sales = df.withColumn("total_sales", f.col("price") * f.col("quantity"))

# 2. 지역(region) 별로 매출 합계 계산
df_with_region = df_sales.groupBy("region").agg(f.sum("total_sales").alias("total_sales"))

# 3. 상품 카테고리(category)별로 매출 합계를 계산
df_with_cate = df_sales.groupBy("category").agg(f.sum("total_sales").alias("total_sales"))

# 4. 매출이 높은 상위 3개 지역을 추출
df_top_sales = df_with_region.orderBy(f.desc("total_sales")).limit(3)

# 5. 결과를 csv로 저장
# df_top_sales.write.csv("./top_sales.csv")

# 심화과제
# 1. 사용자별 분석
# user_id별로 평균 주문 가격과 총 매출을 계산
df_id_sales = df_sales.groupBy("user_id").agg(f.avg("price"), f.sum("total_sales"))

# 각 사용자가 가장 많이 주문한 카테고리 파악
# 2-1. 사용자별 카테고리별 총 주문수량 계산
df_user_quantity = df.groupBy("user_id", "category").agg(f.sum("quantity").alias("total_quantity"))

# 2-2. 사용자별 가장 많이 주문한 카테고리
# 사용자별 windowspec 정의 (user_id를 기준으로 partition, total_quantity 내림차순으로 정렬)
window_spec = Window.partitionBy("user_id").orderBy(f.desc("total_quantity"))

df_top_cate = (
    df_user_quantity
    .withColumn("rank", f.rank().over(window_spec)) # 사용자별 수량 내림차순 정렬 후 순위 매기기
    .filter(f.col("rank") == 1) # 순위가 1인 데이터만 추출
    .select("user_id", "category", "total_quantity") # 필요한 칼럼만 선택
)

# 2. 파티션 적용
# 지역(region)별로 데이터를 파티셔닝하여 병렬 처리
df_partitioned = df_sales.repartition("region")

# 각 파티션별 총 매출 합계를 계산
df_region_part = df_partitioned.groupBy("region").agg(f.sum("total_sales").alias("total_sales")).show()


