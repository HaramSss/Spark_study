import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Session 생성
spark = SparkSession.builder.appName("Ecommerce").getOrCreate()

# csv 파일을 읽어 데이터프레임으로 변환
df = spark.read.csv("./ecommerce_data.csv", header=True)

df.printSchema()

df.show()