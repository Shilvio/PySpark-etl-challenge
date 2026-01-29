from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def get_schema():
    # schemas for spark
    return StructType([
        StructField("order_id", IntegerType()),
        StructField("user_id", StringType()),
        StructField("product_id", StringType()),
        StructField("amount", FloatType()),
        StructField("status", StringType()),
        StructField("created_at", StringType())
    ])