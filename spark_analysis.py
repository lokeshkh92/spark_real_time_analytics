from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def checkOrderStatus(value):
    if(value>=200):
        return "Hooray!!! we have acheived minutes target"
    elif(value==0 or value==""):
        return "What are you doing! Salesboy, No Sale at all"
    else:
        return "Let's work harder to achieve target next minute"

spark  = SparkSession.builder.master("local").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

rdd_read = sc.textFile("<input_file_path>")
rdd = rdd_read.map(lambda x: x.split(","))
_textFile_schema_text = "Date,Product,Status"

rdd_schema = StructType([StructField(field_name, StringType(), True) for field_name in _textFile_schema_text.split(",")])
df = spark.createDataFrame(rdd,rdd_schema)

OrderCountDF = df.groupBy('Status').count()
deliveredValue = OrderCountDF.filter(OrderCountDF['Status']=="delivered").select('count')

orderStatus = checkOrderStatus(deliveredValue)
print("{}".format(orderStatus))
