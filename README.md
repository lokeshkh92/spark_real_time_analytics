# spark_real_time_analytics
Get real time analytics for e-commerce order status

Get real time stats for delivered order. Assuming the input file schema is "Date, Product,Status".
Sample values in the input file is "19-09-2019 00:00:00,Shampoo,Delivered"

In the spark analysis file, replace the input file path.

Use following command for the spark analysis:

spark2-submit --master yarn --deploy-mode client --executor-memory=4g --num-executors=3 --executor-cores=2 --driver-memory=2g spark_analysis.py
================================================================================================================================================
