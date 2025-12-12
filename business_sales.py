import dlt
from pyspark.sql.functions import *

#create a materialized view on top of all the dim and fact tables
@dlt.view(
    name = "business_sales_view"
)

def business_sales_view():
    df_fact = spark.read.table("fact_sales")
    df_dimProd = spark.read.table("dim_product")
    df_dimCust = spark.read.table("dim_customer")

    df_join = df_fact.join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "inner").join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "inner")

    df_prun = df_join.select('region','category','total_amount')

    df_agg = df_prun.groupBy('region','category').agg(sum('total_amount').alias('total_sales'))

    return df_agg.orderBy('total_sales', ascending=False)

