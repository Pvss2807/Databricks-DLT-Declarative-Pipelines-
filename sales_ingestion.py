import dlt

#Sales Expectations
sales_rules = {
    "rule_1" : "sales_id IS NOT NULL"
}

dlt.create_streaming_table(
    name = "sales_staging_table",
    expect_all_or_drop = sales_rules
)

#append flow the sales data into the empty streaming table
@dlt.append_flow(target = 'sales_staging_table')
def east_sales():
    df = spark.readStream.table("dltpvss.source.sales_east")
    return df

@dlt.append_flow(target = 'sales_staging_table')
def west_sales():
    df = spark.readStream.table("dltpvss.source.sales_west")
    return df
