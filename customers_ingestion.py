import dlt

#Customer Expectations
customer_rules = {
   "rule_1" :  "customer_id IS NOT NULL",
   "rule_2" : "customer_name IS NOT NULL"
   }

@dlt.table(
    name = 'customers_staging_table'
)

@dlt.expect_all_or_drop(customer_rules)

def customers_staging_table():
    df = spark.readStream.table("dltpvss.source.customers")
    return df

