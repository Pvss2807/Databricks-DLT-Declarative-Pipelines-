import dlt

#Product Expectations
product_rules = {
   "rule_1" :  "product_id IS NOT NULL",
   "rule_2" : "price > 0"
   }

# Creating products staging table
@dlt.table(
    name = 'products_staging_table'
)

@dlt.expect_all_or_drop(product_rules)

def products_staging_table():
    df = spark.readStream.table("dltpvss.source.products")
    return df
