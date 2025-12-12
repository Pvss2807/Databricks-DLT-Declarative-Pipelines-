import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(
    name = "sales_enriched_table"
)

#Transforming Sales Data
@dlt.view(
    name = "sales_enriched_view"
)

def sales_enriched_view():
    df = spark.readStream.table("sales_staging_table")
    df = df.withColumn("total_amount", col('quantity') * col('amount'))
    return df

dlt.create_auto_cdc_flow(
  target = "sales_enriched_table",
  source = "sales_enriched_view",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = True
)
