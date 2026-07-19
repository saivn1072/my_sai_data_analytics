import requests
import json
from pyspark.sql.functions import current_timestamp, lit

# 1. Setup API details and parameters
# Apify requires a ~ between the username and actor name in the API URL
actor_id = "" 
# apify_token = dbutils.secrets.get(scope="my_secrets", key="apify_token")
apify_token = ""

city_to_scrape = "West Hyderabad"

# The 'run-sync-get-dataset-items' endpoint forces the API call to wait until 
# the scraping finishes, returning the actual data instead of just a run ID.
# url = f"https://api.apify.com/v2/actors/{actor_id}/runs?token={apify_token}"
url = f"https://api.apify.com/v2/actors/{actor_id}/run-sync-get-dataset-items?token={apify_token}"

# 2. Define the input payload (as required by the specific Actor)

payload = {
    "deal_type": "residential_sale",
    "limit": 5,
    "location": [
        "Western Hyderabad"
    ],
    "verified_property": False,
    "with_photo": False,
    "with_video": False
}

print(f"Triggering Apify scraper for {city_to_scrape}. This may take a few minutes...")

# 3. Call the API
response = requests.post(url, json=payload)
response.raise_for_status() # Fails the notebook if the API returns an error

# 4. Parse the JSON response
properties_data = response.json()
print(properties_data)

if not properties_data:
    print(f"Actor ran successfully but returned 0 properties for {city_to_scrape}.")
else:
    print(f"Extracted {len(properties_data)} properties. Loading to Spark...")
    
    # 5. Convert the list of Python dictionaries directly into a Spark DataFrame
    # Spark will automatically infer the schema based on the JSON keys the Actor returns
    import json
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType
    

    # 1. Convert your HTTP response directly into an RDD of JSON strings
    # (If response.json() is already a list of objects, loop through them. If it's a single object, wrap it in a list)
    # api_data = response.json()
    data_list = properties_data if isinstance(properties_data, list) else [properties_data]
    json_strings = [json.dumps(record) for record in data_list]
    

    # 2. Let Spark infer the schema dynamically from the actual data payload
    # This prevents Python from tripping over complex fields like 'attributes'.
    raw_df = spark.createDataFrame([(s,) for s in json_strings], ["raw_json"])

    # 3. Let Spark SQL dynamically infer the full DDL schema string from the data
    sample_json = json_strings[0]
    ddl_schema = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).collect()[0][0]

    # 4. Parse the raw JSON string column into structured Spark columns
    base_df = raw_df.select(F.from_json(F.col("raw_json"), ddl_schema).alias("data")).select("data.*")
 

    # 3. Use the recursive flattener function to expand it completely
    def flatten_nested_structs(df):
        while True:
            struct_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]
            if not struct_cols:
                break
            
            select_expr = []
            for field in df.schema.fields:
                if isinstance(field.dataType, StructType):
                    for sub_field in field.dataType.fields:
                        select_expr.append(F.col(f"`{field.name}`.`{sub_field.name}`").alias(f"{field.name}_{sub_field.name}"))
                else:
                    select_expr.append(F.col(f"`{field.name}`"))
            df = df.select(*select_expr)
        return df

    final_flat_df = flatten_nested_structs(base_df)

    catalog_name = "my_whatsup"
    schema_name = "poc"
    table_name = "silver_property_listings"
    full_table_path = f"{catalog_name}.{schema_name}.{table_name}"

    # 4. Save directly to your Delta Table
    final_flat_df.write.format("delta").mode("append").saveAsTable(full_table_path)
    
    
    # Optional: Add a metadata column to track when the data was ingested
    # df_properties = df_properties.withColumn("ingestion_timestamp", current_timestamp()) \
    #                              .withColumn("searched_city", lit(city_to_scrape))
    # print(f"Success! Data appended to {full_table_path}")
    # display(df_properties)
