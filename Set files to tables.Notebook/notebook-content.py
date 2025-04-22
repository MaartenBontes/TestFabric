# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "79d94125-0f7d-4f6e-a7cf-e9a8e84e2749",
# META       "default_lakehouse_name": "Trial_lake",
# META       "default_lakehouse_workspace_id": "129a36ac-80ef-4ed1-8e35-26c2fc1c762c",
# META       "known_lakehouses": [
# META         {
# META           "id": "79d94125-0f7d-4f6e-a7cf-e9a8e84e2749"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Define lakehouse and table to create
lakehouse = "Trial_lake"
table_to_create = "FactProduct"
# Path to the CSV file: select file meatballs and select Copy ABFS-path
csv_path = "abfss://Trial_Dev@onelake.dfs.fabric.microsoft.com/Trial_lake.Lakehouse/Files/Product shipment.csv"
try:
    # Try to read the CSV file and execute your code if the file exists
    df = spark.read.option("delimiter", ";").csv(csv_path, header=True, inferSchema=True)
    # Replace spaces and parentheses in column names with underscores
    new_columns = [col.replace("(", "_").replace(")", "_").replace(" ", "_") for col in df.columns]
    df = df.toDF(*new_columns)
    #display(df)
    # Write into table of the lakehouse
    df.write.format("delta").mode("overwrite").saveAsTable(f"{lakehouse}.{table_to_create}")
    # Message of succes
    print(f"Table: {table_to_create} is uploaded to lakehouse: {lakehouse} successfully.")
except Exception as e:
    # Display a message if the file does not exist
    print(f"The file {csv_path} does not exist or an error occurred: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define lakehouse and table to create
lakehouse = "Trial_lake"
table_to_create = "FactShipments"
# Path to the CSV file: select file meatballs and select Copy ABFS-path
csv_path = "abfss://Trial_Dev@onelake.dfs.fabric.microsoft.com/Trial_lake.Lakehouse/Files/Shipments.csv"
try:
    # Try to read the CSV file and execute your code if the file exists
    df = spark.read.option("delimiter", ";").csv(csv_path, header=True, inferSchema=True)
    # Replace spaces and parentheses in column names with underscores
    new_columns = [col.replace("(", "_").replace(")", "_").replace(" ", "_") for col in df.columns]
    df = df.toDF(*new_columns)
    #display(df)
    # Write into table of the lakehouse
    df.write.format("delta").mode("overwrite").saveAsTable(f"{lakehouse}.{table_to_create}")
    # Message of succes
    print(f"Table: {table_to_create} is uploaded to lakehouse: {lakehouse} successfully.")
except Exception as e:
    # Display a message if the file does not exist
    print(f"The file {csv_path} does not exist or an error occurred: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
