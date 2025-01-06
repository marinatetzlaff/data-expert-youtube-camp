from pyspark.sql import SparkSession

# SQL query with hosts_cumulated as a source
query = """
WITH yesterday AS (
    SELECT host, host_activity_datelist, date
    FROM hosts_cumulated
    WHERE date = DATE('2022-12-31')
),

today AS (
    SELECT DISTINCT host,
           DATE(CAST(event_time AS timestamp)) AS date_active
    FROM events
    WHERE DATE(CAST(event_time AS timestamp)) = DATE('2023-01-01')
      AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS timestamp))
)

SELECT COALESCE(t.host, y.host) AS host,
       CASE 
           WHEN y.host_activity_datelist IS NULL THEN sort_array(array(t.date_active))
           WHEN t.date_active IS NULL THEN sort_array(y.host_activity_datelist)
           ELSE array(t.date_active) || y.host_activity_datelist
       END AS host_activity_datelist,
       COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host
"""

def do_hosts_cumulated_transformation(spark, events_df, hosts_cumulated_df):
    # Register both tables as temporary views
    events_df.createOrReplaceTempView("events")
    hosts_cumulated_df.createOrReplaceTempView("hosts_cumulated")
    
    # Execute the SQL transformation
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("hosts_cumulated") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read data from both sources
    events_df = spark.table("events")
    hosts_cumulated_df = spark.table("hosts_cumulated")

    # Perform the transformation
    output_df = do_hosts_cumulated_transformation(spark, events_df, hosts_cumulated_df)
    
    # Write results back to hosts_cumulated
    output_df.write.mode("overwrite").insertInto("hosts_cumulated")

if __name__ == "__main__":
    main()