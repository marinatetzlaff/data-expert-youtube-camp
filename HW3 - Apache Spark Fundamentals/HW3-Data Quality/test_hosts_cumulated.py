from chispa.dataframe_comparer import assert_df_equality
from ..jobs.hosts_cumulated_job import do_hosts_cumulated_transformation
from collections import namedtuple

# Define schemas for the datasets
HostEvent = namedtuple("HostEvent", "event_time host")
HostActivity = namedtuple("HostActivity", "host host_activity_datelist date")

def test_scd_generation(spark):
    # Source Dataset 1: Events
    source_data = [
        HostEvent("2023-01-01 00:05:29.405000", 'www.eczachly.com'),
        HostEvent("2023-01-13 00:05:34.806000", 'www.eczachly.com'),
        HostEvent("2023-01-01 22:13:31.901000", 'admin.zachwilson.tech'),
        HostEvent("2023-01-01 11:55:28.032000", 'www.zachwilson.tech')
    ]
    source_df = spark.createDataFrame(source_data)
    
    # Source Dataset 2: Hosts Cumulated
    hosts_cumulated_data = [
        HostActivity("www.eczachly.com", ['2022-12-31'], '2022-12-31'),
        HostActivity("admin.zachwilson.tech", ['2022-12-31'], '2022-12-31')
    ]
    hosts_cumulated_df = spark.createDataFrame(hosts_cumulated_data)

    # Perform transformation with both datasets
    actual_df = do_hosts_cumulated_transformation(spark, source_df, hosts_cumulated_df)

    # Expected Output
    expected_data = [
        HostActivity("admin.zachwilson.tech", ['2023-01-01','2022-12-31'], '2023-01-01'),
        HostActivity("www.eczachly.com", ['2023-01-01','2022-12-31'], '2023-01-01'),
        HostActivity("www.zachwilson.tech", ['2023-01-01'], '2023-01-01')
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Validate Results
    assert_df_equality(actual_df, expected_df)