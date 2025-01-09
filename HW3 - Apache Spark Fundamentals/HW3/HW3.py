# %% [markdown]
# # Homework Week 3 - Spark Fundamentals
# ### by Marina Tetzlaff | December 31, 2024
# 
# Start by loading the datasets from the iceberg data folder. The queries will use the following tables:
# - match_details
# - matches
# - medals_matches_players
# - medals
# - maps
# 

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

spark

# %% [markdown]
# ## 0. Import the data sources

# %%
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
match_details.show(5)

# %%
null_count = match_details.filter(match_details.team_id.isNull()).count()
null_count

# %%
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
matches.show(5)

# %%
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals_matches_players.show(5)

# %%
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
medals.show(5)

# %% [markdown]
# Goal:
# - Build a disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# - Explicitly broadcast JOINs medals and maps
# - Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
# - Aggregate the joined data frame to determine the following: 
#     - Player with highest kills per game average
#     - Playlist with most plays
#     - Map most played
#     - Map with most players earning Killing Spree medals
# - With the aggregated dataset, try different .sortWithinPartitions to see which has the smallest data size (check playlists and maps)

# %% [markdown]
# Next, I need to create a database and insert the data from these tables so we can use SQL to query the data

# %%
%%sql
CREATE DATABASE IF NOT EXISTS bootcamp

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.match_details

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.matches

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.medals_matches_players

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.medals

# %%
%%sql
DROP TABLE IF EXISTS bootcamp.maps

# %%
match_details.dtypes

# %%
from pyspark.sql.functions import countDistinct
unique_players_count = match_details.select(countDistinct('player_gamertag')).collect()[0][0]

unique_players_count

# %%
unique_teams = match_details.select(countDistinct('team_id')).collect()[0][0]
unique_teams

# %%
unique_matches = match_details.select(countDistinct('match_id')).collect()[0][0]
unique_matches

# %%
match_details.count()

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.match_details (
    match_id STRING,
    player_gamertag STRING,
    previous_spartan_rank INT,
    spartan_rank INT,
    previous_total_xp BIGINT,
    total_xp BIGINT,
    previous_csr_tier INT,
    previous_csr_designation INT,
    previous_csr INT,
    previous_csr_percent_to_next_tier DOUBLE,
    previous_csr_rank INT,
    current_csr_tier INT,
    current_csr_designation INT,
    current_csr INT,
    current_csr_percent_to_next_tier DOUBLE,
    current_csr_rank INT,
    player_rank_on_team INT,
    player_finished BOOLEAN,
    player_average_life STRING,
    player_total_kills INT,
    player_total_headshots INT,
    player_total_weapon_damage DOUBLE,
    player_total_shots_landed INT,
    player_total_melee_kills INT,
    player_total_melee_damage DOUBLE,
    player_total_assassinations INT,
    player_total_ground_pound_kills INT,
    player_total_shoulder_bash_kills INT,
    player_total_grenade_damage DOUBLE,
    player_total_power_weapon_damage DOUBLE,
    player_total_power_weapon_grabs INT,
    player_total_deaths INT,
    player_total_assists INT,
    player_total_grenade_kills INT,
    did_win BOOLEAN,
    team_id INT
  ) USING iceberg PARTITIONED BY (team_id)

# %%
matches.dtypes

# %%
%%sql 
CREATE TABLE
  IF NOT EXISTS bootcamp.matches (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    game_variant_id STRING,
    is_match_over BOOLEAN,
    completion_date TIMESTAMP,
    match_duration STRING,
    game_mode STRING,
    map_variant_id STRING,
    partitioned_date DATE
  ) USING iceberg PARTITIONED BY (mapid)

# %%
medals_matches_players.dtypes

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.medals_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    COUNT INT
  ) USING iceberg PARTITIONED BY (medal_id)

# %%
medals_matches_players.count()

# %%
medals.dtypes

# %%
medals.count()

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.medals (
    medal_id BIGINT,
    sprite_uri STRING,
    sprite_left INT,
    sprite_top INT,
    sprite_sheet_width INT,
    sprite_sheet_height INT,
    sprite_width INT,
    sprite_height INT,
    classification STRING,
    description STRING,
    name STRING,
    difficulty INT
  ) USING iceberg

# %%
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
maps.show(5)

# %%
maps.count()

# %%
%%sql
CREATE TABLE
  IF NOT EXISTS bootcamp.maps (mapid STRING, name STRING, description STRING) USING iceberg

# %% [markdown]
# ## 1. Disable automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# # joins default to broadcast join if we let it.

# %%
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# %%
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# %% [markdown]
# ### Write the data to the database

# %%
match_details.write.mode("overwrite").saveAsTable("bootcamp.match_details")
matches.write.mode("overwrite").saveAsTable("bootcamp.matches")
medals_matches_players.write.mode("overwrite").saveAsTable("bootcamp.medals_matches_players")
medals.write.mode("overwrite").saveAsTable("bootcamp.medals")
maps.write.mode("overwrite").saveAsTable("bootcamp.maps")

# %%
%%sql
SELECT
  *
FROM
  bootcamp.match_details
LIMIT
  5

# %%
%%sql
SELECT
  *
FROM
  bootcamp.matches
LIMIT
  5

# %%
%%sql
SELECT
  *
FROM
  bootcamp.medals_matches_players
LIMIT
  5

# %%
%%sql
SELECT
  *
FROM
  bootcamp.medals
LIMIT
  5

# %%
%%sql
SELECT
  *
FROM
  bootcamp.maps
LIMIT
  5

# %% [markdown]
# ## 2. Explicitly broadcast JOINs medals and maps

# %% [markdown]
# To explicitly broadcast the table joins for the medals and maps, we need to have an understanding of the tables: 
# 
# match_details ─┬─── matches (via match_id) ─── maps (via map_id)
#              
#              └─── medals_matches_players (via match_id)
#                          │
#                          
#                          └─── medals (via medal_id)
# Fact Tables: 
# - match_details
# - medals_matches_players
# - matches
# 
# Dimension Tables: 
# - medals
# - maps
# 

# %% [markdown]
# ### Check for duplicates
# Prior to joining, we need to consider the possibility of Duplicates, and dedupe the medals and maps tables (as well as the others)

# %%
duplicate_maps = maps.groupBy(maps.columns).count().filter("count >1")
duplicate_maps.show()

# %%
duplicate_medals = medals.groupBy(medals.columns).count().filter("count >1")
duplicate_medals.show()

# %%
duplicate_match_details = match_details.groupBy(match_details.columns).count().filter("count > 1")
duplicate_match_details.show()

# %%
duplicate_matches = matches.groupBy(matches.columns).count().filter("count > 1")
duplicate_matches.show()

# %%
from pyspark.sql.functions import sum, avg, max, min, col, lit, count

duplicate_medals_matches_players = medals_matches_players.groupBy("match_id", "player_gamertag", "medal_id") \
    .agg(count("*").alias("duplicate_count")) \
    .filter("duplicate_count > 1")

duplicate_medals_matches_players.show()

# %% [markdown]
# There do not seem to be any duplicate rows in any of the tables. 

# %% [markdown]
# ## 3. Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets

# %%
from pyspark.sql.functions import broadcast, split, lit
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IcebergTableManagement") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .getOrCreate()

# %% [markdown]
# The next step is to join all of the tables. 
# - Medals will be broadcasted explicitly
# - the fact tables will be bucketed into 16 buckets
# - the fact tables will be joined on match_id

# %%
matches_bucketed = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/matches.csv")

match_details_bucketed = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/match_details.csv")

medals_matches_players_bucketed = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/medals_matches_players.csv")

# %% [markdown]
# ### 3.1.a. Create matches_bucketed table 🎮

# %%
spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

# Create the table using Iceberg
bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    mapid STRING
)
USING iceberg
PARTITIONED BY (completion_date, bucket(16, match_id))
"""
spark.sql(bucketed_ddl)

# %% [markdown]
# ### 3.1.b. Populate matches_bucketed table

# %%
# Get distinct completion dates
distinct_dates = matches_bucketed.select("completion_date").distinct().collect()

# Process data in chunks based on completion_date
for row in distinct_dates:
    date = row["completion_date"]
    filtered_matches = matches_bucketed.filter(col("completion_date") == date)
    
    # Repartition and persist the filtered data
    optimized_matches = filtered_matches \
        .select("match_id", "is_team_game", "playlist_id", "completion_date","mapid") \
        .repartition(16, "match_id") \
        .persist(StorageLevel.MEMORY_AND_DISK)
    
    optimized_matches.write \
        .mode("append") \
        .bucketBy(16, "match_id") \
        .partitionBy("completion_date") \
        .saveAsTable("bootcamp.matches_bucketed")

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.matches_bucketed")
result.show(5)

# %% [markdown]
# ### 3.2.a. Create the bucketed match_details_bucketed table📝

# %%
spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

bucketed_details_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
        match_id STRING,
        player_gamertag STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER
 )
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_details_ddl)

# %% [markdown]
# ### 3.2.b. Populate the bucketed match_details_bucketed table

# %%
match_details_bucketed.select(
    "match_id", "player_gamertag", "player_total_kills", "player_total_deaths"
).write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.match_details_bucketed")

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.match_details_bucketed")
result.show(5)

# %% [markdown]
# ### 3.3.a. Create the bucketed medals_matches_players table 🎖️

# %%
spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")

bucketed_medals_matches_players_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id BIGINT,
    COUNT INTEGER
 )
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(bucketed_medals_matches_players_ddl)

# %% [markdown]
# ### 3.3.b. Populate the bucketed medals_matches_players table

# %%
medals_matches_players_bucketed.select(
    "match_id", "player_gamertag", "medal_id", "COUNT"
).write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.medals_matches_players_bucketed")

# Verify the data in the table
result = spark.sql("SELECT * FROM bootcamp.medals_matches_players_bucketed")
result.show(5)

# %% [markdown]
# ## 4. Aggregate the joined data frame to figure out questions 
# - Which player averages the most kills per game?
# - Which playlist gets played the most?
# - Which map gets played the most?
# - Which map do players get the most Killing Spree medals on?

# %%
spark.sql("""DROP TABLE IF EXISTS bootcamp.joined_matches""")

joined_matches_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.joined_matches (
        match_id STRING,
        player_gamertag STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        mapid STRING,
        medal_id BIGINT,
        COUNT INTEGER   
 )
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(joined_matches_ddl)

# %%
spark.sql("""
    SELECT * 
    FROM bootcamp.match_details_bucketed mdb 
    JOIN bootcamp.matches_bucketed md 
        ON mdb.match_id = md.match_id
        AND md.completion_date = DATE('2016-01-01')
    JOIN bootcamp.medals_matches_players_bucketed mmp 
        ON mdb.match_id = mmp.match_id
""").explain()

# %%
joined_matches_query = """
    SELECT 
        mdb.match_id,
        mdb.player_gamertag,
        mdb.player_total_kills,
        mdb.player_total_deaths,
        md.is_team_game,
        md.playlist_id,
        md.completion_date,
        md.mapid,
        mmp.medal_id,
        mmp.COUNT    
    FROM bootcamp.match_details_bucketed mdb 
    JOIN bootcamp.matches_bucketed md 
        ON mdb.match_id = md.match_id
    JOIN bootcamp.medals_matches_players_bucketed mmp 
        ON mdb.match_id = mmp.match_id
"""

joined_matches = spark.sql(joined_matches_query)

# %%
joined_matches.select(
    "match_id", "player_gamertag", "player_total_kills", "player_total_deaths", \
"is_team_game", "playlist_id", "completion_date", "mapid", "medal_id", \
    "COUNT").write \
    .format("iceberg") \
    .mode("append") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.joined_matches")

# Verify the data in the table
result = spark.sql("""SELECT * FROM bootcamp.joined_matches""")
result.show(5)

# %% [markdown]
# ## Broadcast JOINs medals and maps

# %%
from pyspark.sql.functions import broadcast

# %%
# Broadcast matches with maps
matches_with_maps = joined_matches \
    .join(broadcast(maps), on="mapid", how="inner")
matches_with_maps.show(5)

# %%
# Broadcast matches with medals
matches_with_medals = joined_matches \
    .join(broadcast(medals), on="medal_id", how="inner")
matches_with_medals.show(5)

# %% [markdown]
# ### a. Player with max kills per game?

# %%
max_kills_query = """
with raw as (
    select player_gamertag
    , player_total_kills
    from  bootcamp.joined_matches jm
)

, total_kills_per_player as (
select player_gamertag
, sum(player_total_kills) as count_k
from raw
group by 1
)

select * from total_kills_per_player 
order by count_k desc 
limit 1
"""

max_kills = spark.sql(max_kills_query)
max_kills.show()

# %% [markdown]
# ### b. Playlist played the most?

# %%
max_plays_query = """
with raw as (
    select playlist_id
    , match_id
    from  bootcamp.joined_matches jm
)

, total_matches_per_playlist as (
select playlist_id
, count(match_id) as count_m
from raw
group by 1
)

select * from total_matches_per_playlist 
order by count_m desc 
limit 1
"""

spark.sql(max_plays_query).show()

# %% [markdown]
# ### c. Map played the most?

# %%
raw_df = matches_with_maps.select("mapid", "name", "match_id").distinct()

# Step 2: Group by mapid and name, count distinct match IDs
total_matches_per_map_df = raw_df.groupBy("mapid", "name") \
    .agg({"match_id": "count"}) \
    .withColumnRenamed("count(match_id)", "count_m")

# Step 3: Get the map with the most unique matches
top_map_df = total_matches_per_map_df.orderBy("count_m", ascending=False).limit(1)

# Step 4: Display the result
top_map_df.show()

# %% [markdown]
# ### d. Which map do players get the most Killing Spree medals on?

# %%
from pyspark.sql.functions import col

killing_spree_df = matches_with_medals.withColumnRenamed("name", "medal_name").filter(col("medal_name") == "Killing Spree")

# Step 2: Select distinct combinations of mapid and match_id
unique_killing_spree_matches_df = killing_spree_df.select("mapid", "medal_name", "match_id").distinct()

# Step 3: Group by mapid and name, and count distinct match_id's
total_unique_matches_per_map_df = unique_killing_spree_matches_df.groupBy("mapid", "medal_name") \
    .agg({"match_id": "count"}) \
    .withColumnRenamed("count(match_id)", "unique_match_count")

total_unique_matches_with_name_df = total_unique_matches_per_map_df \
    .join(broadcast(maps), on="mapid", how="inner") \
    .select("name", "mapid", "medal_name", "unique_match_count")

# Step 5: Get the map with the most unique match IDs for "Killing Spree" medals
top_map_df = total_unique_matches_with_name_df.orderBy("unique_match_count", ascending=False).limit(1)

# Step 6: Display the result
top_map_df.show()

# %% [markdown]
# ## 5. Sort the aggregated dataset

# %%
%%sql

CREATE TABLE IF NOT EXISTS bootcamp.matches_unsorted (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    mapid STRING,
    medal_id BIGINT,
    COUNT INTEGER   
)
USING iceberg
PARTITIONED BY (year(completion_date));

# %%
%%sql

CREATE TABLE IF NOT EXISTS bootcamp.matches_sorted (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    mapid STRING,
    medal_id BIGINT,
    COUNT INTEGER   
)
USING iceberg
PARTITIONED BY (year(completion_date));

# %%
%%sql

CREATE TABLE IF NOT EXISTS bootcamp.matches_sorted_v2 (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP,
    mapid STRING,
    medal_id BIGINT,
    COUNT INTEGER   
)
USING iceberg
PARTITIONED BY (year(completion_date));

# %%
#Unsorted data, partition on completion date
partition_df = joined_matches.repartition(10, col("completion_date"))
partition_df.explain()
partition_df.write.mode("overwrite").saveAsTable("bootcamp.matches_unsorted")

# %%
#Sorted on completion date and player gametag
sorted_df = joined_matches.repartition(10, col("completion_date"))\
    .sortWithinPartitions(col("completion_date"), col("player_gamertag"))
sorted_df.explain()
sorted_df.write.mode("overwrite").saveAsTable("bootcamp.matches_sorted")

# %%
#Sorted on lowest cardinality to highest cardinality
sorted_df_2 = joined_matches.repartition(10, col("completion_date"))\
    .sortWithinPartitions(col("completion_date"), col("player_gamertag"), col("match_id"))
sorted_df_2.explain()
sorted_df_2.write.mode("overwrite").saveAsTable("bootcamp.matches_sorted_v2")

# %%
%%sql

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted v2' 
FROM demo.bootcamp.matches_sorted_v2.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.matches_sorted.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM demo.bootcamp.matches_unsorted.files

# %%



