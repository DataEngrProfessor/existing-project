from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql import Window

# Initialize Spark
conf = SparkConf().setAppName("BFSExample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Sample data representing a social network graph
data = [(1, 'Alice', [2, 3]), (2, 'Bob', [1, 4]), (3, 'Charlie', [1, 5]),
        (4, 'David', [2, 6]), (5, 'Eve', [3]), (6, 'Frank', [4, 7]),
        (7, 'Grace', [6])]

# Create a DataFrame from the data
df = spark.createDataFrame(data, ['id', 'name', 'friends'])

# Specify the source and target users for the shortest path
source_user_id = 1
target_user_id = 6

# Initialize two DataFrames for BFS
frontier = df.filter(df['id'] == source_user_id)
visited = spark.createDataFrame([], df.schema)

# Perform BFS to find the shortest path
while frontier.count() > 0:
    visited = visited.union(frontier)
    frontier = (frontier
                .withColumnRenamed('id', 'prev_id')
                .select(col('friends').alias('id'))
                .withColumn('level', visited.select('id').count())
                .withColumn('friends', col('id'))
                .withColumn('id', col('id'))
                .join(df, on='id')
                .filter(col('id') != source_user_id)
                .filter(col('id') != target_user_id))

# Find the shortest path to the target user
shortest_path = visited.filter(visited['id'] == target_user_id)

# Display the shortest path
shortest_path.show()

# Stop Spark
spark.stop()
