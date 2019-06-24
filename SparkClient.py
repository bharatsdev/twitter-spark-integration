from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from SparkUtil import aggregate_tags_count, process_rdd

# Create spark configuration
sc = SparkSession.builder.appName("TwitterTweetStreamApp").getOrCreate()
# sc.sparkContext.setLogLevel("ERROR")

# Create the streaming context from the above spark session with interval size 2 seconds
sparkStream = StreamingContext(sc, 2)

# Setting a checkpoint to allow RDD Recovery
sparkStream.checkpoint("checkpoint_TwitterStreamApp")

# Read Data from port 9009
dataStream = sparkStream.socketTextStream('localhost', 9009)

# Split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# Filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda word: '#' in word).map(lambda hastagword: (hastagword, 1))

# Append the count of each hashtag to its last count
hashtags_counts = hashtags.updateStateByKey(aggregate_tags_count)

# Do processing for each RDD geneated in each interval 
hashtags_counts.foreachRDD(process_rdd)

# Start the stream computation
sparkStream.start()

# Wait till streaming finished.
sparkStream.awaitTermination()
