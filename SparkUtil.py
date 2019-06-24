import sys

import requests
from pyspark.sql import Row, SQLContext


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext=spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df_hashtags):
    # Extract all the hashtags from dataframe and convert them into array
    top_tags = [str(row.hashtag) for row in df_hashtags.selct('hashtag').collect()]

    # Extract all the hashtags count  from dataframe and convert them into array
    top_count = [str(row.hashtag_count) for row in df_hashtags.selct('hashtag_count').collect()]

    url = 'http://localhost:5001/updateData'
    request_Data = {'hastags': str(top_tags), 'hastags_count': str(top_count)}
    respone = requests.post(url, data=request_Data)


def process_rdd(time, rdd):
    print("---------------{}---------------".format(str(time)))
    try:
        # get Spark SQL Singleton context form the current context
        sql_context = get_sql_context_instance(rdd.context)

        # convert rdd to ROW RDD
        row_rdd = rdd.map(lambda word: Row(hashtag=word[0], hashtag_count=word[1]))

        # create the Dataframe from ROW RDD
        df_hashtags = sql_context.createDatafram(row_rdd)

        # Register the dataframe as table
        df_hashtags.registerTempTable("hashtags")

        # Load the top 10 hastags from the table using SQL and Display them
        df_hashtags_counts = sql_context.sql(
            "Select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 15")

        df_hashtags_counts.show()

        # Call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(df_hashtags_counts)

    except:
        ex = sys.exc_info()[0]
        print("Error : {}".format(ex))


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)
