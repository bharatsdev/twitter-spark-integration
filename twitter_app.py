import json
import socket
import sys

import requests
import requests_oauthlib

# Add the OAuth Twitter
ACCESS_TOKEN = '<>'
ACCESS_SECRET = '<>'
CONSUMER_KEY = '<>'
CONSUMER_SECRET = '<>'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def fetch_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url)
    print(query_url, response)
    return response


def push_tweets_to_spark(response, tcp_connection):
    for line in response.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text : {} ".format(tweet_text))
            print("-----------------------------------------")
            tcp_connection.send(tweet_text + '\n')

        except:
            e = sys.exc_info()[0]
            print("Error :  {}".format(e))


TCP_IP = 'localhost'
TCP_PORT = 9009
conn = None

sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sckt.bind(TCP_IP, TCP_PORT)
sckt.listen(1)

print("Waiting for TCP Connection.........")
conn, addr = sckt.accept()
print("Connected..... Starting getting tweets..")
resp = fetch_tweets()
