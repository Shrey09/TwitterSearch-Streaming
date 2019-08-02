import tweepy as tp
import json
import csv
import re

# tokens required to connect twitter app
access_token="3105648036-QD8OaF37KAqCoGgDrIgOoXHNai7fh8tSe7wot6a"
access_token_secret="X9ntr9biRoxS3mXCXLBbgMYptwo2Y7HMHg98PCNKSDq7m"
consumer_key="lwuvuhctoEIqrrt7ZlNP5IbAM"
consumer_secret="9Kpthxs6HoWp32Me8j3jL3HA1LEMcHh4FPxcY17tCUqAw3IXBp"

#files to store the tweets
file=open("Tweets_Stream_2.txt",'a+',encoding='utf-8')
json_file=open("Tweets_Stream_2.json","a+")

#class for cleaning data
def tweet_cleaning(data):
    data = re.sub(r'@[A-Z0-9a-z]+', '', data) # remove special characters
    data = re.sub(r'#[A-Z0-9a-z]+', '', data) # remove special characters
    data = re.sub('https?://[A-Z-0-9a-z./]+', '', data) # remove urls
    data = re.sub('_images/[A-Z-0-9a-z.jpg]+', '', data)
    data = re.sub('/[A-Z-0-9a-z.jpg]+', '', data) #reomve images
    data = data.lower() # convert all letter in lowercase
    # filter emoticons
    emoticons = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    data = emoticons.sub(r'', data)
    data = data.replace("\\u", "")
    return data

# class to print and store tweets
class fetching_tweet(tp.streaming.StreamListener):

    def on_data(self, tweets):
        print("Streaming started")
        print(type(tweets),tweets)
        file.write(tweets)
        tweets=json.loads(tweets)
        #store tweets in json
        json.dump(tweets,json_file,indent=4)

        with open("stream_2.csv", 'a+') as csvfile:
            columns = ["created_at", "id", "name", "tweet", "user_id", "screen_name", "location"]
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            try:
                # fetch the specific attributes from tweet, clean it and store each tweet in csv file
                text = tweet_cleaning(tweets["text"])
                dict = {'created_at': tweets["created_at"], 'id': tweets["id"], "name": tweets["user"]["name"], "tweet": text,
                        'user_id': tweets["user"]["id"],
                        'screen_name': tweets["user"]["screen_name"], 'location': tweets["user"]["location"]}
                print(dict)
                writer.writerow(dict)
            except:
                pass

    def on_error(self, status_code):
        print("error ocurred",status_code)

tweet_listener=fetching_tweet()

# set up the credentials for tweet listener
authenticate=tp.OAuthHandler(consumer_key,consumer_secret)
authenticate.set_access_token(access_token,access_token_secret)

#set the tweet stream
twitter_stream=tp.Stream(authenticate,tweet_listener)

#filter tweets based on a specific keyword
twitter_stream.filter(track=['Halifax'])


