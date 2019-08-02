import tweepy as tp
import csv
import json
import re

# tokens required to connect twitter app
access_token="3105648036-QD8OaF37KAqCoGgDrIgOoXHNai7fh8tSe7wot6a"
access_token_secret="X9ntr9biRoxS3mXCXLBbgMYptwo2Y7HMHg98PCNKSDq7m"
consumer_key="lwuvuhctoEIqrrt7ZlNP5IbAM"
consumer_secret="9Kpthxs6HoWp32Me8j3jL3HA1LEMcHh4FPxcY17tCUqAw3IXBp"

#file to store tweets
file=open("search.txt","a+",encoding='utf-8')

# establish the credentials
authenticate=tp.OAuthHandler(consumer_key,consumer_secret)
authenticate.set_access_token(access_token,access_token_secret)

# twitter_stream.filter(track=['Halifax'])
api=tp.API(authenticate,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

# class to clean the tweets
def tweet_cleaning(data):

    data = re.sub(r'@[A-Z0-9a-z]+', '', data) # reomve special character
    data = re.sub(r'#[A-Z0-9a-z]+', '', data) # reomve special characters
    data = re.sub('https?://[A-Z-0-9a-z./]+', '', data)
    data = re.sub('_images/[A-Z-0-9a-z.jpg]+', '', data)
    data = re.sub('/[A-Z-0-9a-z.jpg]+', '', data) #reomve images
    data = data.lower() # convert each letter in lowercase
    # reomve emoticons
    emoticons = re.compile('[\U00010000-\U0010ffff]', flags=re.UNICODE)
    data=emoticons.sub(r'', data)
    data = data.replace("\\u", "")
    return data

#counter to count the tweets
count=0
for tweet in tp.Cursor(api.search,q='Halifax').items(10000): #fetch tweets using tweepy cursor and search api
    print(type(tweet._json),tweet._json)

    # convert raw tweets into string and store it in txt file
    file.write(str(tweet._json)+'\n')
    # store raw tweets in json file
    with open('search.json', 'a+') as outfile:
        json.dump(tweet._json, outfile, indent=4)
    # csv file to store cleaned tweets
    with open("search.csv",'a+') as csvfile:
        columns = ["created_at", "id", "name", "tweet", "user_id", "screen_name","location"]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        try:
            # filter the tweets and store them in csv file
            text = tweet_cleaning(tweet.text)
            dict={'created_at':tweet.created_at,'id':tweet.id,"name":tweet.user.name,"tweet":text,'user_id':tweet.user.id,
                  'screen_name':tweet.user.screen_name,'location':tweet.user.location}
            print(dict)
            # store tweet in as row in csv
            writer.writerow(dict)
        except:
            pass

    # increase count of tweets
    count+=1
    print(count)

print("Total tweets collected are",count)


