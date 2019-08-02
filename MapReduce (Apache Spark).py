# open file to read tweets
#file=open("stream.csv","r")
#file=open("stream_2.csv","r")
file=open("/home/ubuntu/Assignment-2/stream.csv","r")
# words to be counted
words=["not safe","safe","accident","long waiting","expensive","friendly","snow storm","good school","good schools","bad schools","bad school",
       "poor schools","poor school","immigrants","immigrant","pollution","bus","buses","parks","park","parking"]
# store the words found
found=[]

# split row by commas and find the words
for tweet in file:
    col=tweet.split(",")
    for word in words:
        for value in col:
            if word in value:
                count=value.count(word)
                for i in range(count):
                    found.append(word)

data=spark.sparkContext.parallelize(found)
# map the words that are found
data=data.map(lambda x:(x,1))
# reduce the word count
data=data.reduceByKey(lambda x,y:x+y)
data.collect()
# store frequency count in output files
data.coalesce(1).saveAsTextFile('word_frequency_count_1.txt')
#data.coalesce(1).saveAsTextFile('word_frequency_count_2.txt')

