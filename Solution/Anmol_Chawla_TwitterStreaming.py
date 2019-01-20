import tweepy
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from random import randint
consumer_key = "AN2meAGw1ml4R5viiFEmO9GOK"
consumer_secret = "EdlYhTEe1voXeOCwgzoimI7xBL3wsbpSmTBBppNxv6ph75xMIM"
access_key = "967948938-DIlY8bnM1qk8ygGpZJEwjZBcGhuky5yXvI6ueu4h"
access_secret = "nlmpzBsQwHG7V6eN3JJ68jfprtLsC90d8xYniQUQJ1WVs"
 


class StdOutListener(StreamListener):

  def on_data(self, data):
    global res
    global counter
    global avg
    global main_counter
    global lenght
    global all_counter
    
    top_five = {}
    hold = json.loads(data)
    avg = []
    main_counter = main_counter + 1

    if 'text' in hold:
      word = hold['text'].encode('utf-8')
      if counter <=100:
        res.append(word)
        counter = counter + 1
      else:
        temp = []
        for j in range(100):
          index = randint(0,all_counter)
          temp.append(index)
        if 50 in temp:
          remove = randint(0,100)
          del res[remove]
          res.append(word)
          #print(word)

          for i in res:
            actual = i
            tempo = len(actual)
            avg.append(tempo)
            b = [word for word in actual.split() if word.startswith('#')]
            for j in b:
              if (j != '# '):
                if j in top_five:
                  top_five[j] = top_five[j] + 1        
                else:
                  top_five[j] = 1
          
          sol = float(sum(avg))/100
          op = top_five.items()
          op.sort(key = lambda x:x[1], reverse = True)
          word1 = op[0]
          word2 = op[1]
          word3 = op[2]
          word4 = op[3]
          word5 = op[4]
          print("The number of the twitter from beginning: {}".format(main_counter))
          print("Top 5 hashtags:")
          print("{} : {}".format(word1[0].replace("#",""),word1[1]))
          print("{} : {}".format(word2[0].replace("#",""),word2[1]))
          print("{} : {}".format(word3[0].replace("#",""),word3[1]))
          print("{} : {}".format(word4[0].replace("#",""),word4[1]))
          print("{} : {}".format(word5[0].replace("#",""),word5[1]))
          print("Average length of a twitter is: {}".format(sol))
          print("\n")
          print("\n")
                  
            
      return True

  def on_error(self, status):
    print(status)



listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
stream = Stream(auth, listener,tweet_mode='extended')


res = []
counter = 0
main_counter = 0
avg = 0
lenght = 0
all_counter = 100
stream.filter(track = ["trump"],languages=["en"])