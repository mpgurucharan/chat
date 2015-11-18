import sys
import time
import datetime
import json
import nltk
import re
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
from textblob import Blobber
from json import JSONEncoder
from collections import Counter
from nltk.corpus import stopwords
from pymongo import MongoClient
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)
		
#tb = Blobber(analyzer=NaiveBayesAnalyzer())


def isReal(txt):
    try:
        float(txt)
        return True
    except ValueError:
        return False	


def main():
	start_time = time.time()
	#Read each line of output.txt and extract text
	input_file = open("C:\\Local_Drives\\D_Drive\\Workspace_Kepler\\chat\\python\\input\\t_input_1.txt")
#	input_file = open("C:\\Users\\v187760\\Documents\\SametimeFileTransfers\\vzw_chat_Aug14.txt")	
	split_char = "||@"
	chat_json = []
	i =0
	cachedStopWords = set(stopwords.words("english"))
	cachedStopWords.update(['verizon','fios'])
	out_file = open("C:\\Local_Drives\\D_Drive\\Workspace_Kepler\\chat\\python\\t_output.txt","w")	

	# Connection to Mongo DB
	try:
		client = MongoClient('mongodb://113.128.162.18:27017/')
		db = client['TIMELINE']
		collection = db.gurucollection
		print "Connected successfully!!!"
	except pymongo.errors.ConnectionFailure, e:
	   print "Could not connect to MongoDB: %s" % e 
	
	pattern = re.compile(r'\b(' + r'|'.join(cachedStopWords) + r')\b\s*')
	
	for line in input_file:
		words = line.split(split_char)
		try:						
			if words[23] != "" and words[24] != "":
				i = i + 1
				print i
				agent_chat = words[23].lower()
#				print 'raw', agent_chat
#				agent_chat = ' '.join([word for word in agent_chat.split() if word not in cachedStopWords])  						
				agent_chat = pattern.sub('', agent_chat)  				
#				print 'first:', agent_chat								
#				agent_chat = re.sub(r'd.{0,}\.\d{0,}',"",agent_chat)  								
#				print 'second:', agent_chat				
				
				agent_textb = TextBlob(agent_chat)
				agent_polarity = agent_textb.sentiment.polarity
				agent_words_list = agent_textb.words
				agent_words_counts = Counter(agent_words_list)
				
				agent_word_count_json = []
				for key, value in agent_words_counts.iteritems():
					if value > 1:
						agent_word_count_json.append({'word':key, 'count':value})

#				print agent_word_count_json
				
				agent_phrases_list = agent_textb.noun_phrases
				agent_phrases_counts = Counter(agent_phrases_list)				
				agent_phrases_count_json = []
				for key, value in agent_phrases_counts.iteritems():
					keyblob = TextBlob(key)
					agent_phrases_count_json.append({'phrase':key, 'count':value, 'sentiment':keyblob.sentiment.polarity})
				
#				print agent_phrases_count_json
				
				customer_chat = words[24].lower()				
				customer_chat = pattern.sub('', customer_chat)  				
#				customer_chat = re.sub(r'd.{0,}\.\d{0,}',"",customer_chat)  				
				
				customer_textb = TextBlob(customer_chat)							
				customer_polarity = customer_textb.sentiment.polarity
				customer_words_list = customer_textb.words
				customer_words_counts = Counter(customer_words_list)

				customer_word_count_json = []
				customer_all_words_count_json = []
				for key, value in customer_words_counts.iteritems():
					if value > 1:					
#						customer_words_count_json = [{'words':key, 'count':value}]													
						customer_word_count_json.append({'word':key, 'count':value})

				customer_phrases_list = customer_textb.noun_phrases
				customer_phrases_counts = Counter(customer_phrases_list)				
				customer_phrases_count_json = []
				for key, value in customer_phrases_counts.iteritems():
					keyblob = TextBlob(key)
					customer_phrases_count_json.append({'phrase':key, 'count':value, 'sentiment':keyblob.sentiment.polarity})
				
#				print customer_phrases_count_json

#				customer_words = []								
#				 if word not in cachedStopWords and not word.isdigit() and not isReal(word):
#					customer_words.append(word)				
#				customer_words_counts = Counter(customer_words)
				
				
#				print agent_polarity, customer_polarity
#				chat_json.append(JSONEncoder().encode({'agent_chat': words[23],'agent_sentiment': agent_sentiment, 'agent_polarity': words[24], 'customer_sentiment': customer_polarity}))
				single_json = {'chat_id':i,'agent_chat': agent_chat,'agent_polarity': agent_polarity, 'agent_word_count':agent_word_count_json, 'agent_phrases_count':agent_phrases_count_json, 'customer_chat': customer_chat, 'customer_polarity': customer_polarity,'customer_word_count':customer_word_count_json, 'customer_phrases_count':customer_phrases_count_json,'timestamp': datetime.datetime.now()}
#				chat_json.append({'chat_id':i,'agent_chat': agent_chat,'agent_polarity': agent_polarity, 'agent_words_counts':agent_words_counts, 'customer_chat': customer_chat, 'customer_polarity': customer_polarity,'customer_words_counts':customer_words_counts})	
				chat_json.append(single_json)	
				collection.insert(single_json)				

		except IndexError:
			print "NA"

#	print json.dumps(chat_json)			
#	collection.insert(chat_json)
	
#	json.dump(chat_json, out_file)					
	out_file.close()
	input_file.close();
	print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
	main()		