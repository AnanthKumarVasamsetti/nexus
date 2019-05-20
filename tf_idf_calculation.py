import json
from pprint import pprint
import math

def get_unigrams(conversation):
	unigrams = []
	for tkn in conversation:
		if(isinstance(tkn, str)):
			unigrams.append(tkn)
	
	return unigrams

def calculate_tf(unigram_tokens):
	tf_set = {
		tkn : unigram_tokens.count(tkn) / len(unigram_tokens) for tkn in unigram_tokens
	}

	return tf_set

def calculate_idf(words, conversations):
	words_set = list(set(words))    # Make sure that there are no duplicates in the colected words
	idf_set = {}
	w_appeared = 0

	for w in words_set:
		w_appeared = 0
		for convo_id in conversations:
			if w in conversations[convo_id]:
				w_appeared = w_appeared + 1
		
		idf_set[w] = math.log((len(conversations) / w_appeared))
	
	return idf_set

def order_word_to_convo(conversations_tf_idf):
    reordered_tf_idf = {}

    for convo_id in conversations_tf_idf:
        for tkn in conversations_tf_idf[convo_id]:
            tkn = tkn.replace('.', '_')
            tkn = tkn.replace(':', '_')
            if tkn not in list(reordered_tf_idf.keys()):
                reordered_tf_idf[tkn] = {}

            reordered_tf_idf[tkn][convo_id] = conversations_tf_idf[convo_id][tkn]
    
    return reordered_tf_idf

def calculated_tf_idf_for_unigrams(conversation_unigrams):
	tf_idf_set = {}
	idf_set = {}
	word_set = []
	for convo_id in conversation_unigrams:
		tf_idf_set[convo_id] = calculate_tf(conversation_unigrams[convo_id])    #Term frequency for each word in each conversation
		word_set = word_set + conversation_unigrams[convo_id]   # Gathering all tokens from each conversation for IDF calculation
	
	idf_set = calculate_idf(word_set, conversation_unigrams)

	for convo_id in tf_idf_set:
		for word in tf_idf_set[convo_id]:
			tf_idf_set[convo_id][word] = tf_idf_set[convo_id][word] * idf_set[word]

	return order_word_to_convo(tf_idf_set)


def calculate(conversations_data):
    conversation_unigrams = {}
    for convo_id in conversations_data:
        conversation_unigrams[convo_id] = get_unigrams(conversations_data[convo_id])
    
    reordered_set = calculated_tf_idf_for_unigrams(conversation_unigrams)
    return reordered_set
