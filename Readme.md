# Nexus

This is a documentation search project which is curretly running on "Apple" support tickets data from Twitter. Thanks **_Kaggle_** :)

It is developed by combining **TF-IDF** and **n-gram** search which provided better results than using them individually.

###Working:

_This is a high level overview of workflow_

    Work flow from entering query to search results.
    1. User_input --> Search
    2. Keyword extraction, this includes phases like stopwords filtering, POS extraction, etc.
    3. Lemmatizing
    4. N-gram generation
    5. Picking from Word2Doc model (explained later)
    6. Relevance scores calculation
    7. Ranking results based on their relevance scores
    8. Displaying results

##Overview
Let me skip the boring parts of cleaning the data but the following are the techniques used for cleaning,
1. Filtering out HTML tags, emocations, wild card characters, '@' representations (if any)
2. Preserving URL's (It's is choice we can filter out them too)
3. Converting to lowercase and removing stopwords
4. Parts of speech identification to identify words which belongs to the classes of Nouns, Adjectives, verbs, etc.,
5. Lemmatizing


Once the data gets cleaned out we will start making n-grams generation (_basically doing bi-grams here_) along with preserving the individual keywords.

Now comes the TF-IDF calculation part where we will be usin TF and IDF of each individual word to calculate it's respective weightage for that particular document. In this we shall calculate the TF-IDF of each n-gram also by taking out the average of each word's TF-IDF values and assigning it to the respective n-gram.

After calculating the formated data would be like the following

    {
        support_tkt_92729 :{
            'itunes' : 0.9,    //This is weightage of that word for 'support_tkt_92729'
            'safari' : 0.4539,
            'ios11' : 0.1242,
            'keyboard' : 0.5939,
            .
            .
            .
            ('itunes', 'safari') : 0.67695,  //Bi-grams
            ('safari', 'ios11') : 0.28905,
            ('ios11', 'keyboard') : 0.35905,
            .
            .
        },
        support_tkt_54306 :{
            'safari' : 0.2772,
            'ios11' : 0.89573,
            'volume' : 0.29485,
            'screen' : 0.1121,
            .
            .
            .
            ('safari', 'ios11') : 0.586465,
            ('ios11', 'volume') : 0.59529,
            ('volume', 'screen') : 0.203475,
            .
            .

        }
    }

So as you can see here the words **safari** and **ios11** are present both **support_tkt_92729** and **support_tkt_54306** . Now on building word2Doc comes into picture, here words are mapped against there respective documents in which they appeared with their weightages.

On running under Word2Doc convertor the data will be reformated into the following

    {
        'itunes' :{
            'support_tkt_92729' : 0.9,
        },
        'safari' : {
            'support_tkt_92729' : 0.4539,
            'support_tkt_54306' : 0.2772
        },
        'ios11' : {
            'support_tkt_92729' : 0.1242,
            'support_tkt_54306' : 0.89573,
        },
        'keyboard' :{
            'support_tkt_92729' : 0.5939
        },
        .
        .
        ('itunes','safari') :{
            'support_tkt_92729' : 0.67695,
        },
        ('safari', 'ios11') :{
            'support_tkt_92729' : 0.28905,
            'support_tkt_54306' : 0.586465
        },
        .
        .
    }
This approach helps to retrive the support tickets with minimum time lag from the **keywords** identified from **user query.**
Now relevance comes into picture, this is an elobrative phase because here comes the topics like synonyms, Enterprise based keyword weight management, User preferred search optimization, if a user want it be an addon on top of document (This basically comes under both relevance gathering and ranking), etc.,

This is the formula used to calculate weight for synonyms
-----_work in progress_-----
Synonym_weightage = Similarity(actual_word, synonym)
Synonym_weightage = actual_word_weightage + Synonym_weightage



So after calculating the relevance scores for the retrived documents ranking is done (_currently it is just sorting_) and proving the results to end user making documents with high relevant scores on the top.

### Tasks under work in progress
Synonym similarity enhancement.
Storing users clicks for a query to help in ranking the document.