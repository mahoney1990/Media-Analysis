# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 13:25:06 2021

@author: mahon
"""

#Load all required packages
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
import nltk 
import string
from nltk.util import ngrams
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import sqlite3
from sqlite3 import Error



#Contents
# 0. CNN Webscrape/tokenization
# 1. CNN Twitter tokenization
# 2. Fox Webscrape/tokenization
# 3. Fox Twitter tokenization
# 4. Concatinate all data
# 5. Get word frequency counts
# 6. Organize master data frame

#%% 0. CNN Webscrape/tokenization
# Required input: CNN_URLS_List.csv, Excluded_Words.csv, Excluded_Bigrams.csv, Excluded_Trigrams.csv
# Output: 3 dataframes CNN_words_df, CNN_bigrams_df, CNN_trigrams_df





#Import Exclusion lists -- required for all sections!
stops=set(stopwords.words('english'))

excluded_words=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Words.csv")
excluded_words=excluded_words.values.tolist()

excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Bigrams.csv")
excluded_bigrams=excluded_bigrams.values.tolist()

excluded_trigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Trigrams.csv")
excluded_trigrams=excluded_trigrams.values.tolist()

#Convert to iterable
for i in range(len(excluded_bigrams)):
    excluded_bigrams[i] = excluded_bigrams[i][0]

for i in range(len(excluded_trigrams)):
    excluded_trigrams[i] = excluded_trigrams[i][0]

for i in range(len(excluded_words)):
    excluded_words[i] = excluded_words[i][0]

numbers=range(1000)
numbers=list(numbers)
numbers=[format(x, '') for x in numbers]

#Connect to SQLite database
conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\pythonsqlite.db')

sql_create_bigrams_table = """ CREATE TABLE IF NOT EXISTS scrape_bigrams_v1 (
                                        Speaker text,
                                        Date text ,
                                        Network text,
                                        Phrase text
                                    ); """

c = conn.cursor()
c.execute(sql_create_bigrams_table)

sql_create_words_table = """ CREATE TABLE IF NOT EXISTS scrape_words_v1 (
                                        Speaker text,
                                        Date text ,
                                        Network text,
                                        Word text
                                    ); """

c = conn.cursor()
c.execute(sql_create_words_table)

sql_create_trigrams_table = """ CREATE TABLE IF NOT EXISTS scrape_trigrams_v1 (
                                        Speaker text,
                                        Date text ,
                                        Network text,
                                        Phrase text
                                    ); """

c = conn.cursor()
c.execute(sql_create_trigrams_table)


#Extract URLS
data=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\CNN_URLS_List.csv")
urls=data['URLS']
N=len(urls)

#Append URLs with correct address -- CNN Only
for i in range(N):
    urls[i]='http://transcripts.cnn.com/'+urls[i][7:]

#Initialize vectors and DFs
texts = ["" for i in range(N)]

CNN_words_df=pd.DataFrame()
CNN_bigrams_df=pd.DataFrame()
CNN_trigrams_df=pd.DataFrame()

import random
size=2500
rand_list=random.sample(range(N), size)

from urllib.error import HTTPError

its=1
print_skip=25

#WEBSCRAPE UTILITY
for j in range(4750,N):

    dt=data['Year'][j]+"-"+data['Month'][j]
    if  j % print_skip ==0:
        print(f"Scraping CNN. Progress:"+str(j/N*100))
    try:    
        page = urlopen(urls[j])
    

        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
    
        soup = BeautifulSoup(html, "html.parser")
        texts[j]=soup.get_text()
        texts[j]=texts[j].replace('\n','')


        tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(texts[j])
        tokens = list(filter(lambda token: token not in string.punctuation, tokens))
        filter(lambda x: x in printable, tokens)
        tokens = [sub.replace("ago", '') for sub in tokens]
        tokens = [sub.replace('+', '') for sub in tokens] 
        tokens = [sub.replace(',', '') for sub in tokens] 
        tokens = [sub.replace('`', '') for sub in tokens] 
        tokens = [sub.replace('-', '') for sub in tokens]
        tokens = [sub.replace("'", '') for sub in tokens]
        tokens = [sub.replace("’", '') for sub in tokens]
        tokens = [sub.replace("”", '') for sub in tokens]
        tokens = [sub.replace("”", '') for sub in tokens]
        tokens = [sub.replace("‘", '') for sub in tokens]
        tokens = [sub.replace("“", '') for sub in tokens]
        tokens = [sub.replace(")", '') for sub in tokens]
        tokens = [sub.replace("(", '') for sub in tokens]
        tokens = [sub.replace("]", '') for sub in tokens]
        tokens = [sub.replace("[", '') for sub in tokens]
        tokens = [sub.replace("00", '') for sub in tokens]
        tokens = [sub.replace("000", '') for sub in tokens]
        tokens = [sub.replace('."', '') for sub in tokens]
        tokens = [sub.replace('."', '') for sub in tokens]
        tokens = [sub.replace('.', '') for sub in tokens]
        tokens = [sub.replace('NewsFacebookTwitterFlipboardPrintEmailYou', '') for sub in tokens]
        tokens = [sub.replace('NewsFacebookTwitterFlipboardPrintEmailNow', '') for sub in tokens]
        tokens = [sub.replace('"', '') for sub in tokens]
        tokens = [sub.replace('Fox', '') for sub in tokens]
        tokens = [sub.replace('com', '') for sub in tokens] 
        tokens=list(filter(lambda a: a != '', tokens))
        tokens=list(filter(lambda a: not a in numbers,tokens))
        tokens=list(filter(lambda w: not w in stops,tokens))
        tokens = list(filter(lambda token: token not in excluded_words, tokens))
   
    
        bigram = list(ngrams(tokens, 2)) 
        for i in range(len(bigram)):
            bigram[i]=str(bigram[i])

        bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
    
        trigram=list(ngrams(tokens,3))
        for i in range(len(trigram)):
            trigram[i]=str(trigram[i])

        trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))
    
    

        size_token=len(tokens)
        size_bigram=len(bigram)
        size_trigram=len(trigram)
    
    #Generate Words DF
    
        Net=data['Program'][j]
        lst2=[Net]*size_token
        lst3=[dt]*size_token
        lst4=['CNN']*size_token
    
    
        words_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Word': tokens})
    
    
    #Generate Bigrams DF
    
        Net=data['Program'][j]
        lst2=[Net]*size_bigram
        lst3=[dt]*size_bigram
        lst4=['CNN']*size_bigram
    
        bigrams_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Phrase': bigram})
    
    #Generate Trigrams DF
    
        Net=data['Program'][j]
        lst2=[Net]* size_trigram
        lst3=[dt] * size_trigram
        lst4=['CNN']*size_trigram
    
        trigrams_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Phrase': trigram})
    
    #Write to SQL database
        bigrams_df_temp.to_sql('scrape_bigrams_v1', conn, if_exists='append', index=False)
        words_df_temp.to_sql('scrape_words_v1', conn, if_exists='append', index=False)
        trigrams_df_temp.to_sql('scrape_trigrams_v1', conn, if_exists='append', index=False)

    
        del bigrams_df_temp
        del words_df_temp
        del trigrams_df_temp
    
        
    except: HTTPError
    its=its+1
    
    
    

#%% 1.  CNN Twitter tokenization
# Required Inputs: CNN.csv, Excluded_Words.csv, Excluded_Bigrams.csv, Excluded_Trigrams.csv
# Outputs: 3 Dataframes CNN_tweet_words_df, CNN_tweet_bigrams_df, CNN_tweet_trigrams_df

CNN_Tweets=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\CNN.csv")

N=len(CNN_Tweets['text'])

#Clean Dates -- Also takes too fucking long
CNN_Tweets['Date']=''
date=list(CNN_Tweets['created_at'])
for j in range(N):
    test=j/10000
    if test.is_integer():
        print("Fixing Dates. Progress:")
        print(j/N)
    date[j]=date[j][0:7]    
    CNN_Tweets['Date'][j]=date[j]

date_range=set(list(CNN_Tweets['Date']))
date_range = list(date_range)
separator = ', '

K=len(date_range)
lst0=["CNN Twitter"]


CNN_Tweet_concat=pd.DataFrame()

#Aggregate by month before tokenizing -- this massivly improves run time (unlike all this other stupid shit)
for i in range(K):
    test=i/(15)
    if test.is_integer():
        print("Cleaning things. Progress:")
        print((i/K)*100)
    indx=CNN_Tweets.index[CNN_Tweets['Date'] == date_range[i]].tolist()
    texts=CNN_Tweets['text'][indx]
    texts=texts.tolist()
    txt=separator.join(texts)
    
    URLless_string = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', txt)
    
    lst1=[date_range[i]]
    lst2=[URLless_string]
    CNNTweet_df_temp=pd.DataFrame({'Speaker':lst0, 'Date': lst1,'Text': lst2})
    
    CNN_Tweet_concat=pd.concat([CNN_Tweet_concat,CNNTweet_df_temp])   

del CNN_Tweets
CNN_Tweets=CNN_Tweet_concat.reset_index()   
CNN_Tweets.reset_index()
#CNN_Tweet_concat.to_csv("concat.csv") Export function if you want it
#CNN_Tweets=pd.read_csv(r"C:\Users\mahon\concat.csv")

#Initialize output DFs
N=len(CNN_Tweets['Text'])
#texts = ["" for i in range(N)]
CNN_tweet_words_df=pd.DataFrame()
CNN_tweet_bigrams_df=pd.DataFrame()
CNN_tweet_trigrams_df=pd.DataFrame()


#Tokenizer/Filter function 
for j in range(N):
    
    test=j/(15)
    if test.is_integer():
        print("Tokenizing. Progress:")
        print((j/N)*100)
    
    texts=CNN_Tweets['Text'][j]
    date=CNN_Tweets['Date'][j][:7]

   # texts=texts[:texts.find('http')]
    
    #Split into n-grams, remove unnessesary shit
    tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(texts)
    tokens = [sub.replace("00", '') for sub in tokens]
    tokens = [sub.replace("000", '') for sub in tokens]
    tokens=list(filter(lambda w: not w in stops,tokens))
    tokens=list(filter(lambda a: not a in numbers,tokens))
    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
    tokens = list(filter(lambda token: token not in excluded_words, tokens))

    bigram = list(ngrams(tokens, 2)) 
    for i in range(len(bigram)):
        bigram[i]=str(bigram[i])

    bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
    
    trigram=list(ngrams(tokens,3))
    for i in range(len(trigram)):
        trigram[i]=str(trigram[i])

    trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))


    size_token=len(tokens)
    size_bigram=len(bigram)
    size_trigram=len(trigram) 

    #Generate Words DF
    ID=CNN_Tweets['Speaker'][j]
    lst1=[ID]*size_token
    lst3=[date]*size_token
    lst4=['CNN']*size_token
    
    words_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Network':lst4,'Word': tokens})
    
    
    #Generate Bigrams DF
    ID=CNN_Tweets['Speaker'][j]
    lst1=[ID] * size_bigram
    lst3=[date]*size_bigram
    lst4=['CNN']*size_bigram
    
    bigrams_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Network':lst4,'Phrase': bigram})
    
    
    #Generate Trigrams DF
    ID=CNN_Tweets['Speaker'][j]
    lst1=[ID] * size_trigram
    lst3=[date] * size_trigram
    lst4=['CNN'] * size_trigram
    
    trigrams_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Network':lst4,'Phrase': trigram})
    
    bigrams_df_temp.to_sql('scrape_test_bigrams', conn, if_exists='append', index=False)
    words_df_temp.to_sql('scrape_test_words', conn, if_exists='append', index=False)
    trigrams_df_temp.to_sql('scrape_test_trigrams', conn, if_exists='append', index=False)

    
del CNN_Tweets
del CNNTweet_df_temp
del CNN_Tweet_concat


#%% 2. Fox Webscrape
# Required inputs: FOX_URLS_List.csv, Excluded_Words.csv, Excluded_Bigrams.csv, Excluded_Trigrams.csv
#

data=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\FOX_URLS_List.csv")

host_names=data['Host'].tolist()
host_names = list(set(data['Host']))


size=len(list(data['URL']))

import random
size=7500
rand_list=random.sample(range(N), size)

texts = ["" for i in range(N)]

fox_words_df=pd.DataFrame()
fox_bigrams_df=pd.DataFrame()
fox_trigrams_df=pd.DataFrame()

import string
printable = set(string.printable)
its=0
rand_list.index(j)
rand_list=rand_list[2269:]

print_con=10
its=0
for i in range(3,len(host_names)):
    print(i)
    FN_words_df=pd.DataFrame()
    FN_bigrams_df=pd.DataFrame()
    FN_trigrams_df=pd.DataFrame()

    words_df_temp=pd.DataFrame()
    bigrams_df_temp=pd.DataFrame()
    trigrams_df_temp=pd.DataFrame()
    
    urls=data[data.Host==host_names[i]]
    N=len(urls)
    
    for j in range(49,N):
        its=its+1
        texts=str()        

        if its % print_con==0:
            print("Scraping Fox News. Progress:")
            print(str((its/N)*100))
    
        page = urlopen(list(urls["URL"])[j])

        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
   
        if list(urls['Host'])[j]=="Bill O'Reilly":
           start=html.find('div class="showBody"')
           end=html.rfind('bottom-wrapper') 
           html=html[start:end]
           dt=list(urls['Year'])[j][:7]+"-"+list(urls['Month'])[j]
    
        if list(urls['Host'])[j]!="Bill O'Reilly":
            dt=list(urls['Year'])[j][:7]+"-"+list(urls['Month'])[j]
    


      
        soup = BeautifulSoup(html, "html.parser")
        texts=soup.get_text()
        texts.replace('\n','')
        if list(urls['Host'])[j]!="Bill O'Reilly":
            try:
                texts=texts.split('By')[1]
                texts=texts.split('Get all the stories')[0]
            except: NameError
        else:
            texts.replace('\t','')
            pass
            
        
        tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(texts)
        tokens = list(filter(lambda token: token not in string.punctuation, tokens))
        filter(lambda x: x in printable, tokens)
        tokens = [sub.replace("ago", '') for sub in tokens]
        tokens = [sub.replace('+', '') for sub in tokens] 
        tokens = [sub.replace(',', '') for sub in tokens] 
        tokens = [sub.replace('`', '') for sub in tokens] 
        tokens = [sub.replace('-', '') for sub in tokens]
        tokens = [sub.replace("'", '') for sub in tokens]
        tokens = [sub.replace("’", '') for sub in tokens]
        tokens = [sub.replace("”", '') for sub in tokens]
        tokens = [sub.replace("”", '') for sub in tokens]
        tokens = [sub.replace("‘", '') for sub in tokens]
        tokens = [sub.replace("“", '') for sub in tokens]
        tokens = [sub.replace(")", '') for sub in tokens]
        tokens = [sub.replace("(", '') for sub in tokens]
        tokens = [sub.replace("]", '') for sub in tokens]
        tokens = [sub.replace("[", '') for sub in tokens]
        tokens = [sub.replace('."', '') for sub in tokens]
        tokens = [sub.replace("00", '') for sub in tokens]
        tokens = [sub.replace("000", '') for sub in tokens]
        tokens = [sub.replace('NewsFacebookTwitterFlipboardPrintEmailYou', '') for sub in tokens]
        tokens = [sub.replace('NewsFacebookTwitterFlipboardPrintEmailNow', '') for sub in tokens]
        tokens = [sub.replace('"', '') for sub in tokens]
        tokens = [sub.replace('Fox', '') for sub in tokens]
        tokens = [sub.replace('com', '') for sub in tokens] 
        tokens=list(filter(lambda a: a != '', tokens))
        tokens=list(filter(lambda w: not w in stops,tokens))
        tokens=list(filter(lambda a: not a in numbers,tokens))
        tokens = list(filter(lambda token: token not in excluded_words, tokens))
       
        
        bigram = list(ngrams(tokens, 2)) 
        for i in range(len(bigram)):
            bigram[i]=str(bigram[i])
    
        bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
        
        trigram=list(ngrams(tokens,3))
        for i in range(len(trigram)):
            trigram[i]=str(trigram[i])
    
        trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))
        
        
    
        size_token=len(tokens)
        size_bigram=len(bigram)
        size_trigram=len(trigram)
        
        #Generate Words DF
        if list(urls['Host'])[j] != "Bill O'Reilly":
            Net=list(urls['Host'])[j][2:]
        else:
            Net = "Bill"
        
        lst2=[Net]*size_token
        
        lst3=[dt]*size_token
        lst4=['Fox']*size_token
        
        words_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Word': tokens})
        
        
        #Generate Bigrams DF
        

        lst2=[Net]*size_bigram
        lst3=[dt]*size_bigram
        lst4=['Fox']*size_bigram
        
        bigrams_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Phrase': bigram})
        
        #Generate Trigrams DF
        

        lst2=[Net]* size_trigram
        lst3=[dt] * size_trigram
        lst4=['Fox']*size_trigram
        
        trigrams_df_temp=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Network':lst4,'Phrase': trigram})
        
        #Pre-aggregate
        FN_words_df = pd.concat([FN_words_df,words_df_temp])
        FN_bigrams_df = pd.concat([FN_bigrams_df,bigrams_df_temp])
        FN_trigrams_df = pd.concat([FN_trigrams_df,trigrams_df_temp])

    
    FN_bigrams_df=FN_bigrams_df.replace("ll O'Reilly","Bill")
    FN_trigrams_df=FN_trigrams_df.replace("ll O'Reilly","Bill")


    FN_bigrams_df.to_sql('scrape_bigrams_v1', conn, if_exists='append', index=False)
    FN_words_df.to_sql('scrape_words_v1', conn, if_exists='append', index=False)
    FN_trigrams_df.to_sql('scrape_trigrams_v1', conn, if_exists='append', index=False)

    
    
    

    del trigrams_df_temp
    del bigrams_df_temp
    del words_df_temp
    del FN_trigrams_df
    del FN_bigrams_df
    del FN_words_df



#%% 3. Fox Twitter Filtering/Tokenization 

FN_Tweets=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\FN_Tweets.csv")

FN_Tweet_concat=pd.DataFrame()


N=len(FN_Tweets['text'])

FN_Tweets['Date']=''
date=list(FN_Tweets['created_at'])
for j in range(N):
    test=j/10000
    if test.is_integer():
        print("Fixing Dates. Progress:")
        print(j/N)
    date[j]=date[j][0:7]    
    FN_Tweets['Date'][j]=date[j] #This is a stupid, stupid fucking way to make this work
    #Python even tells you its bad

date_range=set(list(FN_Tweets['Date']))
date_range = list(date_range)
separator = ', '

K=len(date_range)

lst0=["Fox Twitter"]

#Aggregate by month before filtering
for i in range(K):
    print(i)
    indx=FN_Tweets.index[FN_Tweets['Date'] == date_range[i]].tolist()
    texts=FN_Tweets['text'][indx]
    texts=texts.tolist()
    txt=separator.join(texts)
    #Remove URLs
    URLless_string = re.sub(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', '', txt)
    lst1=[date_range[i]]
    lst2=[URLless_string]
    FNTweet_df_temp=pd.DataFrame({'Speaker':lst0, 'Date': lst1,'Text': lst2})
    FN_Tweet_concat=pd.concat([FN_Tweet_concat,FNTweet_df_temp])   

    FN_Tweet_concat['Text'][i]=text.replace('@(.-?)""','')
    
    
FN_Tweet_concat.to_csv("concat.csv") 

N=len(FN_Tweets['Text'])

import random
size=100

rand_list=random.sample(range(N), size)

texts = ["" for i in range(N)]

j=0

import random
size=100
rand_list=random.sample(range(N), size)

texts = ["" for i in range(N)]
FN_tweet_words_df=pd.DataFrame()
FN_tweet_bigrams_df=pd.DataFrame()
FN_tweet_trigrams_df=pd.DataFrame()

#Main tokenization/filerting function    
for j in range(N):
    
    print(j)
    
    texts=FN_Tweets['Text'][j]
    date=FN_Tweets['Date'][j][:7]

    texts=texts[:texts.find('http')]
    texts=texts.replace("'",'')
    texts=re.sub('@[^\s]+','',texts)
    texts=re.sub(',"','',texts)
    texts=re.sub('“','',texts)
    texts=re.sub('”','',texts)
    
    tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(texts)
    tokens = [sub.replace("00", '') for sub in tokens]
    tokens = [sub.replace("000", '') for sub in tokens]
    tokens=list(filter(lambda w: not w in stops,tokens))
    tokens=list(filter(lambda a: not a in numbers,tokens))
    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
    tokens = list(filter(lambda token: token not in excluded_words, tokens))

    bigram = list(ngrams(tokens, 2)) 
    for i in range(len(bigram)):
        bigram[i]=str(bigram[i])

    bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
    
    trigram=list(ngrams(tokens,3))
    for i in range(len(trigram)):
        trigram[i]=str(trigram[i])

    trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))


    size_token=len(tokens)
    size_bigram=len(bigram)
    size_trigram=len(trigram) 

 #Generate Words DF
    ID=FN_Tweets['Speaker'][j]
    lst1=[ID]*size_token
    
    lst3=[date]*size_token
    
    
    words_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Word': tokens})
    
    
    #Generate Bigrams DF
    ID=FN_Tweets['Speaker'][j]
    lst1=[ID] * size_bigram
    
    lst3=[date]*size_bigram
    
    bigrams_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Phrase': bigram})
    
    
    #Generate Trigrams DF
    ID=FN_Tweets['Speaker'][j]
    lst1=[ID] * size_trigram
    
    
    lst3=[date] * size_trigram
    
    trigrams_df_temp=pd.DataFrame({'Speaker': lst1,'Date':lst3,'Phrase': trigram})
    
    bigrams_df_temp.to_sql('scrape_test_bigrams', conn, if_exists='append', index=False)
    words_df_temp.to_sql('scrape_test_words', conn, if_exists='append', index=False)
    trigrams_df_temp.to_sql('scrape_test_trigrams', conn, if_exists='append', index=False)

    

#%%
conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\pythonsqlite.db')


phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM scrape_bigrams
                             GROUP BY Phrase
                             HAVING COUNT(*)>19''', conn)

phrases=pd.read_csv("phrases.csv")
phrases=phrases['Phrase']
phrases=list(phrases)
                             
words=pd.read_sql_query('''SELECT Word, COUNT(*)
                             FROM scrape_words
                             GROUP BY Word
                             HAVING COUNT(*)>19''', conn)


dates=pd.read_sql_query('''SELECT DISTINCT Date
                                 FROM scrape_bigrams_v1;
                                 ''',conn)

dates=list(dates['Date'])

speakers=pd.read_sql_query('''SELECT DISTINCT Speaker
                                 FROM scrape_bigrams_v1;
                                 ''',conn)

speakers.to_csv("Speakers.csv")

speakers=pd.read_csv("Speakers.csv")
speakers=list(speakers['Speaker'])      


bigrams_df=pd.DataFrame()
#Rearrange
for i in range(len(speakers)):
    
    speaker="'"+speakers[i]+"'"
    print(speaker)
   
    if i==0:
        print("Building Bigram Counts. Progress:")
    x=str(((i/(len(speakers))*100)))
    print(x[:4])
    
    df1=pd.DataFrame()
    df1= pd.read_sql_query('''SELECT*FROM scrape_bigrams_v1
                              WHERE Speaker='''+speaker+''';''',conn)
    
    
    dates=set(list(df1['Date']))
    dates=list(dates)
    
    Network=df1['Network'][0]
    
    bigrams_temp=pd.DataFrame()
    for j in range(len(dates)):
        print(j)
        
        df2=df1[df1['Date']==dates[j]]
        phrase_count=df2['Phrase'][df2['Phrase'].isin(phrases)].value_counts(ascending=True)
        phrase_count=pd.DataFrame(phrase_count)
        
        phrase_count=pd.DataFrame.transpose(phrase_count)
        phrase_count.insert(0, "Speaker", speakers[i], True)
        phrase_count.insert(1, "Date", dates[j], True)
        phrase_count.insert(2, "Network", Network , True)
        bigrams_temp=pd.concat([bigrams_temp,phrase_count])
    
    bigrams_df=pd.concat([bigrams_df,bigrams_temp])                            
    del bigrams_temp 
    del df2
    del df1                       
                            
bigrams_df=bigrams_df.fillna(0)
bigrams_df.to_csv('Bigram_Counts.csv')

bigrams_df=pd.read_csv('Bigram_Counts.csv')

datz=new_df['Date']
datz=list(datz)

datz.index("#VALUE!-#VALUE!")
g = (i for i, e in enumerate(datz) if e == "#VALUE!-#VALUE!")
next(g)

remove=[592,638,693,829,869,1133,1169,1272,1539,1627,1715,1796,1833,1954,2039,2082,2118,2164,2229,2257]

bigrams_df['Date'][remove]
new_df=bigrams_df.drop(index=remove,axis=0)
print(new_df['Speaker'])
new_df.to_csv('Bigram_Counts.csv')

#%%

import sqlite3
import pandas as pd
conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\pythonsqlite.db')


phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM scrape_trigrams_v1
                             GROUP BY Phrase
                             HAVING COUNT(*)>29''', conn)

phrases.to_csv('Trigram_phrases.csv')

phrases=pd.read_csv('Trigram_phrases.csv')
phrases=phrases['Phrase']
phrases=list(phrases)


speakers=pd.read_csv("Speakers.csv")
speakers=list(speakers['Speaker'])      


trigrams_df=pd.DataFrame()
#Rearrange
for i in range(1,len(speakers)):
    
    speaker="'"+speakers[i]+"'"
    print(speaker)
   
    if i==0:
        print("Building Bigram Counts. Progress:")
    x=str(((i/(len(speakers))*100)))
    print(x[:4])
    
    df1=pd.DataFrame()
    df1= pd.read_sql_query('''SELECT*FROM scrape_trigrams_v1
                              WHERE Speaker='''+speaker+''';''',conn)
    
    
    dates=set(list(df1['Date']))
    dates=list(dates)
    
    Network=df1['Network'][0]
    
    bigrams_temp=pd.DataFrame()
    for j in range(len(dates)):
        print(j)
        
        df2=df1[df1['Date']==dates[j]]
        phrase_count=df2['Phrase'][df2['Phrase'].isin(phrases)].value_counts(ascending=True)
        phrase_count=pd.DataFrame(phrase_count)
        
        phrase_count=pd.DataFrame.transpose(phrase_count)
        phrase_count.insert(0, "Speaker", speakers[i], True)
        phrase_count.insert(1, "Date", dates[j], True)
        phrase_count.insert(2, "Network", Network , True)
        bigrams_temp=pd.concat([bigrams_temp,phrase_count])
    
    trigrams_df=pd.concat([trigrams_df,bigrams_temp])                            
    del bigrams_temp 
    del df2
    del df1












