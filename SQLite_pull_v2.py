# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 13:30:49 2022

@author: mahon
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 14:15:09 2021


@author: mahon
"""
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
import PyPDF2 
from PyPDF2 import PdfFileReader
from os import listdir
from os.path import isfile, join

from os import chdir

#%%

stops=set(stopwords.words('english'))

excluded_words=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Words.csv")
excluded_words=list(excluded_words['Excluded_words'])

excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Bigrams.csv")
excluded_bigrams=list(excluded_bigrams['Excluded_Bigrams'])

excluded_trigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Trigrams.csv")
excluded_trigrams=list(excluded_trigrams['Excluded_Trigrams'])

CNN_personalities=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\CNN_personalities.csv")
CNN_personalities=list(CNN_personalities['Name'])

Fox_personalities=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Fox_personalities.csv")
Fox_personalities=list(Fox_personalities['Name'])

conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\pythonsqlite.db')

its=0
date_list=[""]*84
for i in range(2015,2022):
    for j in range(1,13):
        if j<10:
            date_list[its]=str(i)+"-0"+str(j)
        else:
            date_list[its]=str(i)+"-"+str(j)
        its=its+1
        
date_list=date_list[3:81]

#%%Pull Trigrams

conn=sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\congress_db.db')

excluded_trigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Trigrams_congress.csv")
excluded_trigrams=list(excluded_trigrams['Excluded_Trigrams'])

proc_terms=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\proc_terms.csv")
proc_terms=list(proc_terms['Term'])


excluded_speakers=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_speakers.csv")
excluded_speakers=list(excluded_speakers['Name'])

dates=pd.read_sql_query('''SELECT DISTINCT Date
                                 FROM scrape_trigrams_congress_v1;
                                 ''',conn)

dates=list(dates['Date'])
dates=[its[0:7] for its in dates]
dates=list(set(dates))
dates.sort()
st='2014-09'
dates=dates[dates.index(st):]

phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM scrape_trigrams_congress_v1
                             GROUP BY Phrase
                             HAVING COUNT(*)>19''', conn)



ct=30000
phrases=phrases.sort_values('COUNT(*)',ascending=False)
phrases = phrases[~phrases['Phrase'].isin(excluded_trigrams)]
phrases=list(phrases['Phrase'][:ct])

for i in range(len(proc_terms)):
    phrases=[ x for x in phrases if proc_terms[i] not in x ]

phrases = list(filter(lambda phrase: phrase not in excluded_trigrams, phrases))

parties=['"Democrat"','"Republican"']
trigrams_df=pd.DataFrame()

#Rearrange


for k in range(2):

    Network=parties[k]
    print("Pivoting....:"+Network)
    df1= pd.read_sql_query('''SELECT*FROM scrape_trigrams_congress_v1
                                  WHERE Party='''+parties[k]+''';''',conn)    
    
    #df1['Date']=[its[0:7] for its in df1['Date']]
    
    speakers=list(set(df1['Speaker']))    
    speakers = list(filter(lambda speaker: speaker not in excluded_speakers, speakers))
    speakers.sort()
    

    for j in range(len(dates)):
            trigrams_temp=pd.DataFrame()    
        
            if j==0: print("Building Trigram Counts. Progress:")
            x=str(((j/(len(dates))*100)))
            print(x[:4])
            df2=df1[df1['Date']==dates[j]]
            
           
            for i in range(len(speakers)):
        
                speaker="'"+speakers[i]+"'"
            
                df3=df2[df2['Speaker'] == speakers[i]]
                if df3.empty:
                    continue
                #else:
                    #print('Not Empty: ' + speaker +" "+str(i))
                    
                
                phrase_count=df3['Phrase'][df3['Phrase'].isin(phrases)].value_counts(ascending=True)
                
                #if sum(phrase_count)==0:
                 #   print('Got one!')
                  #  continue
                
                phrase_count=pd.DataFrame(phrase_count)
                phrase_count=pd.DataFrame.transpose(phrase_count)
                phrase_count=phrase_count.astype('int16')
                phrase_count.insert(0, "Speaker", ' '.join(speakers[i].split(' ')[0:2]), True)
                phrase_count.insert(1, "Date", dates[j], True)
                phrase_count.insert(2, "Party", Network , True)
                trigrams_temp=pd.concat([trigrams_temp,phrase_count])
            
            trigrams_df=pd.concat([trigrams_df,trigrams_temp])  
            del trigrams_temp 
            del df2
     
    del df1                

print('Writing csv....')
trigrams_df=trigrams_df.fillna(0)
trigrams_df.to_csv('Trigram_Counts_congress_2014_2021_v1.csv')
print('Export Successful!')


#%%Pull Trigrams, media

conn=sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\corrected_data.db')


excluded_trigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Trigrams.csv")
excluded_trigrams=list(excluded_trigrams['Excluded_Trigrams'])

proc_terms=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\proc_terms_media.csv")
proc_terms=list(proc_terms['Term'])

dates=pd.read_sql_query('''SELECT DISTINCT Date
                                 FROM trigrams_cor;
                                 ''',conn)

dates=list(dates['Date'])
dates=[its[0:7] for its in dates]
dates=list(set(dates))
dates.sort()
st='2014-09'
dates=dates[dates.index(st):]

phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM trigrams_cor
                             GROUP BY Phrase
                             HAVING COUNT(*)>19''', conn)



ct=50000
phrases=phrases.sort_values('COUNT(*)',ascending=False)
phrases = phrases[~phrases['Phrase'].isin(excluded_trigrams)]
phrases=list(phrases['Phrase'][:ct])

for i in range(len(proc_terms)):
    phrases=[ x for x in phrases if proc_terms[i] not in x ]

phrases = list(filter(lambda phrase: phrase not in excluded_trigrams, phrases))

networks=['"Fox"','"CNN"']
trigrams_df=pd.DataFrame()

#Rearrange


for k in range(2):

    Network=networks[k]
    print("Pivoting....:"+Network)
    df1= pd.read_sql_query('''SELECT*FROM trigrams_cor
                                  WHERE Network='''+networks[k]+''';''',conn)    
    
    #df1['Date']=[its[0:7] for its in df1['Date']]
    
    speakers=list(set(df1['Speaker']))    
    speakers.sort()
    

    for j in range(len(dates)):
            trigrams_temp=pd.DataFrame()    
        
            if j==0: print("Building Trigram Counts. Progress:")
            x=str(((j/(len(dates))*100)))
            print(x[:4])
            df2=df1[df1['Date']==dates[j]]
            
           
            for i in range(len(speakers)):
        
                speaker="'"+speakers[i]+"'"
            
                df3=df2[df2['Speaker'] == speakers[i]]
                if df3.empty:
                    continue
                else:
                    print('Not Empty: ' + speaker +" "+str(i))
                    
                
                phrase_count=df3['Phrase'][df3['Phrase'].isin(phrases)].value_counts(ascending=True)
                
                if sum(phrase_count)==0:
                    continue
                
                phrase_count=pd.DataFrame(phrase_count)
                phrase_count=pd.DataFrame.transpose(phrase_count)
                phrase_count=phrase_count.astype('int16')
                phrase_count.insert(0, "Speaker", ' '.join(speakers[i].split(' ')[0:2]), True)
                phrase_count.insert(1, "Date", dates[j], True)
                phrase_count.insert(2, "Network", Network , True)
                trigrams_temp=pd.concat([trigrams_temp,phrase_count])
            
            trigrams_df=pd.concat([trigrams_df,trigrams_temp])  
            del trigrams_temp 
            del df2
     
    del df1                

print('Writing csv....')
trigrams_df=trigrams_df.fillna(0)
trigrams_df.to_csv('Trigram_Counts_2014_2021_v1.csv')
print('Export Successful!')


#%%Pull Bigrams, Congress

conn=sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\congress_db.db')

excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Replication\Excluded_Bigrams_congress_v2.csv")
excluded_bigrams=list(excluded_bigrams['Phrases'])

proc_terms=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\proc_terms.csv")
proc_terms=list(proc_terms['Term'])

dates=pd.read_sql_query('''SELECT DISTINCT Date
                                 FROM scrape_bigrams_congress_v1;
                                 ''',conn)

excluded_speakers=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_speakers.csv")
excluded_speakers=list(excluded_speakers['Name'])


dates=list(dates['Date'])
dates=[its[0:7] for its in dates]
dates=list(set(dates))
dates.sort()
st='2014-09'
dates=dates[dates.index(st):]

phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM scrape_bigrams_congress_v1
                             GROUP BY Phrase
                             HAVING COUNT(*)>19''', conn)



ct=16000
phrases=phrases.sort_values('COUNT(*)',ascending=False)
phrases = phrases[~phrases['Phrase'].isin(excluded_bigrams)]
phrases=list(phrases['Phrase'][:ct])

for i in range(len(proc_terms)):
    phrases=[ x for x in phrases if proc_terms[i] not in x ]

#phrases = list(filter(lambda phrase: phrase not in excluded_bigrams, phrases))

parties=['"Democrat"','"Republican"']
bigrams_df=pd.DataFrame()

#Rearrange

for k in range(2):

    Network=parties[k]
    print("Pivoting....:"+Network)
    df1= pd.read_sql_query('''SELECT*FROM scrape_bigrams_congress_v1
                                  WHERE Party='''+parties[k]+''';''',conn)    
    
    #df1['Date']=[its[0:7] for its in df1['Date']]
    #df1=df1[~df1['Phrase'].isin(excluded_bigrams)]
    #df1=df1[~df1['Speaker'].isin(excluded_speakers)]
    #df1['Date']=[its[0:7] for its in df1['Date']]
    


    

    for j in range(len(dates)):
            trigrams_temp=pd.DataFrame()    
        
            if j==0: print("Building Bigram Counts. Progress:")
            x=str(((j/(len(dates))*100)))
            print(x[:4])
            df2=df1[df1['Date']==dates[j]]
            
           
            speakers=list(set(df2['Speaker']))    
            speakers = list(filter(lambda speaker: speaker not in excluded_speakers, speakers))
            speakers.sort()
           
            for i in range(len(speakers)):
        
                speaker="'"+speakers[i]+"'"
            
                df3=df2[df2['Speaker'] == speakers[i]]
                if df3.empty:
                    continue
                else:
                    print('Not Empty: ' + speaker +" "+str(i))
                    
                
                phrase_count=df3['Phrase'][df3['Phrase'].isin(phrases)].value_counts(ascending=True)
                phrase_count=pd.DataFrame(phrase_count)
                phrase_count=pd.DataFrame.transpose(phrase_count)
                phrase_count=phrase_count.astype('int16')
                phrase_count.insert(0, "Speaker", ' '.join(speakers[i].split(' ')[0:2]), True)
                phrase_count.insert(1, "Date", dates[j], True)
                phrase_count.insert(2, "Party", Network , True)
                trigrams_temp=pd.concat([trigrams_temp,phrase_count])
            
            bigrams_df=pd.concat([bigrams_df,trigrams_temp])  
            del trigrams_temp 
            del df2
     
    del df1                

print('Writing csv....')
bigrams_df=bigrams_df.fillna(0)
bigrams_df.to_csv('Bigram_Counts_congress_2014_2021_v1.csv')
print('Export Successful!')

#%%Pull Bigrams,  Media

conn=sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\corrected_bigram_data.db')

excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Replication\Excluded_Bigrams_media_v2.csv")
excluded_bigrams=list(excluded_bigrams['Phrase'])

proc_terms=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\proc_terms_media.csv")
proc_terms=list(proc_terms['Term'])

dates=pd.read_sql_query('''SELECT DISTINCT Date
                                 FROM corrected_bigrams;
                                 ''',conn)

excluded_speakers=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_speakers.csv")
excluded_speakers=list(excluded_speakers['Name'])


dates=list(dates['Date'])
dates=[its[0:7] for its in dates]
dates=list(set(dates))
dates.sort()
st='2014-09'
dates=dates[dates.index(st):]

phrases=pd.read_sql_query('''SELECT Phrase, COUNT(*)
                             FROM corrected_bigrams
                             GROUP BY Phrase
                             HAVING COUNT(*)>19''', conn)



ct=42000
phrases=phrases.sort_values('COUNT(*)',ascending=False)
phrases = phrases[~phrases['Phrase'].isin(excluded_bigrams)]
phrases=list(phrases['Phrase'][:ct])

for i in range(len(proc_terms)):
    phrases=[ x for x in phrases if proc_terms[i] not in x ]

phrases = list(filter(lambda phrase: phrase not in excluded_bigrams, phrases))

parties=['"Fox"','"CNN"']
bigrams_df=pd.DataFrame()

#Rearrange


for k in range(2):

    Network=parties[k]
    print("Pivoting....:"+Network)
    df1= pd.read_sql_query('''SELECT*FROM corrected_bigrams
                                  WHERE Network='''+parties[k]+''';''',conn)    
    
    #df1['Date']=[its[0:7] for its in df1['Date']]
    df1=df1[~df1['Phrase'].isin(excluded_bigrams)]
    df1=df1[~df1['Speaker'].isin(excluded_speakers)]
    df1['Date']=[its[0:7] for its in df1['Date']]
    


    

    for j in range(len(dates)):
            trigrams_temp=pd.DataFrame()    
        
            if j==0: print("Building Bigram Counts. Progress:")
            x=str(((j/(len(dates))*100)))
            print(x[:4])
            df2=df1[df1['Date']==dates[j]]
            
           
            speakers=list(set(df2['Speaker']))    
            speakers = list(filter(lambda speaker: speaker not in excluded_speakers, speakers))
            speakers.sort()
           
            for i in range(len(speakers)):
        
                speaker="'"+speakers[i]+"'"
            
                df3=df2[df2['Speaker'] == speakers[i]]
                if df3.empty:
                    continue
                else:
                    print('Not Empty: ' + speaker +" "+str(i))
                    
                
                phrase_count=df3['Phrase'][df3['Phrase'].isin(phrases)].value_counts(ascending=True)
                phrase_count=pd.DataFrame(phrase_count)
                phrase_count=pd.DataFrame.transpose(phrase_count)
                phrase_count=phrase_count.astype('int16')
                phrase_count.insert(0, "Speaker", ' '.join(speakers[i].split(' ')[0:2]), True)
                phrase_count.insert(1, "Date", dates[j], True)
                phrase_count.insert(2, "Network", Network , True)
                trigrams_temp=pd.concat([trigrams_temp,phrase_count])
            
            bigrams_df=pd.concat([bigrams_df,trigrams_temp])  
            del trigrams_temp 
            del df2
     
    del df1                

print('Writing csv....')
bigrams_df=bigrams_df.fillna(0)
bigrams_df.to_csv('Bigram_Counts_2014_2021_v1.csv')
iprint('Export Successful!')
