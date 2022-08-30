# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 15:16:35 2021

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

#%% Create SQLite tables

#Connect to SQLite database
conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\congress_db.db')

sql_create_bigrams_table = """ CREATE TABLE IF NOT EXISTS scrape_bigrams_congress_v1 (
                                        Speaker text,
                                        Date text ,
                                        Party text,
                                        Phrase text
                                    ); """

c = conn.cursor()
c.execute(sql_create_bigrams_table)

sql_create_words_table = """ CREATE TABLE IF NOT EXISTS scrape_words_congress_v1 (
                                        Speaker text,
                                        Date text ,
                                        Party text,
                                        Word text
                                    ); """

c = conn.cursor()
c.execute(sql_create_words_table)

sql_create_trigrams_table = """ CREATE TABLE IF NOT EXISTS scrape_trigrams_congress_v1 (
                                        Speaker text,
                                        Date text ,
                                        Party text,
                                        Phrase text
                                    ); """

c = conn.cursor()
c.execute(sql_create_trigrams_table)




#%%
stops=set(stopwords.words('english'))

excluded_words=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Words.csv")
excluded_words=list(excluded_words['Excluded_words'])

excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Bigrams.csv")
excluded_bigrams=excluded_bigrams.values.tolist()

excluded_trigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Trigrams_congress.csv")
excluded_trigrams=excluded_trigrams.values.tolist()

conn = sqlite3.connect(r'C:\Users\mahon\Documents\Python Scripts\congress_db.db')

mypath=r'C:\Users\mahon\PDFs\Congress\Jan Corrections'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]


leg_data=pd.read_csv(r"C:\Users\mahon\PDFs\leg_list.csv")


sen_list=leg_data[leg_data['type']=='sen']
sen_list=list(sen_list['last_name'])
sen_list=[item.split(',')[0] for item in sen_list]
sen_list=[item.upper() for item in sen_list]
lower_sen_list=[item.capitalize() for item in sen_list]
sen_list.extend(lower_sen_list)

sen_party_list=leg_data[leg_data['type']=='sen']
sen_party_list=list(sen_party_list['party'])

rep_list=leg_data[leg_data['type']=='rep']
rep_list=list(rep_list['last_name'])
rep_list=[item.split(',')[0] for item in rep_list]
rep_list=[item.upper() for item in rep_list]
lower_rep_list=[item.capitalize() for item in rep_list]
rep_list.extend(lower_rep_list)

rep_party_list=leg_data[leg_data['type']=='rep']
rep_party_list=list(rep_party_list['party'])


sen_dict = {sen_list[i]: sen_party_list[i] for i in range(len(sen_party_list))}
rep_dict = {rep_list[i]: rep_party_list[i] for i in range(len(rep_party_list))}
leg_dict = {**sen_dict, **rep_dict}
leg_dict['LORETTA']='Democrat'

text=''
 
words_df_temp=pd.DataFrame()
bigrams_df_temp=pd.DataFrame()
trigrams_df_temp=pd.DataFrame()

days=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

months=['January' , 'February','March' ,'April' ,'May','June','July','August','September' ,'October','November','December']

num_month=[1,2,3,4,5,6,7,8,9,10,11,12]

month_dict={months[i]: num_month[i] for i in range(len(months))}

onlyfiles=[x for x in onlyfiles if 'daily' not in x]
onlyfiles=[x for x in onlyfiles if '(1)' not in x]

n_docs=len(onlyfiles)


words_df=pd.DataFrame()
bigrams_df=pd.DataFrame()
trigrams_df=pd.DataFrame()

trigrams_df_temp=pd.DataFrame()
bigrams_df_temp=pd.DataFrame()
words_df_temp=pd.DataFrame()

leg_list=sen_list+rep_list


#%%

for k in range(n_docs):
    print("No way this works......")
    print(str(k))
    trigrams_df_temp=pd.DataFrame()
    bigrams_df_temp=pd.DataFrame()
    words_df_temp=pd.DataFrame()
    
    
    if onlyfiles[k][0:4]=='CHRG':
        text=''
        
        pdfFileObject = open(onlyfiles[k], 'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObject)
        count = pdfReader.numPages
        for i in range(count):
            page = pdfReader.getPage(i)
            text += page.extractText()
        
        pdfFileObject.close()
        chunk1=''
        chunk2=''
        edited_text=''
        
        
        dt_index=[text.find(day) for day in days]
        
        dt_position = min(i for i in dt_index if i > 0)
        
        day=(days[dt_index.index(dt_position)])
        day=day.capitalize()
        
        dt=text[dt_position+len(day)+2:dt_position+len(day)+20]
        dt=dt.capitalize()
        month=month_dict[dt.split(' ')[0]]
        dt=dt.split(' ')[2]+"-"+str(month)
        dt=dt.replace(',', '')
            
        bookends=['Chairman','Chairwoman','Senator','Representative','Mr.','Mrs.','Ms.','Dr.']
        
        #text=text[text.find('OPENING STATEMENT'):]
        text=text.replace('\n', '')
        text=text.replace('™','')
        text=re.sub(r'[0-9]+', '', text)
        
        text=re.sub("([\(\[]).*?([\)\]])", "", text)
        
        index=[]
        starts=[]
        
        for i in range(len(bookends)):
            index += [m.start() for m in re.finditer(bookends[i], text)]
        
        index.sort()
        
        words=['']
        spk_save='aaaa'
        for j in range(len(index)-1):
                    
            chunk1=text[index[j]:index[j+1]]
            try:
                title=chunk1.split()[0]
            except:
                title='aaaa'
            try:
                spk=chunk1.split(title)[1].split()[0]
                spk=spk.replace('.', '')
            except: 
                spk='nada'
                
            if spk.isupper() and spk in sen_list:
                include=True
                updat=True
            elif not spk.isupper() and spk_save in sen_list:
                include=True
                updat=False    
            elif spk.isupper() and spk not in sen_list:
                include=False
                updat=True
            elif not spk.isupper() and spk_save not in sen_list:
                include=False
                updat=False
            if updat:
                spk_save=spk
                
            if not include:
                print(include)
                print(spk_save)
                
            
            if include:
    
            #Text analysis shit
                tokens=[]        
                tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(chunk1)
                
                tokens = list(filter(lambda token: token not in string.punctuation, tokens))
                
                tokens = list(filter(lambda token: token not in ['™','())','(()','()(',')()','..,',');',',™™','.™™','™,','Ms','Mr','Speaker',',.'], tokens ))
                tokens = list(filter(lambda token: token not in bookends, tokens))
                tokens = list(filter(lambda token: token not in sen_list, tokens))
                tokens = list(filter(lambda token: token not in excluded_words,tokens))
                tokens = list(filter(lambda token: token not in stops,tokens))
            
                bigram = list(ngrams(tokens, 2)) 
                for i in range(len(bigram)):
                    bigram[i]=str(bigram[i])
            
                bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
            
                trigram=list(ngrams(tokens,3))
                for i in range(len(trigram)):
                    trigram[i]=str(trigram[i])
            
                trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))
                
        
                
                print(spk_save)
                
                size_token=len(tokens)
                size_bigram=len(bigram)
                size_trigram=len(trigram)
                
            #Generate Words DF
            
                
                lst2=[spk_save]*size_token
                lst3=[dt]*size_token
                lst4=[sen_dict[spk_save]]*size_token
                lst5=[onlyfiles[k]]*size_token
            
                words_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Word': tokens})
                words_df_temp=pd.concat([words_df_builder,words_df_temp])
        
        #Generate Bigrams DF
                lst2=[spk_save]*size_bigram
                lst3=[dt]*size_bigram
                lst4=[sen_dict[spk_save]]*size_bigram
            
                bigrams_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Phrase': bigram})
                bigrams_df_temp=pd.concat([bigrams_df_builder,bigrams_df_temp])
        #Generate Trigrams DF
        
                lst2=[spk_save]*size_trigram
                lst3=[dt]*size_trigram
                lst4=[sen_dict[spk_save]]*size_trigram
            
                trigrams_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Phrase': trigram})
                trigrams_df_temp=pd.concat([trigrams_df_temp,trigrams_df_builder])
            
    #words_df=pd.concat([words_df,words_df_temp])         
    #bigrams_df=pd.concat([bigrams_df,bigrams_df_temp])
    #trigrams_df=([trigrams_df,trigrams_df_temp])
   
        #Write to SQL database
        bigrams_df_temp.to_sql('scrape_bigrams_congress', conn, if_exists='append', index=False)
        words_df_temp.to_sql('scrape_words_congress', conn, if_exists='append', index=False)
        trigrams_df_temp.to_sql('scrape_trigrams__congress', conn, if_exists='append', index=False)

    if onlyfiles[k][0:4]=='CREC':
       # try:
            text=''
            
            pdfFileObject = open(onlyfiles[k], 'rb')
            pdfReader = PyPDF2.PdfFileReader(pdfFileObject)
            count = pdfReader.numPages
            if count<10: continue
            for i in range(count):
                page = pdfReader.getPage(i)
                text += page.extractText()
            text=text.replace('\n', '')
            text=text.replace('-','')
            text=text[text.find('PROCEEDINGS'):]
            text=text[0:text.find('Daily Digest')]
            text=text[0:text.find('EXTENSIONS OF REMARKS')]
            
            rec_index=max(text.find('The Chair recognizes'),0)
    
            adj_index=[m.start() for m in re.finditer('ADJOURNMENT', text)]
            pledge_index=[m.start() for m in re.finditer('liberty and justice for all', text)]
            
            try:
                text=text[min(rec_index,pledge_index[0]):adj_index[0]]+text[pledge_index[1]:adj_index[1]]
            except:
                try:
                    text=text[min(rec_index,pledge_index[0]):adj_index[0]]
                except:
                    print('Doc')
                    continue
                    
            text=text.replace('GUTIE´RREZ','GUTIERREZ')
            text=text.replace('Š','')
            text=re.sub("[\(\[].*?[\)\]]", "", text)
            
            pdfFileObject.close()
            chunk1=''
            chunk2=''
            edited_text=''
            
            #Extract Month and Year
    
            dt=onlyfiles[k][5:12]
            
            
            bookends=['Chairman','Chairwoman','Senator','Representative','Mr.','Mrs.','Ms.','Dr.','The SPEAKER']
    
            
            text=re.sub(r'[0-9]+', '', text)
            
            index=[]
            starts=[]
            #Constuct list of index values
            for i in range(len(bookends)):
                index += [m.start() for m in re.finditer(bookends[i], text)]
            
            index.sort()
            
            words=['']
            spk_save='aaaaa'
            
            for j in range(len(index)-1):
                chunk1=text[index[j]:index[j+1]]
                
                if len(chunk1)==0:
                    continue
                
                try:
                    title=chunk1.split()[0]
                except:
                    title='none'
                try:
                    spk=chunk1.split(title)[1].split()[0]
                    spk=spk.replace('.', '')
                    spk=spk.replace(')', '')
                    spk=spk.replace(',', '')
                    spk=spk.replace(':', '')
                except: 
                    spk='nada'
               
                include=False
                updat=False
                  
                if spk.isupper() and spk in leg_list:
                    include=True
                    updat=True
                elif not spk.isupper() and spk_save in leg_list:
                    include=True
                    updat=False
                #elif spk.isupper() and spk not in leg_list:
                 #   include=False
                  #  updat=True
                elif not spk.isupper() and spk_save not in leg_list:
                    include=False
                    updat=False
                
                if spk=='I' and spk_save in leg_list:
                    include=True
                    updat=False
                
                spk_index=chunk1.find(spk)
                spk_len=len(spk)
               
                try:
                    if chunk1[spk_index+spk_len]==',':
                        updat=False
                except:
                    updat=True
                
                if updat:
                    spk_save=spk
                    print(spk_save)
                
                
                print(include)
                
    
                
                if include and spk_save in leg_list:
                    chunk1 = re.sub(r'\b[A-Z]+\b', '', chunk1)
                    tokens=[]        
                    tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(chunk1)
                    
                    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
                    tokens = list(filter(lambda token: token not in ['™','())','(()','()(',')()','..,',');',',™™','.™™','™,','Ms','Mr','Speaker',',.','Œ'], tokens ))
                    tokens = list(filter(lambda token: token not in bookends, tokens))
                    tokens = list(filter(lambda token: token not in leg_list, tokens))
                    tokens = list(filter(lambda token: token not in stops, tokens))
                    tokens = list(filter(lambda token: token not in excluded_words, tokens))
                
                
                    bigram = list(ngrams(tokens, 2)) 
                    for i in range(len(bigram)):
                        bigram[i]=str(bigram[i])
                
                    bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))
                
                    trigram=list(ngrams(tokens,3))
                    for i in range(len(trigram)):
                        trigram[i]=str(trigram[i])
                
                    trigram=list(filter(lambda w: not w in excluded_trigrams,trigram))
                    
            
                    
                    print(spk_save)
                    
                    size_token=len(tokens)
                    size_bigram=len(bigram)
                    size_trigram=len(trigram)
                    
                #Generate Words DF
                
                    
                    lst2=[spk_save]*size_token
                    lst3=[dt]*size_token
                    lst4=[leg_dict[spk_save]]*size_token
                    lst5=[onlyfiles[k]]*size_token
                
                    words_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Word': tokens})
                   
                    words_df_temp=pd.concat([words_df_temp,words_df_builder])
            
            #Generate Bigrams DF
                    lst2=[spk_save]*size_bigram
                    lst3=[dt]*size_bigram
                    lst4=[leg_dict[spk_save]]*size_bigram
                
                    bigrams_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Phrase': bigram})
                    bigrams_df_temp=pd.concat([bigrams_df_temp,bigrams_df_builder])
            #Generate Trigrams DF
            
                    lst2=[spk_save]*size_trigram
                    lst3=[dt]*size_trigram
                    lst4=[leg_dict[spk_save]]*size_trigram
      
                        
                    trigrams_df_builder=pd.DataFrame({'Speaker': lst2,'Date':lst3,'Party':lst4,'Phrase': trigram})
                    trigrams_df_temp=pd.concat([trigrams_df_builder,trigrams_df_temp])
            
            bigrams_df_temp.to_sql('scrape_bigrams_congress_v1', conn, if_exists='append', index=False)
            words_df_temp.to_sql('scrape_words_congress_v1', conn, if_exists='append', index=False)
            trigrams_df_temp.to_sql('scrape_trigrams_congress_v1', conn, if_exists='append', index=False)
        
        #except:
         #   print("ERROR AT DOCUMENT "+str(k)+": "+str(onlyfiles[k]) )
          #  continue
        
    





















