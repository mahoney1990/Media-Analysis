# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 15:25:17 2021

@author: mahon
"""
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import ElementClickInterceptedException

#%%
#Extract Fox News URLS

data=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\FOX_inputs.csv")
urls=data['URLS']
N=len(urls)

url='https://www.foxnews.com/person/personalities'
driver = webdriver.Chrome("C:/Users/mahon/Documents/Python Scripts/chromedriver.exe")
driver.get(url)
html = driver.page_source.encode('utf-8')
html = html.decode("utf-8")

links=re.findall('<a href="(.+?)">',html)

del links[0:64]
del links[0]
del links[173:195]
fox_host_links=links
del fox_host_links[173]



#%% Scrape Personalitites
N=len(fox_host_links)
fox_host_links_df=pd.DataFrame()
for i in range(N):
    url=fox_host_links[i]
    name=url.split("/person/",1)[1]
    print(name)
    
    driver.get(url)
    time.sleep(3)
    
    while True:
        time.sleep(1)
        try:
            element = WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="wrapper"]/div[2]/div[2]/div/main/section/footer/div/a')))
            element.click()
        except (TimeoutException,StaleElementReferenceException,ElementClickInterceptedException):
                break
    
    time.sleep(1)
    html = driver.page_source.encode('utf-8')
    html = html.decode("utf-8")
    links=re.findall('article class="article"><div class="m"><a href="(.+?)">',html)
    
    dates = re.findall('="time">(.+?)<', html)

    
    for k in range(len(links)):
        links[k]="https://foxnews.com"+links[k]
    
    links_df=pd.DataFrame({'URL': links,'Host':name,'Date':dates})
    
    fox_host_links_df = pd.concat([fox_host_links_df,links_df])
    
fox_host_links_df.to_csv('FOX_HOST_URLS_List.csv',index=False) 
    
    


#%% Extract Words
stops=set(stopwords.words('english'))
host_names=data['Host'].tolist()
host_names = list(set(data['HOST_CODE']))


data=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\FOX_URLS_List.csv")
excluded_bigrams=pd.read_csv(r"C:\Users\mahon\Documents\Python Scripts\Excluded_Bigrams.csv")
excluded_bigrams=excluded_bigrams.values.tolist()

for i in range(len(excluded_bigrams)):
    excluded_bigrams[i] = excluded_bigrams[i][0]


urls=list(data['URL'])
N=len(urls)

import random
size=4000
#randy=[random.randint(0, N) for iter in range(size)]
#rand_list=randy
rand_list=random.sample(range(N), size)

texts = ["" for i in range(N)]

fox_words_df=pd.DataFrame()
fox_bigrams_df=pd.DataFrame()
j=0

import string
printable = set(string.printable)


for j in rand_list:
    print(j)
    page = urlopen(urls[j])

    html_bytes = page.read()
    html = html_bytes.decode("utf-8")
    
    dt=re.findall('datePublished": "(.+?)T',html)
      
    soup = BeautifulSoup(html, "html.parser")
    texts[j]=soup.get_text()
    texts[j].replace('\n','')
    if data['Host'][j]!="Bill O'Reilly":
        texts[j]=texts[j].split('By')[1]
        texts[j]=texts[j].split('CoronavirusU.S.CrimeMilitary')[0]
    
    tokens=nltk.tokenize.RegexpTokenizer("[\\w']+|[^\\w\\s]+").tokenize(texts[j])
    tokens = list(filter(lambda token: token not in string.punctuation, tokens))
    filter(lambda x: x in printable, tokens)
    tokens = [sub.replace('+', '') for sub in tokens] 
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
    tokens = [sub.replace('."', '') for sub in tokens]
    tokens = [sub.replace('"', '') for sub in tokens]
    tokens = [sub.replace('Fox', '') for sub in tokens]
    tokens = [sub.replace('com', '') for sub in tokens] 
    tokens=list(filter(lambda a: a != '', tokens))
    tokens=list(filter(lambda w: not w in stops,tokens))
    
    
    bigram = list(ngrams(tokens, 2)) 
    for i in range(len(bigram)):
        bigram[i]=str(bigram[i])

    bigram=list(filter(lambda w: not w in excluded_bigrams,bigram))

    size_token=len(tokens)
    size_bigram=len(bigram)

    
    ID=data['DOCID'][j]
    lst1=[ID] * size_token
    
    Net=data['Network'][j]
    lst2=[Net]*size_token
    
    lst3=[dt]*size_token
    
    
    words_df_temp=pd.DataFrame({'DOCID': lst1,'Network': lst2,'Word': tokens,'Date':lst3})
    
    ID=data['DOCID'][j]
    lst1=[ID] * size_bigram
    
    Net=data['Network'][j]
    lst2=[Net]*size_bigram
    
    lst3=[dt]*size_bigram
    
    bigrams_df_temp=pd.DataFrame({'DOCID': lst1,'Network': lst2,'Phrase': bigram,'Date':lst3})
    
    fox_words_df = pd.concat([fox_words_df,words_df_temp])
    fox_bigrams_df = pd.concat([fox_bigrams_df,bigrams_df_temp])


fox_master=pd.DataFrame()

merged_df  = fox_bigrams_df.merge(data, how = 'inner', on = ['DOCID'])
import seaborn as sns
counts=merged_df['Phrase'].value_counts(ascending=True)
counts=pd.DataFrame(counts)

phrase_list = counts[counts['Phrase'] > 10]
phrases=phrase_list.index.values.tolist()

for i in range(len(host_names)):
    df1= merged_df[merged_df['HOST_CODE']==host_names[i]]
    phrase_count=df1['Phrase'][df1['Phrase'].isin(phrases)].value_counts(ascending=True)
    phrase_count=pd.DataFrame(phrase_count)

    phrase_count=pd.DataFrame.transpose(phrase_count)
    phrase_count.insert(0, "HOST_CODE", [host_names[i]], True)

    fox_master=pd.concat([fox_master,phrase_count])

fox_master=fox_master.fillna(0)

fox_master.to_csv('Bigram_counts.csv')






