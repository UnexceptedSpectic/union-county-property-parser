#! /usr/bin/python3

# script to scRAPE union county property data

import pandas as pd
import requests, re, time
from bs4 import BeautifulSoup as bs
import threading
from datetime import datetime


rdf = pd.read_csv('property_info.csv')
df = pd.DataFrame()
df['PIN'] = rdf['PIN'].str.strip()
df['Owner'] = rdf['OWNER1'].str.strip()
df['Address'] = rdf['LocAddr'].str.strip()
df['Township'] = ""
df['Property Class'] = ""
df['Acreage'] = ""
df['Land Value'] = ""
df['Building Value'] = ""

df_length = len(df)

def populate_dataframe(start, stop, num_blocks):
    global requests_complete
    for pin in range(start, stop):
        while True:
            try: # request property data and retry until success upon failure
                page = requests.get('http://gis-web.co.union.nc.us/tax/property.aspx?PIN=%s' %df['PIN'][pin])
                contents = page.content
                soup = bs(contents, 'html.parser')
                df['Township'][pin] = soup.find_all('b')[3].next_element.next_element.next_element.next_element.strip()
                df['Property Class'][pin] = soup.find_all('b')[1].next_element.next_element.next_element.next_element.strip()
                df['Acreage'][pin] = "".join(re.findall(r'\d.*\d', soup.find_all('b')[5].next_element.next_element.next_element.next_element)).strip()
                df['Land Value'][pin] = "".join(re.findall(r'[0-9]', soup.find_all('b')[10].next_element.next_element.next_element.next_element))
                df['Building Value'][pin] = "".join(re.findall(r'[0-9]', soup.find_all('b')[11].next_element.next_element.next_element.next_element))
                requests_complete += 1
                print("%s Property %s of %s added" %(datetime.now(), pin+1, df_length))
                if requests_complete % 10 == 0:
                    print("Requests completed: %s" %requests_complete)
            except:
                print("Error: unable to request html for property %s, retrying..." %(pin+1))
                time.sleep(1)
                continue
            break
    
# find factors of dataframe length value
#factors = []    
#for n in range(1, len(df) + 1):
#    if len(df) % n == 0:
#        factors.append(n)

# test script using a block size of x
factors = [353]

# parse site using increasing number of threads
for block_size in factors: 
    start_time = time.time()
    requests_complete = 0
    num_blocks = int(len(df)/block_size)

    # define block indices, where a blocks are segments of the dataframe to be assigned to each thread
    block_indices = []
    for block_ind in range(0, len(df) + 1, block_size):
        block_indices.append(block_ind)
    
    # define threads
    threads = []
    threads_started = 0
    for index in range(0, len(block_indices)-1): # for each block, start a new thread running the populate_dataframe function
        start = block_indices[index]
        stop = block_indices[index + 1]
        threads.append(threading.Thread(target = populate_dataframe, args = (start, stop, num_blocks)))

    # start threads and ensure they finish before the script exits
    for thread in threads:
        thread.start()
        threads_started += 1
        print("%s of %s threads created" %(threads_started, num_blocks))
    for thread in threads:
        thread.join()

# ensure that all rows of df have been populated and save df to csv       
if requests_complete != df_length:
    print('\n%s of %s http requests were completed. Runtime using %s threads was %s seconds \n' %(requests_complete, df_length, num_blocks, round(time.time() - start_time, 2)))
else:
    print("\nAll http requests were completed. Runtime using %s threads was %s seconds \n" %(num_blocks, round(time.time() - start_time, 2)))

print('Saving data to csv. Please wait...')
df.to_csv("./output.csv")
print("Data saved to ./output.csv")