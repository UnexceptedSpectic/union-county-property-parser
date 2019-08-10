#! /bin/bash/python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('output.csv')
df['Land Price/Acre'] = pd.to_numeric(df['Land Price/Acre'], errors='coerce')
df = df[(df['Land Price'] > 0) & (df['Acreage'] > 0) & (df['Building Value'] == 0) & (df['Land Price/Acre'] < 10000)]
df = df.sort_values(by='Land Price/Acre', ascending=True)
df = df.groupby(['Owner'])['PIN'].apply(lambda x: ','.join(set(x))).reset_index()

plt.plot(df['Acreage'][:100], df['Land Price'][:100])
plt.xlabel('Acreage')
plt.ylabel('Land Price')
plt.show()

topHundredByAcreage = df[:100].sort_values(by='Acreage', ascending=False)

plt.plot(df['Land Price/Acre'])
plt.show()
