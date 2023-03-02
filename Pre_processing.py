#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import re
with open('data/RC_2010-10') as f:
    data = pd.read_json(f, lines=True)
    
data = data.drop(data[data['body'] == '[deleted]'].index)

for index, row in data.iterrows():
    row['body'] = re.sub(r'[^\w\s]', '', row['body'])
# general pre processing


# In[ ]:


# If youo edit this file, use: jupyter nbconvert --to script Pre_processing.ipynb 
# to conver it to Pre_processing.py and then use from Pre_processing import data to use data

