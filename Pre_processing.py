#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

with open('data/RC_2006-01') as f:
    data = pd.read_json(f, lines=True)

# general pre processing


# In[2]:


# If youo edit this file, use: jupyter nbconvert --to script Pre_processing.ipynb 
# to conver it to Pre_processing.py and then use from Pre_processing import data to use data

