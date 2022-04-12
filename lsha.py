#!/usr/bin/env python
# coding: utf-8

# In[161]:


# Produce a Python script to populate an aggregated table with the following structure:

# userId int
# registrationDate datetime
# registrartionUtmSource varchar (100)
# number_of_utm_touches int
# more_then_2_utm_touches_ind (int) (the indicator should be 1 if there were more then 2 utm touches before purchase, 
#     and 0 in any other case)


import pandas as pd
import numpy as np


#step 1: load csv to dfs

df_users_utm = pd.read_csv("C:/Users/ilay.elazar/Desktop/python/lsha/users_utm.csv")
df_users = pd.read_csv("C:/Users/ilay.elazar/Desktop/python/lsha/users.csv")
df_purchases = pd.read_csv("C:/Users/ilay.elazar/Desktop/python/lsha/purchases.csv")


# In[162]:


#step 2: convert date strings to dt.datetime

df_users_utm['utmDate'] = pd.to_datetime(df_users_utm['utmDate'])  #turn to datetime
df_users['registrationDate'] = pd.to_datetime(df_users['registrationDate'])  #turn to datetime


# In[163]:


#step 3: join between users_utm <-> users    -> agg min(regDate), count(utmSource)

df_user_utm_regs = df_users_utm.merge(df_users, left_on='userId', right_on='userId').groupby('userId').agg(
    {'registrationDate': 'min',
    'utmSource':'count'
    }                                         
 ).reset_index()


# In[ ]:


#step 4: inner join to get the utmSource of the registrated UTM

res_df = df_user_utm_regs.merge(df_users_utm, left_on='registrationDate', right_on='utmDate')

#flag -   0: number_of_utm_touches <= 2 ,  1:number_of_utm_touches > 2   (3 touches means 2 touches before purchase)

res_df['more_then_2_utm_touches_ind'] = np.where(res_df['utmSource_x'] > 2 ,1,0)
res_df


# In[166]:


res_df = res_df.drop(['userId_y','utmDate'],axis = 1)
res_df.columns = ['userId','registrationDate','number_of_utm_touches','registrartionUtmSource','more_then_2_utm_touches_ind']

res_df

