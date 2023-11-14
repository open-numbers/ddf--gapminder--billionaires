# %%
import pandas as pd
# %%
hist = pd.read_csv('../../ddf--datapoints--annual_income--by--person--time.csv')
# %%
hist
# %%
# get top 10 for each year
gs = hist.groupby('time')
# %%
tops = list()

for g, df in gs:
    df_ = df.sort_values(by='annual_income', ascending=False)
    top = df_.iloc[:10]['person'].values.tolist()
    tops = tops + top
# %%
set(tops)
# %%
from collections import Counter
# %%
Counter(tops)
# %%
c = Counter(tops)
# %%
c.keys()
# %%
import os
# %%
existing = list()

for f in os.listdir('../../assets'):
    name = f[:-4]
    existing.append(name)
# %%
for n in c.keys():
    if n not in existing:
        print(n)
# %%
existing
# %%
c.keys()[0]
# %%
# top for each regions
person = pd.read_csv('../../ddf--entities--person.csv')
# %%
person
# %%
person.columns
# %%
person['country'] = person['countries'].map(lambda x: x.split(';')[0])
# %%
country = pd.read_csv('../../ddf--entities--geo--country.csv')
# %%
country
# %%
country_map = country.set_index('country')['world_4region'].to_dict()
# %%
country_map
# %%
person['region'] = person['country'].map(lambda x: country_map.get(x))
# %%
person_map = person.set_index('person')['region'].to_dict()
# %%
person_map
# %%
hist 
# %%
hist['region'] = hist['person'].map(lambda x: person_map[x])
# %%
person_map.get('aaa')
# %%
hist['region'].hasnans
# %%
gs = hist.groupby(['region', 'time'])

tops = list()

for g, df in gs:
    df_ = df.sort_values(by='annual_income', ascending=False)
    top = df_.iloc[:10]['person'].values.tolist()
    tops = tops + top
# %%
c = Counter(tops)
# %%
c
# %%
existing = list()

for f in os.listdir('../../assets'):
    name = f[:-4]
    existing.append(name)

# %%
for n in c.keys():
    if n not in existing:
        print(n)
# %%
# just for this year?
hist2021 = hist[hist['time'] == 2021]
# %%
gs = hist2021.groupby(['region', 'time'])

tops = list()

for g, df in gs:
    df_ = df.sort_values(by='annual_income', ascending=False)
    top = df_.iloc[:10]['person'].values.tolist()
    tops = tops + top

c = Counter(tops)
for n in c.keys():
    if n not in existing:
        print(n)
# %%
