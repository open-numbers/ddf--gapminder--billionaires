"""
A script to download profile photos from Forbes.
"""

import pandas as pd
# import numpy as np
import lxml.html
import requests as req
import multiprocessing
import time
import pickle

profile_tmpl = 'https://www.forbes.com/profile/{}/'
all_forbes = pd.read_csv('../../forbes/ddf--entities--person.csv')


def get_text(person):
    p = person.replace('_', '-')
    url = profile_tmpl.format(p)
    res = req.get(url)
    time.sleep(2)
    if not res.ok:
        print(f'{person}: http problem: {res.status_code}')
        return (person, None)
    root = lxml.html.fromstring(res.content)
    e = root.xpath("//div[@class='profile-text']")
    if len(e) != 1:
        print(f'{person}: html problem')
        return (person, None)
    text = '\n'.join(e[0].itertext())
    return (person, text)


if __name__ == '__main__':
    poolsize = 8
    with multiprocessing.Pool(poolsize) as pool:
        res = pool.map(get_text, all_forbes.person.values)

    with open('forbes_list.pickle', 'wb') as handle:
        pickle.dump(res, handle, protocol=pickle.HIGHEST_PROTOCOL)
