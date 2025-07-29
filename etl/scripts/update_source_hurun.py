# -*- coding: utf-8 -*-

import sys
import pandas as pd
import requests as req
import copy


url_tmpl = 'https://www.hurun.net/en-US/Rank/HsRankDetailsList?num={}&search=&offset={}&limit=200'
urlnum = {
    2025: 'ND77BFWM',
    2024: 'A3I4FTLA',
    2023: 'GJD3W34B',
    2022: 'GJD3WF1Q',
    # 2021: 'IH8GTUI9',
    2021: '1G85FXHD',
    2020: 'PYSXN53E',
    2019: 'Q9TGQF1L'
}


def create_record(r):
    row = copy.deepcopy(r)
    record = pd.DataFrame.from_records(row.pop('hs_Character'))
    for k, v in row.items():
        record[k] = v
    return record


def download(y):
    jsons = []
    offset = 0
    while True:
        print(offset)
        url = url_tmpl.format(urlnum[y], offset)
        res = req.get(url).json()
        jsons.append(res)
        offset = offset + 200
        if offset >= res['total']:
            break
    return jsons


def download_and_serve(y):
    jsons = download(y)
    recs = []
    for j in jsons:
        for r in j['rows']:
            recs.append(create_record(r))
    res = pd.concat(recs, ignore_index=True)
    res['year'] = y
    res.to_csv(f'../source/hurun/{y}.csv', index=False)
    return res


def main():
    args = sys.argv
    if len(args) != 2:
        print("please specify a year to continue download")
        sys.exit(127)
    year = int(args[1])
    print(f"downloading forbes {year}")
    download_and_serve(year)


if __name__ == "__main__":
    main()
