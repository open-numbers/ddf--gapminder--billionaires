# -*- coding: utf-8 -*-

import requests as req
import pandas as pd
import sys


url_tmpl = "https://www.forbes.com/ajax/list/data?year={}&uri=billionaires&type=person"


def get_data(year):
    res = req.get(url_tmpl.format(year)).json()
    df = pd.DataFrame.from_records(res)
    return df


def main():
    args = sys.argv
    if len(args) != 2:
        print("please specify a year to continue download")
        sys.exit(127)
    if args[1] == 'all':
        for i in range(2002, 2024):
            print(f"downloading forbes {i}")
            df = get_data(i)
            df.to_csv(f'../source/forbes/{i}.csv', index=False)
    else:
        year = int(args[1])
        print(f"downloading forbes {year}")
        df = get_data(year)
        print(df.head())
        df.to_csv(f'../source/forbes/{year}.csv', index=False)


if __name__ == "__main__":
    main()
