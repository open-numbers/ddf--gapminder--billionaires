# -*- coding: utf-8 -*-

import os
import numpy as np
import pandas as pd
from ddf_utils.str import to_concept_id


def cleanup(s):
    if isinstance(s, str):
        s_new = s.strip()
        if s_new.endswith('& family'):
            s_new = s_new[:-9]
        if "&#38;" in s_new:
            s_new = s_new.replace("&#38;", '')
        return to_concept_id(s_new)
    else:
        return s


def is_broken_year_range(frame):
    years = frame['year']
    r1 = years.max() - years.min() + 1
    r2 = len(years)
    return not (r1 == r2)


def combine_values(frame):
    values = dict()
    for c in frame.columns:
        values[c] = '; '.join(frame[c].dropna().astype(str).unique())
    return pd.Series(values)


def get_last_value(frame):
    values = dict()
    for c in frame.columns:
        d = frame[c].dropna()
        if d.empty:
            values[c] = np.nan
        else:
            values[c] = d.iloc[-1]
    return pd.Series(values)


def check_groups(frame):
    n = frame.index[0]
    if is_broken_year_range(frame):
        print(n)
    return frame


def get_data_file(year, subdir=None):
    if subdir:
        fn = os.path.join('../source', subdir, f'{year}.csv')
    else:
        fn = os.path.join('../source', f'{year}.csv')
    return pd.read_csv(fn)
