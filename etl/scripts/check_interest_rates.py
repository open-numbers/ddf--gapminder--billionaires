# -*- coding: utf-8 -*-

import numpy as np
import polars as pl

worth_file = '../../ddf--datapoints--worth--by--person--time.csv'

worth = pl.read_csv(worth_file)

worth = worth.sort(['person', 'time'])

worth.with_columns(
    pl.col('worth').pct_change().over('person')
).drop_nulls().filter(
    (pl.col('person') == 'alisher_usmanov') & (pl.col('time').is_in([2009, 2008]))
)

worth.filter(
    (pl.col('person') == 'ziyad_manasir') & (pl.col('time').is_in([2013, 2014, 2015, 2016, 2017]))
)
# NOTE: double check with ola about bill gates and jeff bezos

worth.with_columns(
    pl.col('worth').pct_change().over('person')
).drop_nulls().select(
    pl.col('worth').quantile(1)
)

changes = worth.with_columns(
    pl.col('worth').pct_change().over('person').alias('change') * 100
).drop_nulls()

changes.select(
    pl.col('worth').mean().alias('mean'),
    pl.col('worth').min().alias('min'),
    pl.col('worth').quantile(.25).alias('25%'),
    pl.col('worth').quantile(.5).alias('50%'),
    pl.col('worth').quantile(.75).alias('75%'),
    pl.col('worth').max().alias('max')
).to_pandas().T

changes.filter(
    pl.col('change') < -80
)

# create brackets
brackets_delta_robin = (13 - (-7)) / 500
brackets_delta_robin  # 0.04


def bracket_number_from_income_robin(s):
    res = ((np.log2(s) + 7) / 0.04)
    return res

bracket_number_from_income_robin(8192)


worth.with_columns(
    bracket_number_from_income_robin((pl.col('worth') * 1e6 * 0.03 / 365.).alias('bracket')).cast(pl.Int64)
)

bracket = worth.with_columns(
    bracket_number_from_income_robin((pl.col('worth') * 1e6 * 0.03 / 365.).alias('bracket')).cast(pl.Int64),
    pl.col('worth').pct_change().alias('change')
).drop_nulls()

bracket_agg = bracket.groupby(['bracket', 'time']).agg(
    pl.col('change').mean(),
    pl.col('person').unique().count()
).sort(['bracket', 'time'])

bracket.filter(
    pl.col('person') == 'jeff_bezos'
).tail()

# double check if it looks correct
total1 = pl.read_csv('../../ddf--datapoints--population--by--geo--time--income_group.csv')
total1 = total1.groupby(['income_group', 'time']).agg(pl.col('population').sum())
total1 = total1.sort(['income_group', 'time'])

bracket_agg.groupby(
    (pl.col('bracket') // 10, pl.col('time'))
).agg(
    pl.col('person').sum()
).sort(['bracket', 'time'])

bagg = bracket_agg.groupby(
    (pl.col('bracket') // 10, pl.col('time'))
).agg(
    pl.col('person').sum()
).select(['bracket', 'time', 'person'])


total1 = total1.filter(
    pl.col('population') > 0
)

bagg.join(total1, left_on=['bracket', 'time'], right_on=['income_group', 'time'], how='outer').filter(
    (pl.col('person') - pl.col('population')) != 0
)

# Out[100]:
# shape: (1, 4)
# ┌─────────┬──────┬────────┬────────────┐
# │ bracket ┆ time ┆ person ┆ population │
# │ ---     ┆ ---  ┆ ---    ┆ ---        │
# │ i64     ┆ i64  ┆ u32    ┆ i64        │
# ╞═════════╪══════╪════════╪════════════╡
# │ 57      ┆ 2021 ┆ 164    ┆ 165        │
# └─────────┴──────┴────────┴────────────┘
# ^ Not sure why...

# ok let's save file to disk
# 1. average change per billionaire
person = pl.scan_csv('../../ddf--entities--person.csv')
person.head().collect()
person = person.select(['person', 'name']).collect()
person.filter(pl.any(person.is_duplicated()))
person

out1 = worth.with_columns(
    pl.col('worth').shift(1).over('person').alias('worth_perv'),
    pl.col('worth').pct_change().over('person').alias('change') * 100
).join(person, on='person', how='inner').sort(['person', 'time'])
out1.columns
out1.select(['person', 'name', 'time', 'worth', 'worth_perv', 'change']).write_csv('./out1.csv')

out1.select(
    pl.col('change').mean().alias('mean'),
    pl.col('change').min().alias('min'),
    pl.col('change').quantile(.25).alias('25%'),
    pl.col('change').quantile(.5).alias('50%'),
    pl.col('change').quantile(.75).alias('75%'),
    pl.col('change').max().alias('max')
)

# 2. average change per bracket
bracket = worth.with_columns(
    bracket_number_from_income_robin((pl.col('worth') * 1e6 * 0.03 / 365.).alias('bracket')).cast(pl.Int64),
    pl.col('worth').pct_change().alias('change')
).drop_nulls()

bracket_agg = bracket.groupby(['bracket', 'time']).agg(
    pl.col('change').mean() * 100,
    pl.col('person').unique().count()
).sort(['time', 'bracket'])



all_bracket = pl.read_csv('../../../ddf--worldbank--povcalnet/ddf--entities--income_bracket_800.csv')
all_bracket

out2 = bracket_agg.join(all_bracket, left_on=['bracket'], right_on=['income_bracket_800'], how='inner')
out2.columns

out2.select(['time', 'bracket', 'person', 'bracket_start', 'bracket_end', 'change']).write_csv('out2.csv')
out2

bracket_agg

bracket

worth
