# -*- coding: utf-8 -*-

""" Try using bigger interest rates for income.

based on https://docs.google.com/spreadsheets/d/1aq8xd5h8PhtFYljDkBTBWgMtuxG_MOPyHX_G3Y_124A/edit#gid=1813914259


Below is message from Ola, in slack.

So, my conclusions, is that we should use an incremental rate to
extract the annual return on capital,... that starts on 3% for the
poorest Billies,..and then increases linearly from 3% to 33% over the
brackets 599 to 653, like the black dashed line show in this graph
(form column REVENUE RATE here).  Please calculate a new version of
income mountains with this incremental rate and we'll see what income
Mr. Musk ends up on now!

"""

import numpy as np
import polars as pl

worth_file = '../../ddf--datapoints--worth--by--person--time.csv'

worth = pl.read_csv(worth_file)

worth = worth.sort(['person', 'time'])

worth

# First, we need a mapping from worth to interest rates
brackets_delta_robin = 0.04


def bracket_number_from_income_robin(s, integer=True):
    # FIXME: double check if it should +1 to the result
    # because int(x) will drop the decimal part.
    res = ((np.log2(s) + 7) / brackets_delta_robin)
    if integer:
        return res.astype(int)
    return res


def bracket_number_to_income_robin(n):
    res = np.power(2, (n+1) * brackets_delta_robin - 7)
    return res


bracket_number_from_income_robin(8192)  # 500

bracket_number_to_income_robin(499)  # 8192 (the upper bound)

# so we want to set the interest rate to 0.03 before 600 and incerease linearly to 0.33 at 653
# and use 0.33 after 653
bracket_number_to_income_robin(599)  # 131072.0
bracket_number_to_income_robin(653)  # 585780.2390456081

# ... But the income bracket calculated above
# is based on the assumption of 3% interest rates.
# so we want to convert them into worth. And see the thresholds
w599 = bracket_number_to_income_robin(599) * 365 / 0.03  # 1594709333.3333335
w653 = bracket_number_to_income_robin(653) * 365 / 0.03  # 7126992908.388232
w599, w653

bracket_number_from_income_robin(w599 * 0.03 / 365)  # 600
bracket_number_from_income_robin(w653 * 0.03 / 365)  # 654

# now compute the slope in log scale
# we know that (x = w599, y = 0.03), (x = w653, y = 0.33)
logw599 = np.log(w599)
logw653 = np.log(w653)

# y = ax + b
a = 0.3 / (logw653 - logw599)
b = 0.03 - (a * logw599)

a, b


def interest_rate_from_worth(w):
    if np.log(w) < logw599:
        return 0.03
    elif np.log(w) >= logw653:
        return 0.33
    else:
        return a * np.log(w) + b


interest_rate_from_worth(1594709333.3333335)  # 0.03
interest_rate_from_worth(7126992908.388232)  # 0.33

# checking the plot
xs = np.linspace(1000, 10000, 10000)
xs_ = [bracket_number_from_income_robin(x * 1e6 * 0.03 / 365, integer=False) for x in xs]
ys = [interest_rate_from_worth(x * 1e6) for x in xs]

import matplotlib.pyplot as plt

plt.plot(xs_, ys)
plt.show()

# looks good.
# now we can go from worth to rates and income

output = worth.with_columns(
    # would be speed up by changing the function into expression. pl.when()
    (pl.col('worth') * 1e6).apply(interest_rate_from_worth).alias('rates')
).with_columns([
    (pl.col('worth') * pl.col('rates') * 1e6).cast(pl.Int64).alias('annual_income'),
    (pl.col('worth') * pl.col('rates') * 1e6 / 365).cast(pl.Int64).alias('daily_income'),
    (pl.col('worth') * 1e6 * 0.03 / 365).apply(bracket_number_from_income_robin).alias('bracket_0_3')
])

output

output.select(
    ['person', 'time', 'annual_income']
).write_csv('../../ddf--datapoints--annual_income--by--person--time.csv')


output.select(
    ['person', 'time', 'daily_income']
).write_csv('../../ddf--datapoints--daily_income--by--person--time.csv')

output.filter(
    pl.col('bracket_0_3') < 600
)

output['worth'].min() * 1e6


worth