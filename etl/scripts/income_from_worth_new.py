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

Update:
https://gapminder.slack.com/archives/C020AAUCB6H/p1700726047067889?thread_ts=1700094154.474099&cid=C020AAUCB6H
I think about it that way I think we should use 20% as our standard TOP-return (instead of 30%) for the future scenario.
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

# now create a function to calculate the rate of capital return from worth.
logw599 = np.log(w599)
logw653 = np.log(w653)
max_val = 0.2
min_val = 0.03


def interest_rate_from_worth_linear(w,
                                    min_threshold=logw599,
                                    max_threshold=logw653,
                                    min_val=min_val,
                                    max_val=max_val):
    logw = np.log(w)
    if logw < min_threshold:
        y = min_val
    elif logw > max_threshold:
        y = max_val
    else:
        y = ((logw - min_threshold) /
             (max_threshold - min_threshold)) * (max_val - min_val) + min_val
    return y


interest_rate_from_worth_linear(1594709333.3333335)  # should equal min_val
interest_rate_from_worth_linear(7126992908.388232)  # should equal max_val

# checking the plot
xs = np.logspace(2, 6, 10000)
xs_ = [bracket_number_from_income_robin(x * 1e6 * 0.03 / 365, integer=False) for x in xs]
ys1 = [interest_rate_from_worth_linear(x * 1e6) for x in xs]

import matplotlib.pyplot as plt
plt.rcParams['font.size'] = 12
plt.rcParams['figure.dpi'] = 196

plt.plot(xs_, ys1)
plt.show()

# The turn in this curve is so abrupt that it causes the generated
# data to change a lot at the turning point, so we need a better way
# to do it.
# Alternative: Use a sigmoid function.
def sigmoid(x, a=1, b=0, max_val=1, min_val=0):
    scaled_x = (x - b) * a
    return (max_val - min_val) / (1 + np.exp(-scaled_x)) + min_val

# the midpoint
mid = (599 + 653) / 2 + 1
mid

# steepness
steepness = 1/20

xs2 = np.linspace(500, 1000, 1000)
ys2 = sigmoid(xs2, a=steepness, b=mid, max_val=max_val*100, min_val=min_val*100) / 100
# ys2[:20]
plt.plot(xs_, ys1)
plt.plot(xs2, ys2)
plt.show()

def interest_rate_from_worth_alt(w):
    bn = bracket_number_from_income_robin(w * 0.03 / 365, integer=False)
    return sigmoid(bn, a=steepness, b=mid, max_val=max_val, min_val=min_val)


xs = np.logspace(2, 6, 10000)
xs_ = [bracket_number_from_income_robin(x * 1e6 * 0.03 / 365, integer=False) for x in xs]
ys2 = [interest_rate_from_worth_alt(x * 1e6) for x in xs]

import seaborn as sns
sns.set_style('whitegrid')

plt.plot(xs_, ys1, label='old')
plt.plot(xs_, ys2, label='new')
plt.legend()
plt.show()


# looks good.
# now we can go from worth to rates and income

output = worth.with_columns(
    # would be speed up by changing the function into expression. pl.when()
    (pl.col('worth') * 1e6).map_elements(interest_rate_from_worth_alt).alias('rates')
).with_columns([
    (pl.col('worth') * pl.col('rates') * 1e6).cast(pl.Int64).alias('annual_income'),
    (pl.col('worth') * pl.col('rates') * 1e6 / 365).cast(pl.Int64).alias('daily_income'),
    (pl.col('worth') * 1e6 * 0.03 / 365).map_elements(bracket_number_from_income_robin).alias('bracket_0_3')
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
