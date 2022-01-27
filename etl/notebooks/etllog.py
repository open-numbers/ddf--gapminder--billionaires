 2/1: import pandas as pd
 2/2: pd.__version__
 2/3: !ls -lah
 2/4: df = pd.read_csv('ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv')
 2/5: df.memory_usage
 2/6: df.memory_usage()
 2/7: df.memory_usage().sum() / 1024 / 1024
 2/8: from ddf_utils.str import format_float_digits
 2/9: df.columns
2/10: df.dtypes
2/11: df['population'] = df['population'].asof(int)
2/12: df['population'] = df['population'].as(int)
2/13: df['population'] = df['population'].asof("int")
2/14: df['population'].asof?
2/15: df['population'].asof
2/16: s = df['population']
2/17: s.asof?
2/18: s.astype?
2/19: df['population'] = df['population'].astype("int")
2/20: df['population'] = df['population'].fillna(0).astype("int")
2/21: df.to_csv('ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv', index=False)
 3/1: import  pandas as pd
 3/2: pd.read_csv("ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
 3/3: df = pd.read_csv("ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
 3/4: df_ = df.groupby(['geo', 'year', 'income_bracket']).first()
 3/5: df_
 3/6: df_ = df_.reset_index()
 3/7: df_.coverage_type.unique()
 3/8: df_ = df_[['geo', 'year', 'income_bracket', 'population']]
 3/9: df_.to_csv('ddf--datapoints--population--by--geo--year--income_bracket.csv', index=False)
 4/1: import pandas as pd
 4/2: df = pd.read_csv('etl/source/0000.csv')
 4/3: df.head()
 4/4: df[['CountryCode', 'CoverageType']].drop_duplicates()
 4/5: df[['CountryCode', 'CoverageType']].drop_duplicates().to_csv('~/tmp/test.csv', index=False)
10/1: import sys
10/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
10/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
10/4: print (ps_json)
10/5: sys.exit(0)
 9/1: import codecs, os;__pyfile = codecs.open('''/tmp/pyIqKMGH''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIqKMGH''');exec(compile(__code, '''/tmp/pyIqKMGH''', 'exec'));
 9/2: import codecs, os;__pyfile = codecs.open('''/tmp/pyTh0X8l''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyTh0X8l''');exec(compile(__code, '''/tmp/pyTh0X8l''', 'exec'));
 9/3: import codecs, os;__pyfile = codecs.open('''/tmp/py9CaNHO''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py9CaNHO''');exec(compile(__code, '''/tmp/py9CaNHO''', 'exec'));
 9/4: import codecs, os;__pyfile = codecs.open('''/tmp/pyIg9tvC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIg9tvC''');exec(compile(__code, '''/tmp/pyIg9tvC''', 'exec'));
 9/5: import codecs, os;__pyfile = codecs.open('''/tmp/pyinMxlG''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyinMxlG''');exec(compile(__code, '''/tmp/pyinMxlG''', 'exec'));
11/1: import pandas as pd
11/2: pd.read_csv("ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
11/3: df = pd.read_csv("ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
11/4: df.geo.unique()
11/5: df.geo.hasnans
11/6:
for c in df.columns:
    print(f"{c}, {df[c].hasnans}")
11/7: df[pd.isnull(df['population'])]
11/8: df[pd.isnull(df['population'])].geo.unique()
14/1: import pandas as pd
14/2: df = pd.read_csv('../../ddf--datapoints--population_percentage--by--geo--year--coverage_type--income_bracket.csv')
14/3: df[pd.isnull(df['population_percentage'])]
14/4: df = pd.read_csv('../../ddf--datapoints--population_percentage_smooth--by--geo--year--coverage_type--income_bracket.csv')
14/5: df[pd.isnull(df['population_percentage_smooth'])]
14/6: df = pd.read_csv('../../ddf--datapoints--population_percentage--by--geo--year--coverage_type--income_bracket.csv')
14/7: df = df.set_index(['geo', 'year', 'coverage_type', 'income_bracket'])
14/8: df
14/9: df.groupby(level=['geo', 'year', 'coverage_type', 'income_bracket'])
14/10: df.groupby(level=['geo', 'year', 'coverage_type', 'income_bracket']).agg(func)
14/11:
def func(x):
    if x.hasnans:
        if x.dropna().empty:
            return None
        else:
            return x.fillna(0)
    else:
        return x
14/12: df.groupby(level=['geo', 'year', 'coverage_type', 'income_bracket']).agg(func)
14/13: df_new = df.groupby(level=['geo', 'year', 'coverage_type', 'income_bracket']).agg(func)
14/14: df_new.shape
14/15: df.shape
14/16: df.hasnans
14/17: df['population_percentage'].hasnans
14/18: df_new['population_percentage'].hasnans
14/19: df_new = df.groupby(level=['geo', 'year', 'coverage_type', 'income_bracket']).get_group(('IND', 2018, 'A'))
14/20: df.groupby(level=['geo', 'year', 'coverage_type']).get_group(('IND', 2018, 'A'))
14/21: df.groupby(level=['geo', 'year', 'coverage_type']).get_group(('IND', 2018, 'N'))
14/22: df_new = df.groupby(level=['geo', 'year', 'coverage_type']).agg(func)
14/23:
def func(x):
    if x.hasnans:
        if x.dropna().empty:
            return pd.Series([])
        else:
            return x.fillna(0)
    else:
        return x
14/24: df_new = df.groupby(level=['geo', 'year', 'coverage_type']).agg(func)
14/25: df_new = df.groupby(level=['geo', 'year', 'coverage_type']).apply(func)
14/26: df_new = df.groupby(level=['geo', 'year', 'coverage_type'])['population_percentage'].apply(func)
14/27: q
16/1: import pandas as pd
16/2: df = pd.read_csv('../../ddf--datapoints--population_percentage--by--geo--year--coverage_type--income_bracket.csv')
16/3: df = df.set_index(['geo', 'year', 'coverage_type', 'income_bracket'])
16/4: df.groupby(level=['geo', 'year', 'coverage_type']).get_group(('IND', 2018, 'N'))
16/5: df.groupby(level=['geo', 'year', 'coverage_type']).get_group(('IND', 2018, 'A'))
16/6: df.hasnans
16/7: df['population_percentage'].hasnans
16/8: df = pd.read_csv("ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
16/9: df = pd.read_csv("../../ddf--datapoints--population--by--geo--year--coverage_type--income_bracket.csv")
16/10: df = df.set_index(['geo', 'year', 'coverage_type', 'income_bracket'])
16/11: df['population'].hasnans
27/1: import sys
27/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
27/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
27/4: print (ps_json)
27/5: sys.exit(0)
28/1: wb_groups = pd.read_csv('../source/fixtures/wb_income_groups.csv'')
28/2: import pandas as pd
28/3: wb_groups = pd.read_csv('../source/fixtures/wb_income_groups.csv'')
28/4: wb_groups = pd.read_csv('../source/fixtures/wb_income_groups.csv')
28/5: import codecs, os;__pyfile = codecs.open('''/tmp/pyVkW2Ki''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyVkW2Ki''');exec(compile(__code, '''/tmp/pyVkW2Ki''', 'exec'));
28/6: import codecs, os;__pyfile = codecs.open('''/tmp/pyo5YU1T''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyo5YU1T''');exec(compile(__code, '''/tmp/pyo5YU1T''', 'exec'));
28/7: import codecs, os;__pyfile = codecs.open('''/tmp/pyb61f7K''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyb61f7K''');exec(compile(__code, '''/tmp/pyb61f7K''', 'exec'));
28/8: import codecs, os;__pyfile = codecs.open('''/tmp/pypSCQtp''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pypSCQtp''');exec(compile(__code, '''/tmp/pypSCQtp''', 'exec'));
28/9: import codecs, os;__pyfile = codecs.open('''/tmp/pygiVhTQ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pygiVhTQ''');exec(compile(__code, '''/tmp/pygiVhTQ''', 'exec'));
28/10: import codecs, os;__pyfile = codecs.open('''/tmp/pyJdyGHB''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyJdyGHB''');exec(compile(__code, '''/tmp/pyJdyGHB''', 'exec'));
28/11: import codecs, os;__pyfile = codecs.open('''/tmp/pyX5l8Es''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyX5l8Es''');exec(compile(__code, '''/tmp/pyX5l8Es''', 'exec'));
28/12: import codecs, os;__pyfile = codecs.open('''/tmp/pyeXRm4D''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyeXRm4D''');exec(compile(__code, '''/tmp/pyeXRm4D''', 'exec'));
28/13: import codecs, os;__pyfile = codecs.open('''/tmp/pyTt42MZ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyTt42MZ''');exec(compile(__code, '''/tmp/pyTt42MZ''', 'exec'));
28/14: import codecs, os;__pyfile = codecs.open('''/tmp/pyzFS71e''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyzFS71e''');exec(compile(__code, '''/tmp/pyzFS71e''', 'exec'));
28/15: import codecs, os;__pyfile = codecs.open('''/tmp/py31wusO''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py31wusO''');exec(compile(__code, '''/tmp/py31wusO''', 'exec'));
28/16: import codecs, os;__pyfile = codecs.open('''/tmp/pyjs5gnf''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyjs5gnf''');exec(compile(__code, '''/tmp/pyjs5gnf''', 'exec'));
28/17: import codecs, os;__pyfile = codecs.open('''/tmp/pyK2EAPi''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyK2EAPi''');exec(compile(__code, '''/tmp/pyK2EAPi''', 'exec'));
28/18: import codecs, os;__pyfile = codecs.open('''/tmp/pyESuhhI''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyESuhhI''');exec(compile(__code, '''/tmp/pyESuhhI''', 'exec'));
28/19: import codecs, os;__pyfile = codecs.open('''/tmp/pyLjxbyN''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyLjxbyN''');exec(compile(__code, '''/tmp/pyLjxbyN''', 'exec'));
28/20: import codecs, os;__pyfile = codecs.open('''/tmp/pyqzBFyP''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyqzBFyP''');exec(compile(__code, '''/tmp/pyqzBFyP''', 'exec'));
28/21: import codecs, os;__pyfile = codecs.open('''/tmp/pybHNvx8''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pybHNvx8''');exec(compile(__code, '''/tmp/pybHNvx8''', 'exec'));
28/22: import codecs, os;__pyfile = codecs.open('''/tmp/py1RWW26''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py1RWW26''');exec(compile(__code, '''/tmp/py1RWW26''', 'exec'));
28/23: wb_groups
28/24: import codecs, os;__pyfile = codecs.open('''/tmp/pyIvkZif''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIvkZif''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/25: import codecs, os;__pyfile = codecs.open('''/tmp/pybjaWjg''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pybjaWjg''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/26: import codecs, os;__pyfile = codecs.open('''/tmp/pyxmNoyb''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyxmNoyb''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/27: import codecs, os;__pyfile = codecs.open('''/tmp/pywIT3AR''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pywIT3AR''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/28: import codecs, os;__pyfile = codecs.open('''/tmp/pycCVVQ7''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pycCVVQ7''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/29: import codecs, os;__pyfile = codecs.open('''/tmp/py8FlcbJ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py8FlcbJ''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/30: import codecs, os;__pyfile = codecs.open('''/tmp/pyp9pbBa''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyp9pbBa''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/31: import codecs, os;__pyfile = codecs.open('''/tmp/py9Jjzqv''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py9Jjzqv''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/32: import codecs, os;__pyfile = codecs.open('''/tmp/pyWvJfjU''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWvJfjU''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/33: import codecs, os;__pyfile = codecs.open('''/tmp/pyhE5REZ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyhE5REZ''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/34: import codecs, os;__pyfile = codecs.open('''/tmp/pya8e9mt''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pya8e9mt''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
28/35: import codecs, os;__pyfile = codecs.open('''/tmp/py3AsvuM''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3AsvuM''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/geogroup.py''', 'exec'));
31/1: import sys
31/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
31/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
31/4: print (ps_json)
31/5: sys.exit(0)
33/1: import pandas as pd
33/2:
synonyms = pd.read_csv('../source/fixtures/ddf--open_numbers/ddf--synonyms--geo.csv')
wb_groups = pd.read_csv('../source/fixtures/wb_income_groups.csv')
33/3: syn_mapping = synonyms.set_index('synonym')['geo'].to_dict()
33/4: df = pd.read_csv('../../ddf--datapoints--population--by--geo--year--income_bracket.csv')
33/5: df
33/6: df['geo'] = df['geo'].map(syn_mapping)
33/7: df['geo'].hasnans
33/8: df = pd.read_csv('../../ddf--datapoints--population--by--geo--year--income_bracket.csv')
33/9: df['geo_'] = df['geo'].map(syn_mapping)
33/10: df['geo'].hasnans
33/11: df['geo_'].hasnans
33/12: df[pd.isnull(df['geo_'])]
33/13: geo = pd.read_csv('../../ddf--entities--geo.csv')
33/14:
geo['geonew'] = geo['name'].map(syn_mapping)
geo_mapping = geo.set_index('geo')['geonew'].to_dict()
33/15: df['geo_'] = df['geo'].map(geo_mapping)
33/16: df['geo_'].hasnans
33/17: df[pd.isnull(df['geo_'])]
33/18: df[df['geo'] != df['geo_']]
33/19: df['geo'] = df['geo'].map(geo_mapping)
33/20: df = pd.read_csv('../../ddf--datapoints--population--by--geo--year--income_bracket.csv')
33/21: df
33/22: df['geo'] = df['geo'].map(geo_mapping)
33/23: df['geo'].hasnans
33/24:
def groups_over_time(df, col):
    df_copy = df.copy()
    df_copy['newindex'] = df['geo'] + '-' + df['year'].astype(str)
    return df_copy.set_index('newindex')[col].to_dict()
33/25: groups_over_time(wb_groups, "WB's 4 income levels")
33/26:
def groups_over_time(df, col):
    df_copy = df.copy()
    df_copy['newindex'] = df['geo'] + '-' + df['time'].astype(str)
    return df_copy.set_index('newindex')[col].to_dict()
33/27: groups_over_time(wb_groups, "WB's 4 income levels")
33/28: wb_mapping = groups_over_time(wb_groups, "WB's 4 income levels")
33/29:
def translate_with_time_series(df: pd.DataFrame, mapping, newcol):
    df_copy = df.copy()
    df_copy['newindex'] = df['geo'] + '-' + df['year'].astype(str)
    df_copy[newcol] = df_copy['newindex'].map(mapping)
    return df_copy.drop(columns=['newindex'])
33/30: translate_with_time_series(df, wb_mapping, 'wb_income_level')
33/31: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_level')
33/32: df2.dropna(how='any')
33/33: df2 = df2.dropna(how='any')
33/34:
import seaborn
import matplotlib.pyplot as plt
%matplotlib inline
33/35:
import seaborn
import matplotlib.pyplot as plt
%matplotlib inline
33/36: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/37: df2.set_index(['wb_income_level', 'year', 'income_bracket']).groupby(levels='wb_income_level').sum()
33/38: df2.set_index(['wb_income_level', 'year', 'income_bracket']).groupby(level='wb_income_level').sum()
33/39: df2.set_index(['wb_income_level', 'year', 'income_bracket']).groupby(level=['wb_income_level', 'year', 'income_bracket']).sum()
33/40: df_wb = df2.groupby(level=['wb_income_level', 'year', 'income_bracket']).sum()
33/41: df_wb = df2.groupby(level=['wb_income_level', 'year', 'income_bracket'], as_index=True).sum()
33/42: df_wb = df2.groupby(by=['wb_income_level', 'year', 'income_bracket'], as_index=True).sum()
33/43: df_wb
33/44: df_wb = df2.groupby(by=['wb_income_level', 'year', 'income_bracket']).sum()
33/45: df_wb
33/46: df_wb.loc["High income"]
33/47: df_wb.loc[("High income", 1987)]
33/48: df_wb.loc[("High income", 1987)].plot()
33/49:
seaborn.set_context('notebook')
seaborn.set_style('whitegrid')
plt.rcParams['figure.figsize'] = [16, 8]
33/50: df_wb.loc[("High income", 1987)].plot()
33/51: df_wb.std()
33/52: df_wb.loc[("High income", 2018)].plot()
33/53: df_wb.std()
33/54: df_wb.loc[("High income", 2018)].std()
33/55: df_wb.loc[("High income", 1987)].std()
33/56:
import sys
sys.path.append('../scripts')
33/57: import smoothlib
33/58: df = pd.read_csv('../../ddf--datapoints--population_smooth--by--geo--year--income_bracket.csv')
33/59: df
33/60: df['geo'] = df['geo'].map(geo_mapping)
33/61: df['geo'].hasnans
33/62: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_level')
33/63: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/64: df_wb = df2.groupby(by=['wb_income_level', 'year', 'income_bracket']).sum()
33/65: df_wb
33/66: df_wb.loc[("High income", 2018)].plot()
33/67: df = pd.read_csv('../../ddf--datapoints--population--by--geo--year--income_bracket.csv')
33/68: df['geo'] = df['geo'].map(geo_mapping)
33/69: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_level')
33/70: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/71: df_wb = df2.groupby(by=['wb_income_level', 'year', 'income_bracket']).sum()
33/72: df_wb
33/73: df_wb.loc[("High income", 2018)].plot()
33/74: smoothlib.run_smooth(df_wb.loc[("High income", 1987)], 8, 0)
33/75: smoothlib.run_smooth(df_wb.loc[("High income", 1987)].values, 8, 0)
33/76: df_wb.loc[("High income", 1987)]
33/77: df_wb.loc[("High income", 1987)]['populatin']
33/78: df_wb.loc[("High income", 1987)]['population']
33/79: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'], 8, 0)
33/80: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'], 12, 0)
33/81: df_wb.loc[("High income", 1987)].plot()
33/82: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'], 12, 1)
33/83: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'], 20, 0)
33/84: df_wb.loc[("High income", 1987)]['population'].values
33/85: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000, 20, 0)
33/86: df_wb.loc[("High income", 1987)]['population'] / 1000
33/87: df_wb.loc[("High income", 1987)]['population'] / 10000
33/88: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 10000, 20, 0)
33/89: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 20, 0)
33/90: smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 20, 0) * 1000000
33/91: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 20, 0) * 1000000
33/92: plt.plot(p)
33/93: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 5, 0) * 1000000
33/94: plt.plot(p)
33/95: p.values
33/96: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 20, 0) * 1000000
33/97: plt.plot(p)
33/98: p.values
33/99: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 5, 0) * 1000000
33/100: plt.plot(p)
33/101:
p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 10, 0)
p = smoothlib.run_smooth(p, 5, 0)
p = smoothlib.run_smooth(p, 5, 0) * 1000000
33/102: plt.plot(p)
33/103: p.values
33/104: df = pd.read_csv('../../ddf--datapoints--population_smooth--by--geo--year--income_bracket.csv')
33/105: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_level')
33/106: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/107: df_wb = df2.groupby(by=['wb_income_level', 'year', 'income_bracket']).sum()
33/108: df_wb
33/109: df_wb.loc[("High income", 1987)].plot()
33/110: df_wb.loc[("High income", 1987)]['population'].values
33/111: df_wb.loc[("High income", 1987)]['population_smooth'].values
33/112: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population'] / 1000000, 5, 0)
33/113: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 5, 0)
33/114: plt.plot(p)
33/115: df_wb.loc[("Low income", 1987)].plot()
33/116: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 10, 0)
33/117: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 8, 0)
33/118: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 2, 0)
33/119: plt.plot(p)
33/120: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 2, 1)
33/121: plt.plot(p)
33/122: p = smoothlib.run_smooth(df_wb.loc[("High income", 1987)]['population_smooth'], 3, 0)
33/123: plt.plot(p)
33/124: p.values
33/125: df_wb.loc[("High income", 2018)]['population_smooth'].values
33/126: df_wb.loc[("Low income", 2018)]['population_smooth'].values
33/127: df_wb.loc[("Low income", 1987)]['population_smooth'].values
33/128:
df_wb.loc[("Low income", 1987)]['population_smooth'].plot()
df_wb.loc[("High income", 1987)]['population_smooth']
33/129:
df_wb.loc[("Low income", 1987)]['population_smooth'].plot()
df_wb.loc[("High income", 1987)]['population_smooth'].plot()
33/130:
df_wb.loc[("Low income", 1987)]['population_smooth'].plot()
# df_wb.loc[("High income", 1987)]['population_smooth'].plot()
33/131:
df_wb.loc[("Low income", 1987)]['population_smooth'].plot()
df_wb.loc[("High income", 1987)]['population_smooth'].plot()
33/132:
df_wb.loc[("High income", 1987)]['population_smooth'].plot()
df_wb.loc[("High income", 2018)]['population_smooth'].plot()
33/133: df_wb.loc[("Middle income", 1987)].plot()
33/134: df_wb.loc[("Upper middle income", 1987)].plot()
33/135: df_wb.loc[("Lower middle income", 1987)].plot()
33/136: df_wb.loc[("Upper middle income", 2000)].plot()
33/137: translate_with_time_series(df, wb_mapping, 'wb_income_group')
33/138: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_group')
33/139: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/140: df_wb = df2.groupby(by=['wb_income_group', 'year', 'income_bracket']).sum()
33/141: df_wb
33/142: df_wb2 = df_wb.reset_index()
33/143: df_wb2['wb_income_group'].unique()
33/144: df_wb2['wb_income_group'].replace?
33/145: df_wb2['wb_income_group'].replace({'High income': 1, 'Low income': 4, 'Upper middle income': 2, 'Lower middle income': 3})
33/146: df_wb2['wb_income_group'] = df_wb2['wb_income_group'].replace({'High income': 1, 'Low income': 4, 'Upper middle income': 2, 'Lower middle income': 3})
33/147: df_wb2
33/148: df_wb2.sort_values(by=['wb_income_group', 'year', 'income_bracket'])
33/149: df_wb2 = df_wb2.sort_values(by=['wb_income_group', 'year', 'income_bracket'])
33/150: df_wb2.to_csv('../../ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv', index=False)
33/151: west_and_rest = pd.read_csv('../source/fixtures/west_and_rest.csv')
33/152: wr_mapping = west_and_rest.set_index('geo')['geo.gm_west_rest'].to_dict()
33/153: wr_mapping
33/154: df_wr = df.copy()
33/155: df_wr['west_and_rest'] = df_wr['geo'].map(wr_mapping)
33/156: df_wr
33/157: df_wr.drop(columns=['geo']).groupby(['west_and_rest', 'year', 'income_bracket']).sum()
33/158: df_wr_sum = df_wr.drop(columns=['geo']).groupby(['west_and_rest', 'year', 'income_bracket']).sum()
33/159:
df_wr_sum = df_wr_sum.reset_index()
df_wr_sum['west_and_rest'] = df_wr_sum['west_and_rest'].replace({'west': 1, 'rest': 2})
33/160: df_wr_sum
33/161: df_wr_sum.sort_values(by=['west_and_rest', 'year', 'income_bracket']).to_csv('../../ddf--datapoints--population_smooth--by--west_and_rest--year--income_bracket.csv', index=False)
33/162: df_wb2
33/163: df_wb2.groupby(['wb_income_group', 'year', 'income_bracket']).apply(lambda x: x / x.sum())
33/164: df_wb2.groupby(['wb_income_group', 'year']).set_index('income_bracket').apply(lambda x: x / x.sum())
33/165: df_wb2.groupby(['wb_income_group', 'year'])['population_smooth'].apply(lambda x: x / x.sum())
33/166: df_wb2.set_index('income_bracket').groupby(['wb_income_group', 'year']).apply(lambda x: x / x.sum())
33/167: df_wb2.set_index('income_bracket').groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/168: df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/169: from ddf_utils.str import format_float_digits
33/170: df_wr_sum['population_smooth'] = df_wr_sum['population_smooth'].astype(int)
33/171: df_wr_sum.sort_values(by=['west_and_rest', 'year', 'income_bracket']).to_csv('../../ddf--datapoints--population_smooth--by--west_and_rest--year--income_bracket.csv', index=False)
33/172: df_wb2['population_smooth'] = df_wb2['population_smooth'].astype(int)
33/173: df_wb2.to_csv('../../ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv', index=False)
33/174: df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/175: df_wb3 = df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/176: df_wb3
33/177:
df_wb3.columns = ['population_smooth']
df_wb3['population_smooth'] = df_wb3['population_smooth'].map(format_float_digits)
33/178: df_wb3 = df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/179:
df_wb3.columns = ['population_percentage_smooth']
df_wb3['population_smooth'] = df_wb3['population_smooth'].map(format_float_digits)
33/180:
df_wb3.columns = ['population_percentage_smooth']
df_wb3['population_percentage_smooth'] = df_wb3['population_percentage_smooth'].map(format_float_digits)
33/181: df_wb3 = df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/182:
df_wb3.columns = ['population_percentage_smooth']
df_wb3['population_percentage_smooth'] = df_wb3['population_percentage_smooth'].map(format_float_digits)
33/183: df_wb3.to_csv('../../ddf--datapoints--population_percentage_smooth--by--wb_income_group--year--income_bracket.csv')
33/184: df_wb3.groupby(level=['wb_income_group', 'year']).sum()
33/185: df_wb3.drop(columns=['income_bracket']).groupby(level=['wb_income_group', 'year']).sum()
33/186: df_wb3.groupby(level=['wb_income_group', 'year'])['population_percentage_smooth'].sum()
33/187: df_wb3.groupby(level=['wb_income_group', 'year'])['population_percentage_smooth'].agg(sum)
33/188: df_wb3.groupby(level=['wb_income_group', 'year'])['population_percentage_smooth'].agg("sum")
33/189: df_wr2 = df_wr_sum.set_index(['west_and_rest', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/190: df_wr2 = df_wr_sum.set_index(['west_and_rest', 'year', 'income_bracket']).groupby(['west_and_rest', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/191: df_wr2
33/192:
df_wr2.columns = ['population_percentage_smooth']
df_wr2['population_percentage_smooth'] = df_wr2['population_percentage_smooth'].map(format_float_digits)
33/193: df_wr2.to_csv('../../ddf--datapoints--population_percentage_smooth--by--west_and_rest--year--income_bracket.csv')
33/194: df_wr2 = df_wr_sum.set_index(['west_and_rest', 'year', 'income_bracket']).groupby(['west_and_rest', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/195:
df_wr2.columns = ['population_percentage_smooth']
df_wr2['population_percentage_smooth'] = df_wr2['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6)))
33/196:
df_wr2.columns = ['population_percentage_smooth']
df_wr2['population_percentage_smooth'] = df_wr2['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/197: df_wr2.to_csv('../../ddf--datapoints--population_percentage_smooth--by--west_and_rest--year--income_bracket.csv')
33/198: df_wb3 = df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/199:
df_wb3.columns = ['population_percentage_smooth']
df_wb3['population_percentage_smooth'] = df_wb3['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/200: df_wb3.to_csv('../../ddf--datapoints--population_percentage_smooth--by--wb_income_group--year--income_bracket.csv')
33/201: regions = west_and_rest.set_index('geo')['four_regions'].to_dict()
33/202: df_region = df.copy()
33/203: df_region
33/204: df_region['region'] = df_region['geo'].map(regions)
33/205: df_regioin
33/206: df_region
33/207: df_region['regioin'].hasnans
33/208: df_region['region'].hasnans
33/209: df
33/210: df_region[pd.isnull(df_region['region'])]
33/211: df = pd.read_csv('../../ddf--datapoints--population_smooth--by--geo--year--income_bracket.csv')
33/212: df
33/213: df['geo'] = df['geo'].map(geo_mapping)
33/214: df['geo'].hasnans
33/215: df_region = df.copy()
33/216: df_region['region'] = df_region['geo'].map(regions)
33/217: df_region['region'].hasnans
33/218: df_region[pd.isnull(df_region['region'])]
33/219: df_region[pd.isnull(df_region['region'])].geo.unique
33/220: df_region[pd.isnull(df_region['region'])].geo.unique()
33/221: df2 = translate_with_time_series(df, wb_mapping, 'wb_income_group')
33/222: df2 = df2.dropna(how='any').drop(columns=['geo'])
33/223: df_wb = df2.groupby(by=['wb_income_group', 'year', 'income_bracket']).sum()
33/224: df_wb
33/225: df_wb2 = df_wb.reset_index()
33/226: df_wb2['wb_income_group'].unique()
33/227: df_wb2['wb_income_group'] = df_wb2['wb_income_group'].replace({'High income': 1, 'Low income': 4, 'Upper middle income': 2, 'Lower middle income': 3})
33/228: df_wb2 = df_wb2.sort_values(by=['wb_income_group', 'year', 'income_bracket'])
33/229: df_wb2['population_smooth'] = df_wb2['population_smooth'].astype(int)
33/230: df_wb2.to_csv('../../ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv', index=False)
33/231: df_wb3 = df_wb2.set_index(['wb_income_group', 'year', 'income_bracket']).groupby(['wb_income_group', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/232:
df_wb3.columns = ['population_percentage_smooth']
df_wb3['population_percentage_smooth'] = df_wb3['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/233: df_wb3.to_csv('../../ddf--datapoints--population_percentage_smooth--by--wb_income_group--year--income_bracket.csv')
33/234: df_wr = df.copy()
33/235: df_wr['west_and_rest'] = df_wr['geo'].map(wr_mapping)
33/236: df_wr
33/237: df_wr_sum = df_wr.drop(columns=['geo']).groupby(['west_and_rest', 'year', 'income_bracket']).sum()
33/238:
df_wr_sum = df_wr_sum.reset_index()
df_wr_sum['west_and_rest'] = df_wr_sum['west_and_rest'].replace({'west': 1, 'rest': 2})
33/239: df_wr_sum['population_smooth'] = df_wr_sum['population_smooth'].astype(int)
33/240: df_wr_sum.sort_values(by=['west_and_rest', 'year', 'income_bracket']).to_csv('../../ddf--datapoints--population_smooth--by--west_and_rest--year--income_bracket.csv', index=False)
33/241: df_wr2 = df_wr_sum.set_index(['west_and_rest', 'year', 'income_bracket']).groupby(['west_and_rest', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/242:
df_wr2.columns = ['population_percentage_smooth']
df_wr2['population_percentage_smooth'] = df_wr2['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/243: df_wr2.to_csv('../../ddf--datapoints--population_percentage_smooth--by--west_and_rest--year--income_bracket.csv')
33/244: df_regioin = df_region.dropna(how='any')
33/245: df_region = df_region.dropna(how='any')
33/246: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/247: df_region.groupby(by=['region', 'year', 'income_bracket']).sum()
33/248: regions
33/249: regions
33/250: west_and_rest
33/251: west_and_rest[west_and_rest.geo == 'geo']
33/252: west_and_rest = pd.read_csv('../source/fixtures/west_and_rest.csv')
33/253: west_and_rest[west_and_rest.geo == 'geo']
33/254: regions = west_and_rest.set_index('geo')['four_regions'].to_dict()
33/255: df_region = df.copy()
33/256: df_region['region'] = df_region['geo'].map(regions)
33/257: df_region['region'].hasnans
33/258: df_region[pd.isnull(df_region['region'])].geo.unique()
33/259: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/260: df_region.groupby(by=['region', 'year', 'income_bracket']).sum()
33/261: df_region['population_smooth'] = df_region['populatioin_smooth'].astype(int)
33/262: df_region['population_smooth'] = df_region['population_smooth'].astype(int)
33/263: df_region = df.copy()
33/264: df_region['world_4region'] = df_region['geo'].map(regions)
33/265: df_region['world_4region'].hasnans
33/266: df_region[pd.isnull(df_region['world_4region'])].geo.unique()
33/267: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/268: df_region.groupby(by=['world_4region', 'year', 'income_bracket']).sum()
33/269: df_region['population_smooth'] = df_region['population_smooth'].astype(int)
33/270: df_region.to_csv('../../ddf--datapoints--population_smooth--by--world_4region--year--income_bracket.csv')
33/271: df_region['world_4region'].unique()
33/272: df_region['world_4region'] = df_region['world_4region'].map({'africa': 1, 'americas': 2, 'asia': 3, 'europ': 4})
33/273: df_region.groupby(by=['world_4region', 'year', 'income_bracket']).sum()
33/274: df_region = df.copy()
33/275: df_region['world_4region'] = df_region['geo'].map(regions)
33/276: df_region['world_4region'].hasnans
33/277: df_region[pd.isnull(df_region['world_4region'])].geo.unique()
33/278: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/279: df_region['world_4region'] = df_region['world_4region'].map({'africa': 1, 'americas': 2, 'asia': 3, 'eurl': 4})
33/280: regions = west_and_rest.set_index('geo')['four_regions'].to_dict()
33/281: df_region = df.copy()
33/282: df_region['world_4region'] = df_region['geo'].map(regions)
33/283: df_region['world_4region'].hasnans
33/284: df_region[pd.isnull(df_region['world_4region'])].geo.unique()
33/285: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/286: df_region['world_4region'].unique()
33/287: df_region['world_4region'] = df_region['world_4region'].map({'africa': 1, 'americas': 2, 'asia': 3, 'europe': 4})
33/288: df_region.groupby(by=['world_4region', 'year', 'income_bracket']).sum()
33/289: df_region['population_smooth'] = df_region['population_smooth'].astype(int)
33/290: df_region.to_csv('../../ddf--datapoints--population_smooth--by--world_4region--year--income_bracket.csv')
33/291: df_region = df_region.groupby(by=['world_4region', 'year', 'income_bracket']).sum()
33/292: df_region = df.copy()
33/293: df_region['world_4region'] = df_region['geo'].map(regions)
33/294: df_region['world_4region'].hasnans
33/295: df_region[pd.isnull(df_region['world_4region'])].geo.unique()
33/296: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/297: df_region['world_4region'].unique()
33/298: df_region['world_4region'] = df_region['world_4region'].map({'africa': 1, 'americas': 2, 'asia': 3, 'europe': 4})
33/299: df_region = df_region.groupby(by=['world_4region', 'year', 'income_bracket']).sum()
33/300: df_region['population_smooth'] = df_region['population_smooth'].astype(int)
33/301: df_region.to_csv('../../ddf--datapoints--population_smooth--by--world_4region--year--income_bracket.csv')
33/302: df_region = df.copy()
33/303: df_region['world_4regions'] = df_region['geo'].map(regions)
33/304: df_region['world_4regions'].hasnans
33/305: df_region[pd.isnull(df_region['world_4regions'])].geo.unique()
33/306: df_region = df_region.dropna(how='any').drop(columns=['geo'])
33/307: df_region['world_4regions'].unique()
33/308: df_region['world_4regions'] = df_region['world_4regions'].map({'africa': 1, 'americas': 2, 'asia': 3, 'europe': 4})
33/309: df_region = df_region.groupby(by=['world_4regions', 'year', 'income_bracket']).sum()
33/310: df_region['population_smooth'] = df_region['population_smooth'].astype(int)
33/311: df_region.to_csv('../../ddf--datapoints--population_smooth--by--world_4regions--year--income_bracket.csv')
33/312: df_region2 = df_region.groupby(['world_4regions', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/313: df_region2
33/314: df_region2.columns = ['population_percentage_smooth']
33/315: df_region2['population_percentage_smooth'] = df_region2['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/316: df_region2.to_csv('../../ddf--datapoints--population_percentage_smooth--by--world_4regions--year--income_bracket.csv')
33/317: on_income = pd.read_csv('../source/fixtures/on_income_groups.csv')
33/318: on_income
33/319: on_income_mapping = groups_over_time(on_income, 'ilevels4')
33/320: on_income.columns = ['geo', 'time']
33/321: on_income.columns = ['geo', 'time', 'ilevels4']
33/322: on_income_mapping = groups_over_time(on_income, 'ilevels4')
33/323: on_income_mapping
33/324: on_income['ilevels'] = on_income['ilevels'].map({'Level 1': 1, 'Level 2': 2, 'Level 3': 3, 'Level 4': 4})
33/325: on_income['ilevels4'] = on_income['ilevels4'].map({'Level 1': 1, 'Level 2': 2, 'Level 3': 3, 'Level 4': 4})
33/326: on_income
33/327: on_income_mapping = groups_over_time(on_income, 'ilevels4')
33/328: on_income_mapping
33/329: translate_with_time_series(df, on_income_mapping, 'on_income_level')
33/330: df_on = translate_with_time_series(df, on_income_mapping, 'on_income_level')
33/331: df_on[pd.isnull(df_on['on_income_level'])]
33/332: df_on[pd.isnull(df_on['on_income_level'])]['geo'].unique()
33/333: df_on.dropna(how='any').drop(columns=['geo'])
33/334: df_on.dropna(how='any').drop(columns=['geo']).groupby(['on_income_level', 'year', 'income_bracket']).sum()
33/335: df_on = df_on.dropna(how='any').drop(columns=['geo']).groupby(['on_income_level', 'year', 'income_bracket']).sum()
33/336: df_on
33/337: df_on = translate_with_time_series(df, on_income_mapping, 'on_income_level')
33/338: df_on[pd.isnull(df_on['on_income_level'])]['geo'].unique()
33/339: df_on = df_on.dropna(how='any').drop(columns=['geo'])
33/340: df_on
33/341: df_on['on_income_level'] = df_on['on_income_level'].astype(int)
33/342: df_on = df_on.groupby(['on_income_level', 'year', 'income_bracket']).sum()
33/343: df_on
33/344: df_on.to_csv('../../ddf--datapoints--population_smooth--by--on_income_level--year--income_bracket.csv')
33/345: df_on2 = df_on.groupby(['world_4regions', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/346: df_on2 = df_on.groupby(['on_income_level', 'year'], as_index=True).apply(lambda x: x / x.sum())
33/347: df_on2
33/348: df_on2.columns = ['population_percentage_smooth']
33/349: df_on2['population_percentage_smooth'] = df_on2['population_percentage_smooth'].map(lambda x: format_float_digits(x, digits=6))
33/350: df_on2
33/351: df_on2.to_csv('../../ddf--datapoints--population_percentage_smooth--by--on_income_level--year--income_bracket.csv')
35/1: import pandas as pd
35/2: df = pd.read_csv('ddf--entities--income_bracket.csv', dtype=str)
35/3: fname = 'ddf--entities--geo.csv'
35/4: df = pd.read_csv(fname, dtype=str)
35/5: df['is--country'] = 'TRUE'
35/6: df.to_csv('ddf--entities--geo--country.csv', index=False)
35/7:
def process(fname, entity_set):
    df = pd.read_csv(fname, dtype=str)
    col = f'is--{entity_set}'
    df[col] = 'TRUE'
    outname = f'ddf--entities--geo--{entity_set}.csv'
    df.to_csv(outname, index=False)
35/8: process('ddf--entities--wb_income_group.csv', 'wb_income_group')
35/9: process('ddf--entities--on_income_group.csv', 'on_income_group')
35/10: process('ddf--entities--world_4regions.csv', 'world_4regions')
35/11: process('ddf--entities--west_and_rest.csv', 'west_and_rest')
36/1: import os
36/2:
for f in os.listdir('./'):
    if '--geo--' in f:
        os.rename(f, f.replace('geo', 'country'))
36/3: !ls
37/1: import os
37/2:
for f in os.listdir('./'):
    if 'entities--country--' in f:
        os.rename(f, f.replace('entities--country', 'entities--geo'))
33/352: geo
33/353: geo['world_4regions'] = geo['geonew'].map(wr_mapping)
33/354: geo
33/355: geo['world_4regions'] = geo['geonew'].map(regions)
33/356: geo
33/357: geo['west_and_rest'] = geo['geonew'].map(wr_mapping)
33/358: geo
33/359: west_and_rest
33/360: west_and_rest.geo.unique()
33/361: west_and_rest[west_and_rest['geo'] == 'kos']
33/362: on_income
33/363: on_income_current = on_income.groupby(['geo', 'time']).last()
33/364: on_income_current
33/365: on_income_current = on_income.set_index(['geo', 'time']).groupby(['geo']).last()
33/366: on_income_current
33/367: on_income_current.to_dict()
33/368: on_income_current['ilevels4'].to_dict()
33/369: on_current_map = on_income_current['ilevels4'].to_dict()
33/370: geo['on_income_level'] = geo['geonew'].map(on_current_map)
33/371: geo
33/372: wb_groups
33/373: wb_groups.set_index(['geo', 'time']).groupby('geo')["WB's income levels"].last()
33/374: wb_groups.set_index(['geo', 'time']).groupby('geo')["WB's 4 income levels"].last()
33/375: wb_groups.set_index(['geo', 'time']).groupby('geo')["WB's 4 income levels"].last().to_dict()
33/376: wb_current_map = wb_groups.set_index(['geo', 'time']).groupby('geo')["WB's 4 income levels"].last().to_dict()
33/377: geo['wb_income_group'] = geo['geonew'].map(wb_current_map)
33/378: geo
33/379: geo['world_4regions'] = geo['world_4regions'].map({'africa': '1', 'americas': '2', 'asia': '3', 'europe': '4'})
33/380: geo['on_income_level'] = geo['on_income_level'].map({1.: '1', 2.: '2', 3.: '3', 4.: '4'})
33/381: geo
33/382: geo['west_and_rest'] = geo['west_and_rest'].map({'west': 1, 'rest': 2})
33/383:
geo['wb_income_group'] = geo['wb_income_group'].map({'High income': '1', 
                                                     'Low income': '4', 
                                                     'Upper middle income': '2', 
                                                     'Lower middle income': '3'})
33/384: geo
33/385:
# geo['west_and_rest'] = geo['west_and_rest'].map({'west': '1', 'rest': '2'})
geo['west_and_rest'] = geo['west_and_rest'].map({1.: '1', 2.: '2'})
33/386: geo
33/387: country = geo.drop(columns='geo')
33/388: country
33/389: country['is--country'] = 'TRUE'
33/390: country
33/391: country.columns
33/392: country.columns = ['name', 'geo', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group', 'is--country']
33/393: country = country[['geo', 'name', 'is--country', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group']]
33/394: country
33/395: country.to_csv('../../ddf--entities--geo--country.csv', index=False)
38/1: import pandas as pd
38/2: import os
38/3:
for f in os.listdir('./'):
    if 'datapoints' in f and 'by--country' in f:
        df = pd.read_csv(f, dtypes=str)
        df['country'] = df['country'].replace({'xkx': 'kos'})
        df.to_csv(f, index=False)
38/4:
for f in os.listdir('./'):
    if 'datapoints' in f and 'by--country' in f:
        df = pd.read_csv(f, dtype=str)
        df['country'] = df['country'].replace({'xkx': 'kos'})
        df.to_csv(f, index=False)
33/396: country = geo.drop(columns='geo')
33/397: country['is--country'] = 'TRUE'
33/398: country.columns
33/399: country.columns = ['name', 'geo', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group', 'is--country']
33/400: country.columns = ['name', 'country', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group', 'is--country']
33/401: country = country[['geo', 'name', 'is--country', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group']]
33/402: country = country[['country', 'name', 'is--country', 'world_4regions', 'west_and_rest', 'on_income_level', 'wb_income_group']]
33/403: country
33/404:
country['world_4regions'] = country['world_4regions'].map(
    {'1': '11', 
     '2': '12', 
     '3': '13', 
     '4': '14'})
33/405:
country['west_and_rest'] = country['west_and_rest'].map(
    {'1': '41', 
     '2': '42'})
33/406:
country['on_income_level'] = country['on_income_level'].map(
    {'1': '21', 
     '2': '22', 
     '3': '23', 
     '4': '24'})
33/407:
country['wb_income_group'] = country['wb_income_group'].map(
    {'1': '31', 
     '2': '32', 
     '3': '33', 
     '4': '34'})
33/408: country
33/409: country.to_csv('../../ddf--entities--geo--country.csv', index=False)
39/1: import pandas as pd
33/410: import os
39/2: import os
40/1: import pandas as pd
40/2: import os
40/3:
for f in os.listdir('./'):
    if 'world_4regions' in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df['world_4regions'] = df['world_4regions'].map({'1': '11', 
                                                         '2': '12', 
                                                         '3': '13', 
                                                         '4': '14'})
40/4:
for f in os.listdir('./'):
    k = 'west_and_rest'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '41', '2': '42'})
40/5:
for f in os.listdir('./'):
    k = 'on_income_level'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '21', 
                           '2': '22',
                           '3': '23',
                           '4': '24'})
40/6:
for f in os.listdir('./'):
    k = 'wb_income_group'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '31', 
                           '2': '32',
                           '3': '33',
                           '4': '34'})
40/7:
for f in os.listdir('./'):
    k = 'wb_income_group'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '31', 
                           '2': '32',
                           '3': '33',
                           '4': '34'})
                           
        df.to_csv(f)
40/8:
for f in os.listdir('./'):
    k = 'on_income_level'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '21', 
                           '2': '22',
                           '3': '23',
                           '4': '24'})
                           
        
        df.to_csv(f, index=False)
40/9:
for f in os.listdir('./'):
    k = 'on_income_level'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '21', 
                           '2': '22',
                           '3': '23',
                           '4': '24'})
                           
        
        df.to_csv(f, index=False)
40/10:
for f in os.listdir('./'):
    k = 'west_and_rest'
    if 'datapoints' in f and k in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df[k] = df[k].map({'1': '41', '2': '42'})
        
        df.to_csv(f, index=False)
40/11:
for f in os.listdir('./'):
    if 'world_4regions' in f and 'datapoint' in f:
        print(f)
        df = pd.read_csv(f, dtype=str)
        df['world_4regions'] = df['world_4regions'].map({'1': '11', 
                                                         '2': '12', 
                                                         '3': '13', 
                                                         '4': '14'})
        df = df.to_csv(f, index=False)
40/12: df = pd.read_csv('ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv')
40/13: df
40/14: df = pd.read_csv('ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv', dtype=str)
40/15: df = df.loc[:, 1:]
40/16: df = df.iloc[:, 1:]
40/17: df
40/18: df.to_csv('ddf--datapoints--population_smooth--by--wb_income_group--year--income_bracket.csv', index=False)
40/19: df = pd.read_csv('ddf--datapoints--population_percentage_smooth--by--wb_income_group--year--income_bracket.csv', dtype=str)
40/20: df = df.iloc[:, 1:]
40/21: df.to_csv('ddf--datapoints--population_percentage_smooth--by--wb_income_group--year--income_bracket.csv', index=False)
40/22: df
42/1: import pandas as pd
42/2: import os
42/3: df = pd.read_csv('ddf--datapoints--population_smooth--by--on_income_level--year--income_bracket.csv', dtype=str)
42/4: df.head()
42/5: df.on_income_level.unique()
42/6:
df.on_income_level = df.on_income_level.map({'1': '21', '2': '22', '3': '23', '4': '24'}
)
42/7: df
42/8: df.to_csv('ddf--datapoints--population_smooth--by--on_income_level--year--income_bracket.csv', index=False)
42/9: df = pd.read_csv('ddf--datapoints--population_percentage_smooth--by--on_income_level--year--income_bracket.csv', dtype=str)
42/10:
df.on_income_level = df.on_income_level.map({'1': '21', '2': '22', '3': '23', '4': '24'}
)
42/11: df.to_csv('ddf--datapoints--population_percentage_smooth--by--on_income_level--year--income_bracket.csv', index=False)
42/12: df.on_income_level.unique()
42/13: df = pd.read_csv('ddf--datapoints--population_smooth--by--on_income_level--year--income_bracket.csv', dtype=str)
42/14: df
42/15: df = pd.read_csv('ddf--datapoints--population_percentage_smooth--by--on_income_level--year--income_bracket.csv', dtype=str)
42/16: df
42/17:
df.on_income_level = df.on_income_level.map({'1': '21', '2': '22', '3': '23', '4': '24'}
)
42/18: df
42/19: df.to_csv('ddf--datapoints--population_percentage_smooth--by--on_income_level--year--income_bracket.csv', index=False)
45/1: import sys
45/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
45/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
45/4: print (ps_json)
45/5: sys.exit(0)
46/1: import pandas as pd
46/2: import os
46/3: brackets = pd.read_csv(os.path.join(source_dir, 'fixtures', 'brackets.csv'))
46/4: brackets = pd.read_csv(os.path.join('../source', 'fixtures', 'brackets.csv'))
46/5: brackets
46/6: brackets['bracket_start'] + ' - ' + brackets['bracket_end']
46/7: brackets = pd.read_csv(os.path.join('../source', 'fixtures', 'brackets.csv'), dtype=str)
46/8: brackets['bracket_start'] + ' - ' + brackets['bracket_end']
46/9:
coverage_domain = pd.DataFrame.from_records([
        {'coverage_type': 'n',
                 'name': 'National'},
                         {'coverage_type': 'a',
                                  'name': 'Aggregated'},
                                          {'coverage_type': 'R',
                                                   'name': 'Rural'},
                                                           {'coverage_type': 'u',
                                                                    'name': 'Urban'}
                                                                        ])
46/10: coverage_domain
46/11: import codecs, os;__pyfile = codecs.open('''/tmp/pylDRtdP''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pylDRtdP''');exec(compile(__code, '''/tmp/pylDRtdP''', 'exec'));
46/12: import codecs, os;__pyfile = codecs.open('''/tmp/pyGtYcYf''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyGtYcYf''');exec(compile(__code, '''/tmp/pyGtYcYf''', 'exec'));
46/13: import codecs, os;__pyfile = codecs.open('''/tmp/py5lFjPC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py5lFjPC''');exec(compile(__code, '''/tmp/py5lFjPC''', 'exec'));
46/14: import codecs, os;__pyfile = codecs.open('''/tmp/py6coGWl''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py6coGWl''');exec(compile(__code, '''/tmp/py6coGWl''', 'exec'));
46/15: import codecs, os;__pyfile = codecs.open('''/tmp/pym1Z4oT''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pym1Z4oT''');exec(compile(__code, '''/tmp/pym1Z4oT''', 'exec'));
46/16: import codecs, os;__pyfile = codecs.open('''/tmp/pyi9EG94''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyi9EG94''');exec(compile(__code, '''/tmp/pyi9EG94''', 'exec'));
46/17: import codecs, os;__pyfile = codecs.open('''/tmp/py18P6Jn''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py18P6Jn''');exec(compile(__code, '''/tmp/py18P6Jn''', 'exec'));
46/18: import codecs, os;__pyfile = codecs.open('''/tmp/pyd14bmG''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyd14bmG''');exec(compile(__code, '''/tmp/pyd14bmG''', 'exec'));
46/19: import codecs, os;__pyfile = codecs.open('''/tmp/py4EkD3q''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py4EkD3q''');exec(compile(__code, '''/tmp/py4EkD3q''', 'exec'));
46/20: import codecs, os;__pyfile = codecs.open('''/tmp/pyYbO2OB''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyYbO2OB''');exec(compile(__code, '''/tmp/pyYbO2OB''', 'exec'));
46/21: import codecs, os;__pyfile = codecs.open('''/tmp/pylN6TNH''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pylN6TNH''');exec(compile(__code, '''/tmp/pylN6TNH''', 'exec'));
46/22: import codecs, os;__pyfile = codecs.open('''/tmp/pyfmoo03''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyfmoo03''');exec(compile(__code, '''/tmp/pyfmoo03''', 'exec'));
46/23: import codecs, os;__pyfile = codecs.open('''/tmp/pyMJueG9''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyMJueG9''');exec(compile(__code, '''/tmp/pyMJueG9''', 'exec'));
46/24: import codecs, os;__pyfile = codecs.open('''/tmp/pymQpzuI''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pymQpzuI''');exec(compile(__code, '''/tmp/pymQpzuI''', 'exec'));
46/25: df = pd.read_csv('../../ddf--datapoints--population--by--country--year--coverage_type--income_bracket.csv')
46/26: df
46/27: df = df.set_index(['country', 'year' 'coverage_type', 'income_bracket'])
46/28: df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
46/29: df
46/30: import codecs, os;__pyfile = codecs.open('''/tmp/pyiNeiLy''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyiNeiLy''');exec(compile(__code, '''/tmp/pyiNeiLy''', 'exec'));
46/31: df.index.levels[0]
46/32: df.index.levels[0].replace()
46/33: df.index.levels[0].map
46/34: df.index.levels[0].map({'kos': 'xkx'})
46/35: import codecs, os;__pyfile = codecs.open('''/tmp/pyvibQ0y''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyvibQ0y''');exec(compile(__code, '''/tmp/pyvibQ0y''', 'exec'));
46/36: import codecs, os;__pyfile = codecs.open('''/tmp/py8GQFmQ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py8GQFmQ''');exec(compile(__code, '''/tmp/py8GQFmQ''', 'exec'));
46/37: import codecs, os;__pyfile = codecs.open('''/tmp/pykPuZ3E''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pykPuZ3E''');exec(compile(__code, '''/tmp/pykPuZ3E''', 'exec'));
46/38: newvalues = df.index.levels[0].to_list().replace('kos', 'xkx')
49/1: import sys
49/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
49/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
49/4: print (ps_json)
49/5: sys.exit(0)
48/1: import pandas as pd
48/2: import os
48/3: import codecs, os;__pyfile = codecs.open('''/tmp/pyW3GFDn''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyW3GFDn''');exec(compile(__code, '''/tmp/pyW3GFDn''', 'exec'));
48/4: import codecs, os;__pyfile = codecs.open('''/tmp/pyxX8A9I''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyxX8A9I''');exec(compile(__code, '''/tmp/pyxX8A9I''', 'exec'));
48/5: import codecs, os;__pyfile = codecs.open('''/tmp/pysCWvzZ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pysCWvzZ''');exec(compile(__code, '''/tmp/pysCWvzZ''', 'exec'));
48/6: import codecs, os;__pyfile = codecs.open('''/tmp/pyIwJg9Q''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIwJg9Q''');exec(compile(__code, '''/tmp/pyIwJg9Q''', 'exec'));
48/7: import codecs, os;__pyfile = codecs.open('''/tmp/pyt9E0HT''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyt9E0HT''');exec(compile(__code, '''/tmp/pyt9E0HT''', 'exec'));
48/8: import codecs, os;__pyfile = codecs.open('''/tmp/pysIbvVl''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pysIbvVl''');exec(compile(__code, '''/tmp/pysIbvVl''', 'exec'));
48/9: df = pd.read_csv('../../ddf--datapoints--population--by--country--year--coverage_type--income_bracket.csv')
48/10: df
48/11: import codecs, os;__pyfile = codecs.open('''/tmp/pyUSQpJN''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyUSQpJN''');exec(compile(__code, '''/tmp/pyUSQpJN''', 'exec'));
48/12: import codecs, os;__pyfile = codecs.open('''/tmp/pytXU5L5''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pytXU5L5''');exec(compile(__code, '''/tmp/pytXU5L5''', 'exec'));
48/13: import codecs, os;__pyfile = codecs.open('''/tmp/pypqktPn''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pypqktPn''');exec(compile(__code, '''/tmp/pypqktPn''', 'exec'));
48/14: import codecs, os;__pyfile = codecs.open('''/tmp/py4dcS9d''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py4dcS9d''');exec(compile(__code, '''/tmp/py4dcS9d''', 'exec'));
48/15: import codecs, os;__pyfile = codecs.open('''/tmp/py755nVw''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py755nVw''');exec(compile(__code, '''/tmp/py755nVw''', 'exec'));
48/16: import codecs, os;__pyfile = codecs.open('''/tmp/pyoPtHru''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyoPtHru''');exec(compile(__code, '''/tmp/pyoPtHru''', 'exec'));
48/17: import codecs, os;__pyfile = codecs.open('''/tmp/py3BHnVn''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3BHnVn''');exec(compile(__code, '''/tmp/py3BHnVn''', 'exec'));
48/18: import codecs, os;__pyfile = codecs.open('''/tmp/pydweitu''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pydweitu''');exec(compile(__code, '''/tmp/pydweitu''', 'exec'));
48/19: import codecs, os;__pyfile = codecs.open('''/tmp/pyj3XKLu''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyj3XKLu''');exec(compile(__code, '''/tmp/pyj3XKLu''', 'exec'));
48/20: df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
48/21: df
48/22: import codecs, os;__pyfile = codecs.open('''/tmp/pyybUKci''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyybUKci''');exec(compile(__code, '''/tmp/pyybUKci''', 'exec'));
48/23: import codecs, os;__pyfile = codecs.open('''/tmp/pyD8egPv''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyD8egPv''');exec(compile(__code, '''/tmp/pyD8egPv''', 'exec'));
48/24: import codecs, os;__pyfile = codecs.open('''/tmp/pyZXhVwc''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyZXhVwc''');exec(compile(__code, '''/tmp/pyZXhVwc''', 'exec'));
48/25: df.index.levels[0]
48/26: import codecs, os;__pyfile = codecs.open('''/tmp/pyIAB9Zr''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIAB9Zr''');exec(compile(__code, '''/tmp/pyIAB9Zr''', 'exec'));
48/27: import codecs, os;__pyfile = codecs.open('''/tmp/pyatnw2T''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyatnw2T''');exec(compile(__code, '''/tmp/pyatnw2T''', 'exec'));
48/28: import codecs, os;__pyfile = codecs.open('''/tmp/pyWZHYHb''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWZHYHb''');exec(compile(__code, '''/tmp/pyWZHYHb''', 'exec'));
48/29: c = df.index.levels[0]
48/30: import codecs, os;__pyfile = codecs.open('''/tmp/pyWuuk5p''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWuuk5p''');exec(compile(__code, '''/tmp/pyWuuk5p''', 'exec'));
48/31: import codecs, os;__pyfile = codecs.open('''/tmp/pyhjs8bV''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyhjs8bV''');exec(compile(__code, '''/tmp/pyhjs8bV''', 'exec'));
48/32: import codecs, os;__pyfile = codecs.open('''/tmp/pyE6vdJd''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyE6vdJd''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/etl_.py''', 'exec'));
48/33: synonyms = pd.read_csv('../source/fixtures/ddf--open_numbers/ddf--synonyms--geo.csv')
48/34: syn_mapping = synonyms.set_index('synonym')['geo'].to_dict()
48/35: c_ = c.map(syn_mapping)
48/36: c_
48/37: import codecs, os;__pyfile = codecs.open('''/tmp/pyrFFSRE''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyrFFSRE''');exec(compile(__code, '''/tmp/pyrFFSRE''', 'exec'));
48/38: import codecs, os;__pyfile = codecs.open('''/tmp/py6KWcfW''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py6KWcfW''');exec(compile(__code, '''/tmp/py6KWcfW''', 'exec'));
48/39: import codecs, os;__pyfile = codecs.open('''/tmp/pyedKWDX''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyedKWDX''');exec(compile(__code, '''/tmp/pyedKWDX''', 'exec'));
48/40: import codecs, os;__pyfile = codecs.open('''/tmp/pyRCfUq5''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyRCfUq5''');exec(compile(__code, '''/tmp/pyRCfUq5''', 'exec'));
48/41: import codecs, os;__pyfile = codecs.open('''/tmp/pydSfMMX''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pydSfMMX''');exec(compile(__code, '''/tmp/pydSfMMX''', 'exec'));
48/42: import codecs, os;__pyfile = codecs.open('''/tmp/pyW8e4C6''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyW8e4C6''');exec(compile(__code, '''/tmp/pyW8e4C6''', 'exec'));
48/43: syn_mapping = dict([(x, x) for x in synonyms.geo.values])
48/44: syn_mapping['xkx'] = 'kos'
48/45: c_ = c.map(syn_mapping)
48/46: c_
48/47: c_.values
48/48: import codecs, os;__pyfile = codecs.open('''/tmp/pyBTx8DI''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyBTx8DI''');exec(compile(__code, '''/tmp/pyBTx8DI''', 'exec'));
48/49: import codecs, os;__pyfile = codecs.open('''/tmp/pywbp9WQ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pywbp9WQ''');exec(compile(__code, '''/tmp/pywbp9WQ''', 'exec'));
48/50: import codecs, os;__pyfile = codecs.open('''/tmp/py3oCe9q''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3oCe9q''');exec(compile(__code, '''/tmp/py3oCe9q''', 'exec'));
48/51: import codecs, os;__pyfile = codecs.open('''/tmp/pyH6654c''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyH6654c''');exec(compile(__code, '''/tmp/pyH6654c''', 'exec'));
48/52: import codecs, os;__pyfile = codecs.open('''/tmp/pyo82yKx''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyo82yKx''');exec(compile(__code, '''/tmp/pyo82yKx''', 'exec'));
48/53: import codecs, os;__pyfile = codecs.open('''/tmp/pyU7Q6yS''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyU7Q6yS''');exec(compile(__code, '''/tmp/pyU7Q6yS''', 'exec'));
48/54: import codecs, os;__pyfile = codecs.open('''/tmp/pyfceOaw''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyfceOaw''');exec(compile(__code, '''/tmp/pyfceOaw''', 'exec'));
48/55: import codecs, os;__pyfile = codecs.open('''/tmp/pyW4HgNs''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyW4HgNs''');exec(compile(__code, '''/tmp/pyW4HgNs''', 'exec'));
48/56: import codecs, os;__pyfile = codecs.open('''/tmp/pytiqv9K''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pytiqv9K''');exec(compile(__code, '''/tmp/pytiqv9K''', 'exec'));
48/57: import codecs, os;__pyfile = codecs.open('''/tmp/pyQHXwjz''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyQHXwjz''');exec(compile(__code, '''/tmp/pyQHXwjz''', 'exec'));
48/58: import codecs, os;__pyfile = codecs.open('''/tmp/pyM6gFup''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyM6gFup''');exec(compile(__code, '''/tmp/pyM6gFup''', 'exec'));
48/59: import codecs, os;__pyfile = codecs.open('''/tmp/pyeqY28D''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyeqY28D''');exec(compile(__code, '''/tmp/pyeqY28D''', 'exec'));
48/60: c.index.set_level
48/61: import codecs, os;__pyfile = codecs.open('''/tmp/pyW8Pw4C''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyW8Pw4C''');exec(compile(__code, '''/tmp/pyW8Pw4C''', 'exec'));
48/62: import codecs, os;__pyfile = codecs.open('''/tmp/pyCrlqZh''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyCrlqZh''');exec(compile(__code, '''/tmp/pyCrlqZh''', 'exec'));
48/63: import codecs, os;__pyfile = codecs.open('''/tmp/pyxywIuy''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyxywIuy''');exec(compile(__code, '''/tmp/pyxywIuy''', 'exec'));
48/64: country.index.set_level
48/65: import codecs, os;__pyfile = codecs.open('''/tmp/pyTyWoj3''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyTyWoj3''');exec(compile(__code, '''/tmp/pyTyWoj3''', 'exec'));
48/66: import codecs, os;__pyfile = codecs.open('''/tmp/pyLvDCOC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyLvDCOC''');exec(compile(__code, '''/tmp/pyLvDCOC''', 'exec'));
48/67: import codecs, os;__pyfile = codecs.open('''/tmp/pyvRM3pm''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyvRM3pm''');exec(compile(__code, '''/tmp/pyvRM3pm''', 'exec'));
48/68: import codecs, os;__pyfile = codecs.open('''/tmp/pyp1gLGI''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyp1gLGI''');exec(compile(__code, '''/tmp/pyp1gLGI''', 'exec'));
48/69: import codecs, os;__pyfile = codecs.open('''/tmp/pyCC1XI0''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyCC1XI0''');exec(compile(__code, '''/tmp/pyCC1XI0''', 'exec'));
48/70: import codecs, os;__pyfile = codecs.open('''/tmp/pypzSuYJ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pypzSuYJ''');exec(compile(__code, '''/tmp/pypzSuYJ''', 'exec'));
48/71: df.index.set_level
48/72: df.index.set_levels
48/73: df.index.set_levels?
48/74: df.index.set_levels(c_, level='country')
48/75: df.index = df.index.set_levels(c_, level='country')
48/76: df
48/77: df = df.reset_index()
48/78: df.country.hasnans
48/79: df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
48/80: df
48/81: import codecs, os;__pyfile = codecs.open('''/tmp/pyQ6No8a''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyQ6No8a''');exec(compile(__code, '''/tmp/pyQ6No8a''', 'exec'));
48/82: import codecs, os;__pyfile = codecs.open('''/tmp/pyn4HaFi''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyn4HaFi''');exec(compile(__code, '''/tmp/pyn4HaFi''', 'exec'));
48/83: import codecs, os;__pyfile = codecs.open('''/tmp/pylZEJKb''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pylZEJKb''');exec(compile(__code, '''/tmp/pylZEJKb''', 'exec'));
48/84: import codecs, os;__pyfile = codecs.open('''/tmp/pyfxiOBm''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyfxiOBm''');exec(compile(__code, '''/tmp/pyfxiOBm''', 'exec'));
48/85: import codecs, os;__pyfile = codecs.open('''/tmp/pyPCEkhd''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyPCEkhd''');exec(compile(__code, '''/tmp/pyPCEkhd''', 'exec'));
48/86: import codecs, os;__pyfile = codecs.open('''/tmp/py2hPeCF''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py2hPeCF''');exec(compile(__code, '''/tmp/py2hPeCF''', 'exec'));
48/87: import codecs, os;__pyfile = codecs.open('''/tmp/pynrrsxT''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pynrrsxT''');exec(compile(__code, '''/tmp/pynrrsxT''', 'exec'));
48/88: import codecs, os;__pyfile = codecs.open('''/tmp/pyWwmzRm''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWwmzRm''');exec(compile(__code, '''/tmp/pyWwmzRm''', 'exec'));
48/89: import codecs, os;__pyfile = codecs.open('''/tmp/pyoS4hWM''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyoS4hWM''');exec(compile(__code, '''/tmp/pyoS4hWM''', 'exec'));
48/90: import codecs, os;__pyfile = codecs.open('''/tmp/pyTdaLF9''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyTdaLF9''');exec(compile(__code, '''/tmp/pyTdaLF9''', 'exec'));
48/91: import codecs, os;__pyfile = codecs.open('''/tmp/pyGzBlE1''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyGzBlE1''');exec(compile(__code, '''/tmp/pyGzBlE1''', 'exec'));
48/92: import codecs, os;__pyfile = codecs.open('''/tmp/pyqKewAY''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyqKewAY''');exec(compile(__code, '''/tmp/pyqKewAY''', 'exec'));
48/93: import codecs, os;__pyfile = codecs.open('''/tmp/pyr3huqF''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyr3huqF''');exec(compile(__code, '''/tmp/pyr3huqF''', 'exec'));
48/94: import codecs, os;__pyfile = codecs.open('''/tmp/pyKzdhdd''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyKzdhdd''');exec(compile(__code, '''/tmp/pyKzdhdd''', 'exec'));
48/95: import codecs, os;__pyfile = codecs.open('''/tmp/pyiSi7Fu''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyiSi7Fu''');exec(compile(__code, '''/tmp/pyiSi7Fu''', 'exec'));
48/96: import codecs, os;__pyfile = codecs.open('''/tmp/pykIQ707''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pykIQ707''');exec(compile(__code, '''/tmp/pykIQ707''', 'exec'));
48/97: import codecs, os;__pyfile = codecs.open('''/tmp/pyXdPQ2r''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyXdPQ2r''');exec(compile(__code, '''/tmp/pyXdPQ2r''', 'exec'));
48/98: import codecs, os;__pyfile = codecs.open('''/tmp/pyAwLHsH''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyAwLHsH''');exec(compile(__code, '''/tmp/pyAwLHsH''', 'exec'));
48/99: df = pd.read_csv('../../ddf--entities--income_bracket.csv', dtype=str)
48/100: df
48/101: ser = df['name'] + ' - ' + df['name'].shift(1)
48/102: ser
48/103: ser = df['name'] + ' - ' + df['name'].shift(-1)
48/104: ser
48/105: import codecs, os;__pyfile = codecs.open('''/tmp/pyKeJis7''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyKeJis7''');exec(compile(__code, '''/tmp/pyKeJis7''', 'exec'));
48/106: import codecs, os;__pyfile = codecs.open('''/tmp/pySuJeHm''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pySuJeHm''');exec(compile(__code, '''/tmp/pySuJeHm''', 'exec'));
48/107: import codecs, os;__pyfile = codecs.open('''/tmp/pyqMp1qz''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyqMp1qz''');exec(compile(__code, '''/tmp/pyqMp1qz''', 'exec'));
48/108: import codecs, os;__pyfile = codecs.open('''/tmp/pyVvYYqc''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyVvYYqc''');exec(compile(__code, '''/tmp/pyVvYYqc''', 'exec'));
48/109: import codecs, os;__pyfile = codecs.open('''/tmp/pyM9J5RG''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyM9J5RG''');exec(compile(__code, '''/tmp/pyM9J5RG''', 'exec'));
48/110: ser2 = ser.iloc[::2]
48/111: ser
48/112: ser2
48/113: import codecs, os;__pyfile = codecs.open('''/tmp/pysrptRn''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pysrptRn''');exec(compile(__code, '''/tmp/pysrptRn''', 'exec'));
48/114: import codecs, os;__pyfile = codecs.open('''/tmp/pyqbs8Ye''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyqbs8Ye''');exec(compile(__code, '''/tmp/pyqbs8Ye''', 'exec'));
48/115: import codecs, os;__pyfile = codecs.open('''/tmp/pyoH4Zpz''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyoH4Zpz''');exec(compile(__code, '''/tmp/pyoH4Zpz''', 'exec'));
48/116: import codecs, os;__pyfile = codecs.open('''/tmp/pyn7RAOU''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyn7RAOU''');exec(compile(__code, '''/tmp/pyn7RAOU''', 'exec'));
48/117: import codecs, os;__pyfile = codecs.open('''/tmp/pyb74dt7''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyb74dt7''');exec(compile(__code, '''/tmp/pyb74dt7''', 'exec'));
48/118: import codecs, os;__pyfile = codecs.open('''/tmp/pyYE1GrC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyYE1GrC''');exec(compile(__code, '''/tmp/pyYE1GrC''', 'exec'));
48/119: ser.map(lambda x: ' - '.join(x.split(' - ')[0], x.split(' - ')[-1]))
48/120: import codecs, os;__pyfile = codecs.open('''/tmp/pyz65M6z''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyz65M6z''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/121: f(ser[0])
48/122: ser[0]
48/123: import codecs, os;__pyfile = codecs.open('''/tmp/pyo3NzXY''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyo3NzXY''');exec(compile(__code, '''/tmp/pyo3NzXY''', 'exec'));
48/124: import codecs, os;__pyfile = codecs.open('''/tmp/pycACPFw''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pycACPFw''');exec(compile(__code, '''/tmp/pycACPFw''', 'exec'));
48/125: import codecs, os;__pyfile = codecs.open('''/tmp/pycPzf1b''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pycPzf1b''');exec(compile(__code, '''/tmp/pycPzf1b''', 'exec'));
48/126: import codecs, os;__pyfile = codecs.open('''/tmp/pylBIfEW''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pylBIfEW''');exec(compile(__code, '''/tmp/pylBIfEW''', 'exec'));
48/127: ser2.map(f)
48/128: df2 = ser2.map(f).reset_index(drop=True)
48/129: df2
48/130: import codecs, os;__pyfile = codecs.open('''/tmp/py9ZsY00''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py9ZsY00''');exec(compile(__code, '''/tmp/py9ZsY00''', 'exec'));
48/131: import codecs, os;__pyfile = codecs.open('''/tmp/pymUqYec''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pymUqYec''');exec(compile(__code, '''/tmp/pymUqYec''', 'exec'));
48/132: import codecs, os;__pyfile = codecs.open('''/tmp/pyl1FDS3''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyl1FDS3''');exec(compile(__code, '''/tmp/pyl1FDS3''', 'exec'));
48/133: df2 = df2.reset_index()
48/134: df2.columns = ['income_bracket_50', 'name']
48/135: df2.to_csv('../../ddf--entities--income_bracket_50.csv', index=False)
48/136: df = pd.read_csv('../../ddf--datapoints--population--by--country--year--income_bracket.csv', dtype=str)
48/137: df
48/138: import codecs, os;__pyfile = codecs.open('''/tmp/pyp6HHzJ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyp6HHzJ''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/139: import codecs, os;__pyfile = codecs.open('''/tmp/pyzgHmxq''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyzgHmxq''');exec(compile(__code, '''/tmp/pyzgHmxq''', 'exec'));
48/140: import codecs, os;__pyfile = codecs.open('''/tmp/pymK0Kmz''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pymK0Kmz''');exec(compile(__code, '''/tmp/pymK0Kmz''', 'exec'));
48/141: import codecs, os;__pyfile = codecs.open('''/tmp/pyGrere0''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyGrere0''');exec(compile(__code, '''/tmp/pyGrere0''', 'exec'));
48/142: import codecs, os;__pyfile = codecs.open('''/tmp/pyWzVf0S''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWzVf0S''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/143: df2 = df.groupby(['country', 'year']).apply(f2)
48/144: df2 = df.groupby(['country', 'year']).agg(f2)
48/145: import codecs, os;__pyfile = codecs.open('''/tmp/pygV90q6''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pygV90q6''');exec(compile(__code, '''/tmp/pygV90q6''', 'exec'));
48/146: import codecs, os;__pyfile = codecs.open('''/tmp/py0e5wvh''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py0e5wvh''');exec(compile(__code, '''/tmp/py0e5wvh''', 'exec'));
48/147: import codecs, os;__pyfile = codecs.open('''/tmp/pyAo01VF''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyAo01VF''');exec(compile(__code, '''/tmp/pyAo01VF''', 'exec'));
48/148: df2 = df.groupby(['country', 'year'])['population'].agg(f2)
48/149: import codecs, os;__pyfile = codecs.open('''/tmp/py2b8iXh''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py2b8iXh''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/150: df2 = df.groupby(['country', 'year'])['population'].agg(f2)
48/151: df2 = df.groupby(['country', 'year'])['population'].apply(f2)
48/152: import codecs, os;__pyfile = codecs.open('''/tmp/pyBNdxFj''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyBNdxFj''');exec(compile(__code, '''/tmp/pyBNdxFj''', 'exec'));
48/153: import codecs, os;__pyfile = codecs.open('''/tmp/pyzirfHc''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyzirfHc''');exec(compile(__code, '''/tmp/pyzirfHc''', 'exec'));
48/154: import codecs, os;__pyfile = codecs.open('''/tmp/py2SQLzY''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py2SQLzY''');exec(compile(__code, '''/tmp/py2SQLzY''', 'exec'));
48/155: df2
48/156: import codecs, os;__pyfile = codecs.open('''/tmp/pyOBwkEL''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyOBwkEL''');exec(compile(__code, '''/tmp/pyOBwkEL''', 'exec'));
48/157: import codecs, os;__pyfile = codecs.open('''/tmp/pyWFsTA2''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWFsTA2''');exec(compile(__code, '''/tmp/pyWFsTA2''', 'exec'));
48/158: import codecs, os;__pyfile = codecs.open('''/tmp/pyGDBSJQ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyGDBSJQ''');exec(compile(__code, '''/tmp/pyGDBSJQ''', 'exec'));
48/159: import codecs, os;__pyfile = codecs.open('''/tmp/pySKxzM3''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pySKxzM3''');exec(compile(__code, '''/tmp/pySKxzM3''', 'exec'));
48/160: import codecs, os;__pyfile = codecs.open('''/tmp/pya17vt8''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pya17vt8''');exec(compile(__code, '''/tmp/pya17vt8''', 'exec'));
48/161: import codecs, os;__pyfile = codecs.open('''/tmp/pyfUl8yV''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyfUl8yV''');exec(compile(__code, '''/tmp/pyfUl8yV''', 'exec'));
48/162: import codecs, os;__pyfile = codecs.open('''/tmp/pyqGkvpT''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyqGkvpT''');exec(compile(__code, '''/tmp/pyqGkvpT''', 'exec'));
48/163: df.dtypes
48/164: df['population'] = df['population'].astype(float)
48/165: df2 = df.groupby(['country', 'year'])['population'].apply(f2)
48/166: df2.dtypes
48/167: df2
48/168: import codecs, os;__pyfile = codecs.open('''/tmp/pyFpoD8z''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyFpoD8z''');exec(compile(__code, '''/tmp/pyFpoD8z''', 'exec'));
48/169: import codecs, os;__pyfile = codecs.open('''/tmp/pyG1j6lh''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyG1j6lh''');exec(compile(__code, '''/tmp/pyG1j6lh''', 'exec'));
48/170: import codecs, os;__pyfile = codecs.open('''/tmp/pyrosv6Q''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyrosv6Q''');exec(compile(__code, '''/tmp/pyrosv6Q''', 'exec'));
48/171: df.loc[('ago', '1981', '0':'6')]
48/172: df.loc[('ago', '1981', '0')]
48/173: df2.loc[('ago', '1981')]
48/174: df.iloc[:100]
48/175: df.iloc[:20]
48/176: from ddf_utils.str import format_float_digits
48/177: from functool import partial
48/178: from functools import partial
48/179: import codecs, os;__pyfile = codecs.open('''/tmp/pyQpx8Lp''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyQpx8Lp''');exec(compile(__code, '''/tmp/pyQpx8Lp''', 'exec'));
48/180: import codecs, os;__pyfile = codecs.open('''/tmp/pynAtuJg''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pynAtuJg''');exec(compile(__code, '''/tmp/pynAtuJg''', 'exec'));
48/181: formattor = partial(format_float_digits, 6)
48/182: df2.to_csv('../../ddf--datapoints--population--by--country--year--income_bracket_50.csv')
48/183: df2['population'] = df2['population'].map(formattor)
48/184: df2
48/185: df2 = df2.map(formattor)
48/186: import codecs, os;__pyfile = codecs.open('''/tmp/pyeVOg2v''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyeVOg2v''');exec(compile(__code, '''/tmp/pyeVOg2v''', 'exec'));
48/187: import codecs, os;__pyfile = codecs.open('''/tmp/py9akh8C''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py9akh8C''');exec(compile(__code, '''/tmp/py9akh8C''', 'exec'));
48/188: import codecs, os;__pyfile = codecs.open('''/tmp/pyrjO9Ey''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyrjO9Ey''');exec(compile(__code, '''/tmp/pyrjO9Ey''', 'exec'));
48/189: df2.hasnans
48/190: import codecs, os;__pyfile = codecs.open('''/tmp/pyJ6cVb1''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyJ6cVb1''');exec(compile(__code, '''/tmp/pyJ6cVb1''', 'exec'));
48/191: import codecs, os;__pyfile = codecs.open('''/tmp/pyWkqpmc''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWkqpmc''');exec(compile(__code, '''/tmp/pyWkqpmc''', 'exec'));
48/192: import codecs, os;__pyfile = codecs.open('''/tmp/pylEsdtX''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pylEsdtX''');exec(compile(__code, '''/tmp/pylEsdtX''', 'exec'));
48/193: import codecs, os;__pyfile = codecs.open('''/tmp/py3kLJlt''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3kLJlt''');exec(compile(__code, '''/tmp/py3kLJlt''', 'exec'));
48/194: import codecs, os;__pyfile = codecs.open('''/tmp/pynyp4nX''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pynyp4nX''');exec(compile(__code, '''/tmp/pynyp4nX''', 'exec'));
48/195: import codecs, os;__pyfile = codecs.open('''/tmp/pyEhiymL''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyEhiymL''');exec(compile(__code, '''/tmp/pyEhiymL''', 'exec'));
48/196:
for x in df2:
    format_float_digits(x, 6)
48/197: formattor = partial(format_float_digits, digits=6)
48/198: df2 = df2.map(formattor)
48/199: import codecs, os;__pyfile = codecs.open('''/tmp/py5xZPXO''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py5xZPXO''');exec(compile(__code, '''/tmp/py5xZPXO''', 'exec'));
48/200: import codecs, os;__pyfile = codecs.open('''/tmp/pyDwVPhZ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyDwVPhZ''');exec(compile(__code, '''/tmp/pyDwVPhZ''', 'exec'));
48/201: import codecs, os;__pyfile = codecs.open('''/tmp/pyS0RoZR''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyS0RoZR''');exec(compile(__code, '''/tmp/pyS0RoZR''', 'exec'));
48/202: df2.to_csv('../../ddf--datapoints--population--by--country--year--income_bracket_50.csv')
48/203: df2 = df2.astype(int)
48/204: df2 = df2.astype(float)
48/205: df2 = df2.astype(int)
48/206: df2
48/207: df2.to_csv('../../ddf--datapoints--population--by--country--year--income_bracket_50.csv')
48/208: import codecs, os;__pyfile = codecs.open('''/tmp/pyeDgYYD''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyeDgYYD''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/209: import codecs, os;__pyfile = codecs.open('''/tmp/pyPFSiAs''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyPFSiAs''');exec(compile(__code, '''/tmp/pyPFSiAs''', 'exec'));
48/210: import codecs, os;__pyfile = codecs.open('''/tmp/pyjDsmBU''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyjDsmBU''');exec(compile(__code, '''/tmp/pyjDsmBU''', 'exec'));
48/211: import codecs, os;__pyfile = codecs.open('''/tmp/py3piOjj''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3piOjj''');exec(compile(__code, '''/tmp/py3piOjj''', 'exec'));
48/212: import codecs, os;__pyfile = codecs.open('''/tmp/pydSCe7V''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pydSCe7V''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/213: res = process('population_smooth', 'country')
48/214: import codecs, os;__pyfile = codecs.open('''/tmp/pyEWuKh3''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyEWuKh3''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/215: res = process('population_smooth', 'country')
48/216: res
48/217: res = process('population_smooth', 'on_income_level')
48/218: res = process('population_smooth', 'wb_income_group')
48/219: res
48/220: res = process('population_smooth', 'west_and_rest')
48/221: res = process('population_smooth', 'world_4regions')
48/222: import codecs, os;__pyfile = codecs.open('''/tmp/pyUTwBKl''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyUTwBKl''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/223: res = process('population_smooth', 'world_4regions')
48/224: res = process('population_smooth', 'west_and_rest')
48/225: res = process('population_smooth', 'wb_income_group')
48/226: res = process('population_smooth', 'on_income_level')
48/227: import codecs, os;__pyfile = codecs.open('''/tmp/pyJsOFAJ''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyJsOFAJ''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/228: import codecs, os;__pyfile = codecs.open('''/tmp/pyIAk1T7''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIAk1T7''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/229: res = process('population_smooth', 'country')
48/230: res
48/231: import codecs, os;__pyfile = codecs.open('''/tmp/pykzfkRB''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pykzfkRB''');exec(compile(__code, '''/tmp/pykzfkRB''', 'exec'));
48/232: import codecs, os;__pyfile = codecs.open('''/tmp/py7pUwTG''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py7pUwTG''');exec(compile(__code, '''/tmp/py7pUwTG''', 'exec'));
48/233: import codecs, os;__pyfile = codecs.open('''/tmp/py3oUnYP''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py3oUnYP''');exec(compile(__code, '''/tmp/py3oUnYP''', 'exec'));
48/234: import codecs, os;__pyfile = codecs.open('''/tmp/pyVb2WmF''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyVb2WmF''');exec(compile(__code, '''/tmp/pyVb2WmF''', 'exec'));
48/235: import codecs, os;__pyfile = codecs.open('''/tmp/pydGJLdI''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pydGJLdI''');exec(compile(__code, '''/tmp/pydGJLdI''', 'exec'));
48/236: import codecs, os;__pyfile = codecs.open('''/tmp/py4WdVH0''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py4WdVH0''');exec(compile(__code, '''/tmp/py4WdVH0''', 'exec'));
48/237: res.grorupby(['country', 'year']).agg(lambda x: x.astype(str).cat(x, sep=','))
48/238: res.reset_index().grorupby(['country', 'year']).agg(lambda x: x.astype(str).cat(x, sep=','))
48/239: res.groupby(['country', 'year']).agg(lambda x: x.astype(str).cat(x, sep=','))
48/240: import codecs, os;__pyfile = codecs.open('''/tmp/pyWUm2ph''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWUm2ph''');exec(compile(__code, '''/tmp/pyWUm2ph''', 'exec'));
48/241: res.groupby(['country', 'year']).agg(lambda x: x.astype(str).str.cat(x, sep=','))
48/242: res[:10]
48/243: import codecs, os;__pyfile = codecs.open('''/tmp/pyH8zcfC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyH8zcfC''');exec(compile(__code, '''/tmp/pyH8zcfC''', 'exec'));
48/244: import codecs, os;__pyfile = codecs.open('''/tmp/pynYDdV3''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pynYDdV3''');exec(compile(__code, '''/tmp/pynYDdV3''', 'exec'));
48/245: import codecs, os;__pyfile = codecs.open('''/tmp/py4gRwnz''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py4gRwnz''');exec(compile(__code, '''/tmp/py4gRwnz''', 'exec'));
48/246: res[:10].str.cat(sep=',')
48/247: res[:10].astype(str).str.cat(sep=',')
48/248: res.groupby(['country', 'year'])['population_smooth'].agg(lambda x: x.astype(str).str.cat(x, sep=','))
48/249: res.reset_index().groupby(['country', 'year'])['population_smooth'].agg(lambda x: x.astype(str).str.cat(x, sep=','))
48/250: res..groupby(['country', 'year'])['population_smooth'].agg(lambda x: x.astype(str).str.cat(sep=','))
48/251: res.groupby(['country', 'year'])['population_smooth'].agg(lambda x: x.astype(str).str.cat(sep=','))
48/252: res.groupby(['country', 'year']).agg(lambda x: x.astype(str).str.cat(sep=','))
48/253: res2 = res.groupby(['country', 'year']).agg(lambda x: x.astype(str).str.cat(sep=','))
48/254: res.name = 'income_mountain_50bracket_shape_for_log'
48/255: res2.name = 'income_mountain_50bracket_shape_for_log'
48/256: import codecs, os;__pyfile = codecs.open('''/tmp/pyR5J7yA''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyR5J7yA''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/257: import codecs, os;__pyfile = codecs.open('''/tmp/pyKnc1ZN''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyKnc1ZN''');exec(compile(__code, '''/tmp/pyKnc1ZN''', 'exec'));
48/258: import codecs, os;__pyfile = codecs.open('''/tmp/pyxPvAH8''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyxPvAH8''');exec(compile(__code, '''/tmp/pyxPvAH8''', 'exec'));
48/259: import codecs, os;__pyfile = codecs.open('''/tmp/pyWnS8yV''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyWnS8yV''');exec(compile(__code, '''/tmp/pyWnS8yV''', 'exec'));
48/260: res = process_2('population_smooth', 'income_bracket', 'country')
48/261: res
48/262: import codecs, os;__pyfile = codecs.open('''/tmp/pyZqcVbX''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyZqcVbX''');exec(compile(__code, '''/home/semio/src/work/gapminder/datasets/repo/github.com/open-numbers/ddf--worldbank--povcalnet/etl/scripts/fiftybracket.py''', 'exec'));
48/263: res = process_2('population_smooth', 'income_bracket', 'country')
48/264: res
48/265: df = pd.read_csv('../../ddf--datapoints--population_smooth--by--country--year--income_bracket.csv')
48/266: df
48/267: res = process_2('population_smooth', 'income_bracket', 'world_4regions')
48/268: res
48/269: res = process_2('population_smooth', 'income_bracket', 'west_and_rest')
48/270: res = process_2('population_smooth', 'income_bracket', 'on_income_level')
48/271: res = process_2('population_smooth', 'income_bracket', 'wb_income_group')
48/272: res = process_2('population_smooth', 'income_bracket_50', 'wb_income_group')
48/273: res = process_2('population_smooth', 'income_bracket_50', 'on_income_level')
48/274: res = process_2('population_smooth', 'income_bracket_50', 'west_and_rest')
48/275: import codecs, os;__pyfile = codecs.open('''/tmp/pyHamWEb''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyHamWEb''');exec(compile(__code, '''/tmp/pyHamWEb''', 'exec'));
48/276: import codecs, os;__pyfile = codecs.open('''/tmp/py8QMczu''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/py8QMczu''');exec(compile(__code, '''/tmp/py8QMczu''', 'exec'));
48/277: import codecs, os;__pyfile = codecs.open('''/tmp/pyd1o0CC''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyd1o0CC''');exec(compile(__code, '''/tmp/pyd1o0CC''', 'exec'));
48/278: res = process_2('population_smooth', 'income_bracket_50', 'world_4regions')
48/279: res = process_2('population_smooth', 'income_bracket_50', 'country')
54/1: import pandas as pd
54/2: unhcr = pd.read_csv('/home/semio/Downloads/_f_UNHCR-regions-file - UNHCR-regions.csv', dtype=str)
54/3: unhcr
54/4: unhcr.columns = ['iso3', 'UNSD short name', 'UNHCR Region name']
54/5: synonyms = pd.read_csv('ddf--synonyms--geo.csv', dtype=str)
54/6: unhcr['geo'] = unhcr['iso3'].str.upper().map(mapping)
54/7: mapping = synonyms.set_index('synonym')['geo'].to_dict()
54/8: unhcr['geo'] = unhcr['iso3'].str.upper().map(mapping)
54/9: unhcr
54/10: unhcr['geo'].hasnans
54/11: unhcr[pd.isnull(unhcr.geo)]
48/280: import codecs, os;__pyfile = codecs.open('''/tmp/pyIQRUm8''', encoding='''utf-8''');__code = __pyfile.read().encode('''utf-8''');__pyfile.close();os.remove('''/tmp/pyIQRUm8''');exec(compile(__code, '''/tmp/pyIQRUm8''', 'exec'));
54/12: synonyms = pd.read_csv('ddf--synonyms--geo.csv', dtype=str)
54/13: mapping = synonyms.set_index('synonym')['geo'].to_dict()
54/14: unhcr['geo'] = unhcr['iso3'].str.upper().map(mapping)
54/15: unhcr['geo'].hasnans
54/16: unhcr['UNHCR Region name'].unique()
54/17: d = unhcr['UNHCR Region name'].unique().to_list()
54/18: d = unhcr['UNHCR Region name'].unique().tolist()
54/19: d
54/20: v = ['unhcr_asia_pacific', 'unhcr_europe', 'unhcr_middle_east_north_africa', 'unhcr_southern_africa', 'unhcr_americas', 'unhcr_west_central_africa', 'unhcr_east_horn_africa_great_lates']
54/21: d1 = dict(zip(d, v))
54/22: d1
54/23: d2 = dict(zip(v, d))
54/24: d2
54/25: pd.DataFrame.from_dict(d2)
54/26: pd.Series.from_dict(d2)
54/27: pd.Series(d2)
54/28: ent = pd.Series(d2)
54/29: ent.name = ['unhcr_region']
54/30: ent.name = 'unhcr_region'
54/31: ent.to_csv('./ddf--entities--geo--unhcr_region.csv')
54/32: unhcr['unhcr_region'] = unhcr['UNHCR Region name'].map(d1)
54/33: unhcr
54/34: ent.index.name = 'unhcr_region'
54/35: ent.name = 'name'
54/36: ent.to_csv('./ddf--entities--geo--unhcr_region.csv')
54/37: country = pd.read_csv('ddf--entities--geo--country.csv', dtype=str, default_nas=False)
54/38: country = pd.read_csv('ddf--entities--geo--country.csv', dtype=str, default_na=False)
54/39: country = pd.read_csv('ddf--entities--geo--country.csv', dtype=str, na_values=None)
54/40: country
54/41: country['unhcr_regions'] = country['country'].map(d3)
54/42: df = unhcr.set_index('geo')['unhcr_region'].to_dict()
54/43: d3 = unhcr.set_index('geo')['unhcr_region'].to_dict()
54/44: country['unhcr_regions'] = country['country'].map(d3)
54/45: country
54/46: country.to_csv('ddf--entities--geo--country.csv', index=False)
71/1: import pandas as pd
71/2: pd.read_csv('http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine=1.2&Countries=CHN&display=C')
71/3: df = pd.read_csv('http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine=1.2&Countries=CHN&display=C')
71/4: df
71/5: df[['RequestYear', 'Mean', "Median"]]
71/6: df = pd.read_csv('http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine=1.2&Countries=USA&display=C')
71/7: df[['RequestYear', 'Mean', "Median"]]
71/8: (2292 - 2137) / 2
71/9: (2292 - 2137) / 2137
71/10: (448 - 388) / 388
72/1: import pandas as pd
72/2: df = pd.read_csv('0000.csv')
72/3: df
72/4: df = df.set_index(['CountryCode', 'RequestYear'])
72/5: df.loc[('AGO', 2018)]
72/6: df.loc[('AGO', 2018), ['Mean', 'Median']]
72/7: df.loc[('ZWE', 2018), ['Mean', 'Median']]
72/8: df[df['Median'] == -1]
72/9: df[df['Mean'] == -1]
72/10: df[df['Mean'] == -1]['CoverageType'].unique()
72/11: df[df['Median'] == -1]['CoverageType'].unique()
72/12: import random
72/13: random.choice([23, 24])
74/1: import pandas as pd
74/2: df = pd.read_csv('0000.csv')
74/3: df
74/4: df.head()
74/5: df2 = pd.read_csv('0001.csv')
74/6: df['Mean'] == df2['Mean']
74/7: import numpy as np
74/8: np.all(df['Mean'] == df2['Mean'])
74/9: df[~(df['Mean'] == df2['Mean'])]
74/10: df[~(df['Mean'] == df2['Mean'])]['Mean']
74/11: df2[~(df['Mean'] == df2['Mean'])]['Mean']
74/12: np.all(df['Mean'].dropna() == df2['Mean'].dropna())
74/13: df
74/14: synonyms = pd.read_csv('../source/fixtures/ddf--open_numbers/ddf--synonyms--geo.csv')
74/15: countrydict = synonyms.set_index('synonym')['geo'].to_dict()
74/16: df = df[['CountryCode', 'RequestYear', 'Mean', 'Median']]
74/17: df
74/18: df = pd.read_csv('0000.csv')
74/19: df2 = df[['CountryCode', 'RequestYear', 'CoverageType', 'Mean', 'Median']]
74/20: df2
74/21: df2['geo'] = df2['CountryCode'].map(countrydict)
74/22: df2 = df[['CountryCode', 'RequestYear', 'CoverageType', 'Mean', 'Median']].copy()
74/23: df2['geo'] = df2['CountryCode'].map(countrydict)
74/24: df2
74/25: df2.geo.hanans
74/26: df2['geo'].hasnans
74/27: df2[pd.isnull(df2['geo'])]
74/28: df2[pd.isnull(df2['geo'])]['CountryCode'].unique()
74/29: synonyms = pd.read_csv('../source/fixtures/ddf--open_numbers/ddf--synonyms--geo.csv')
74/30: countrydict = synonyms.set_index('synonym')['geo'].to_dict()
74/31: df2['geo'] = df2['CountryCode'].map(countrydict)
74/32: df2['geo'].hasnans
74/33: df2[pd.isnull(df2['geo'])]
74/34: df2 = df[['CountryCode', 'CountryName', 'RequestYear', 'CoverageType', 'Mean', 'Median']].copy()
74/35: df2
74/36: df2['geo'] = df2['CountryName'].map(countrydict)
74/37: df2
74/38: df2[pd.isnull(df2['geo'])]
74/39: df2
74/40: df2 = df2.drop(['CountryCode', 'CountryName'], axis=1)
74/41: df2
74/42: df2.columns = ['year', 'coverage_type', 'mean', 'median', 'geo']
74/43: df2
74/44: df2['coverage_type'] = df2['coverage_type'].str.lower()
74/45: df2
74/46: mean = df2.set_index(['geo', 'year', 'coverage_type'])['mean']
74/47: mean
74/48: mean.dropna()
74/49: mean = mean.dropna()
74/50: mean.name = 'mean_income'
74/51: mean.to_csv('../../ddf--datapoints--mean_income--by--geo--year--coverage_type.csv')
74/52: df = pd.read_csv('0000.csv')
74/53: df['CountryCode'].hasnans
74/54: !ls
74/55: !ls ../../
74/56: q
75/1: import pandas as pd
75/2: import numpy as np
75/3: df = pd.read_csv('ddf--datapoints--mean_income--by--country--year.csv')
75/4: df.head()
75/5: gdp = pd.read_csv('../ddf--gapminder--gdp_per_capita_cppp/ddf--datapoints--income_per_person_gdppercapita_ppp_inflation_adjusted--by--geo--time.csv')
75/6: gdp.head()
75/7: gdp = gdp.set_index(['geo', 'time'])['income_per_person_gdppercapita_ppp_inflation_adjusted']
75/8: gdp
75/9: gdp.loc['afg']
75/10: df = df.set_index(['country', 'year'])
75/11: df = df['mean_income']
75/12: df
75/13: df.loc['afg']
75/14: df.loc['ago']
75/15: gdp.loc['ago']
75/16: df.index[0]
75/17: df.loc['ago'].index[0]
75/18: df.loc['ago'].index[-1]
75/19: income = df.loc['ago']
75/20: gdppc = gdp.loc['ago']
75/21: gdppc.loc[:income.index[0]]
75/22: b = gdppc.loc[:income.index[0]]
75/23: b.shift(1)
75/24: b / b.shift(1)
75/25: b2 = b / b.shift(1)
75/26: b2 = b2.reverse()
75/27: b2 = b2[::-1]
75/28: b2
75/29: beg = income[1981]
75/30: beg
75/31: b2.reduce
75/32: np.divide.accumulate(b2, initial=beg)
75/33: np.divide.accumulate(b2, init=beg)
75/34: b2[1981] = 1
75/35: np.divide.accumulate(b2)
75/36: np.divide.accumulate(b2) * beg * 12
75/37: b2
75/38: b
75/39: b / b.shift(1)
75/40: 1692 / 1762
75/41: np.divide.accumulate(b2) * beg * 12
75/42: res = np.divide.accumulate(b2) * beg * 12
75/43: res.values
75/44: b2 = b / b.shift(1)
75/45: b2
75/46: b2.shift(-1)
75/47: b2 = b2.shift(-1).fillna(1)
75/48: b2
75/49: b2 = b2[::-1]
75/50: b2
75/51: np.divide?
75/52: np.divide.accumulate(b2) * beg
75/53: np.divide.accumulate(b2) * beg * 12
75/54: df
75/55: df.groupby(['country'])
75/56: q
96/1: import pycurl
96/2: c = pycurl.Curl
96/3: c = pycurl.Curl()
97/1: import pycurl
97/2: c = pycurl.Curl()
97/3: url = 'http://ipv4.download.thinkbroadband.com/20MB.zip'
97/4: c.setopt(c.URL, url)
97/5: c.getinfo(c.HTTP_CODE)
98/1: import git
98/2: git.__version__
102/1: import matplotlib.pyplot as plt
102/2: plt.plot([1, 2, 3])
102/3: plt.plot([1, 2, 3])
102/4: import pandas as pd
102/5: import numpy as np
102/6: pd.DataFrame?
106/1: import pandas as pd
106/2: import matplotlib.pyplot as plt
106/3: import numpy as np
106/4: import math
106/5: math.ln(0.00897 / 0.00781)
106/6: math.lg(0.00897 / 0.00781, 2)
106/7: math.ln(0.00897 / 0.00781, 2)
106/8: math.log10(0.00897 / 0.00781)
106/9: math.log1p(0.00897 / 0.00781)
106/10: math.log(0.00897 / 0.00781)
106/11: math.log2(0.00897 / 0.00781)
106/12: math.e
106/13:
a = [0.00781,
0.00897,
0.01031,
0.01184,
0.01360,
0.01563,
0.01795,]
106/14: s = pd.Series(a)
106/15: s
106/16:
df = pd.DataFrame([s, s.shift(-1)]
)
106/17: df
106/18: df = df.T
106/19: df
106/20: df.assign(t = np.log(df[1] / df[0]))
106/21: df.assign(t = np.log2(df[1] / df[0]))
106/22: # question: what is the actual range of all datapoints?
106/23: df = pd.read_csv('../../ddf--datapoints--population_percentage--by--country--year--income_bracket.csv')
106/24: df.head()
106/25:
df['population_percentage'].describe(0
)
106/26: df['population_percentage'].describe()
106/27: df.groupby('income_bracket')['population_percentage'].min()
106/28: df.groupby('income_bracket')['population_percentage'].max()
106/29: res = df.groupby('income_bracket')['population_percentage'].max()
106/30: res[res == 0]
106/31: brackets = pd.read_csv('../../ddf--entities--income_bracket.csv')
106/32: brackets
106/33: # so the maximum value of first group is 0.003542, and maximum value of last group is 0.000064. Is it desirable?
109/1: import pandas as pd
109/2: import numpy as np
109/3: import matplotlib.pyplot as plt
109/4: y = np.ones(10)
109/5: res = np.logspace(1, 3, 10, endpoint=True)
109/6: print(res)
109/7: np.logspace?
109/8: plt.plot(res, y)
109/9: plt.plot(res, y, stlye='.')
109/10: plt.plot(res, y, style='.')
109/11: plt.scatter(res, y, style='.')
109/12: plt.scatter(res, y)
109/13: res = np.logspace(0.00781, 8192, 100, endpoint=True, base=2)
109/14: res
109/15: np.logspace?
109/16: np.log(0.00781)
109/17: np.log2(0.00781)
109/18: np.log2(8192)
109/19: res = np.logspace(-7, 13, 100, endpoint=True, base=2)
109/20: res
109/21: !ls
109/22: !ls -la
109/23: !ls -lah
109/24: import pandas as pd
109/25: df = pd.read_csv('0199.csv')
109/26: df
109/27: df.columns
109/28:
def load_file_preprocess(filename, income_bracket):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'PovertyLine', 'HeadCount', 'ReqYearPopulation'
    ]
    df = pd.read_csv(os.path.join(source_dir, filename), usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'PovertyLine': 'income_bracket',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df['income_bracket'] = income_bracket
    df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
    return df
109/29: import os
109/30:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        print(fn.strip(0))
109/31:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        print(fn.strip('0'))
109/32: str.strip?
109/33: str.lstrip?
109/34:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        print(fn.lstrip('0'))
109/35: res = dict()
109/36:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = int(fn.lstrip('0'))
        res[bracket] = load_file_preprocess(f, bracket)
109/37:
def load_file_preprocess(filename, income_bracket):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'PovertyLine', 'HeadCount', 'ReqYearPopulation'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'PovertyLine': 'income_bracket',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df['income_bracket'] = income_bracket
    df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
    return df
109/38:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = int(fn.lstrip('0'))
        res[bracket] = load_file_preprocess(f, bracket)
109/39: coverage_type_dtype = pd.CategoricalDtype(list('NAUR'), ordered=True)
109/40:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = int(fn.lstrip('0'))
        res[bracket] = load_file_preprocess(f, bracket)
109/41:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = int(fn.lstrip('0'))
        if bracket == '':
            bracket = 0
        res[bracket] = load_file_preprocess(f, bracket)
109/42:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f, bracket)
109/43: res[0]
109/44:
def load_file_preprocess(filename, income_bracket):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'PovertyLine', 'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'PovertyLine': 'income_bracket',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year',
            'Mean': 'mean_income'
        })
    df['income_bracket'] = income_bracket
    df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
    return df
109/45:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f, bracket)
109/46: res[0]
109/47:
def load_file_preprocess(filename, income_bracket):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'PovertyLine', 'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'PovertyLine': 'income_bracket',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df['income_bracket'] = income_bracket
    df = df.set_index(['country', 'year', 'coverage_type', 'income_bracket'])
    return df
109/48:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f, bracket)
109/49: res[0]
109/50: res[1]
109/51:
def load_file_preprocess(filename):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df = df.set_index(['country', 'year', 'coverage_type'])
    return df
109/52:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f, bracket)
109/53:
for f in os.listdir('./'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f)
109/54: res[0]
109/55: res[2]
109/56: res[2].loc[('IND')]
109/57: df = res[0]
109/58: !mkdir wip
109/59: df['Mean'].to_csv('wip/mean_income-by-country-year-coverage_type.csv')
109/60: df['ReqYearPopulation'].to_csv('wip/total_population-by-country-year-coverage_type.csv')
109/61: brackets
109/62: all_brackets = np.logspace(-7, 13, 200, endpoint=True, base=2)
109/63: all_brackets
109/64: len(res)
109/65: len(all_brackets)
109/66: import math
109/67: math.log2(all_brackets[1]) - math.log2(all_brackets[0])
109/68: math.log2(all_brackets[2]) - math.log2(all_brackets[1])
109/69: brackets = pd.DataFrame({'start': all_brackets, 'end': all_brackets.shift(-1)})
109/70: brackets = pd.DataFrame({'start': all_brackets, 'end': pd.Series(all_brackets).shift(-1)})
109/71: brackets
109/72: brackets.iloc[:-1].to_csv('wip/list-of-brackets.csv')
109/73: brackets.iloc[:-1]
109/74: np.subtract?
109/75: np.info(np.subtract)
109/76: np.nan - 1
109/77: res2 = dict()
109/78:
for i in range(1, 199):
    df1 = res[i]
    df2 = res[i-1]
    df3 = df1['HeadCount'] - df2['HeadCount']
    res2[i-1] = df3
109/79: res2[1]
109/80: res[1]['HeadCount']
109/81: res[1]['HeadCount'].index.isequal
109/82: res[1]['HeadCount'].index.equal
109/83: res[1]['HeadCount'].index.equals
109/84: res[1]['HeadCount'].index.equals(res[2]['HeadCount'].index)
109/85: res2[0]
109/86: res3 = list()
109/87:
for k, v in res2.items():
    df = v.reset_index()
    df['bracket'] = k
    df = df.set_index(['country', 'year', 'coverage_type', 'bracket'])
    res3.append(df)
109/88: res3[0]
109/89: res3[1]
109/90: res3 = pd.concat(res3, axis=1)
109/91: res3 = pd.concat(res3)
109/92: res3
109/93: res2[199]
109/94: res2[198]
109/95:
for i in range(1, 200):
    df1 = res[i]
    df2 = res[i-1]
    df3 = df1['HeadCount'] - df2['HeadCount']
    res2[i-1] = df3
109/96: res3 = list()
109/97:
for k, v in res2.items():
    df = v.reset_index()
    df['bracket'] = k
    df = df.set_index(['country', 'year', 'coverage_type', 'bracket'])
    res3.append(df)
109/98: res4 = pd.concat(res3)
109/99: res4
109/100: res4['HeadCount'] < 0
109/101: res4[res4['HeadCount'] < 0]
109/102: res4.loc[res4['HeadCount'] < 0] = np.nan
109/103: res4[res4['HeadCount'] < 0]
109/104: res4 = res4.dropna()
109/105: res4
109/106: res4 = pd.concat(res3)
109/107: res4
109/108: res4.loc[res4['HeadCount'] < 0] = np.nan
109/109: !mkdir wip/noicymountain/
109/110: bracket
109/111: brackets
109/112: brackets[0]
109/113: brackets.loc[0]
109/114: res4
109/115: res4.to_csv('wip/population_percentage-by-country-year-coverage_type_bracket.csv')
109/116: gs = res4.groupby('country')
109/117: gs.groups
109/118:
for k, idx in gs.groups:
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].to_csv(fname)
109/119:
for k, df in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    df.to_csv(fname)
109/120:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].to_csv(fname)
109/121: res4.sort_index?
109/122:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].sort_index('bracket').to_csv(fname)
109/123:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].sort_index(3).to_csv(fname)
109/124:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].sort_index(level=3).to_csv(fname)
109/125: res4.sort_index?
109/126:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].sort_index(level=['bracket', 'year', 'coverage_type']).to_csv(fname)
109/127:
for k, idx in gs.groups.items():
    fname = os.path.join('wip', 'noisymountain', f'{k}-population_percentage-by-country-year-coverage_type-bracket.csv')
    res4.loc[idx].sort_index(level=['year', 'bracket', 'coverage_type']).to_csv(fname)
109/128: # a function to find the income bracket from mean income
109/129: res4.loc[idx]
109/130: res4.loc[idx].loc[('ZWE', 1981, 'N', 30)]
109/131: res4.loc[idx].loc[('ZWE', 1981, 'N', '30')]
109/132: res4.loc[idx].loc[('ZWE', 1981)]
109/133: res4.loc[idx].loc[('ZWE', 1981, 'N', 0)]
109/134: res4.loc[idx].query('year=1981')
109/135: res4.loc[idx].loc[('ZWE', 1981)]
109/136: res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount']
109/137: res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'].plot()
109/138: plt.plot(brackets['start'], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/139: plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/140: plt.plot?
109/141: plt.semilogx(base=2)
109/142: plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/143: brackets
109/144: plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/145: plt.xscale('log', base=2)
109/146: p = plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/147: p.xscale('log', base=2)
109/148: p.axis.xscale('log', base=2)
109/149: p
109/150: fig, ax = plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/151: ax, = plt.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/152: ax
109/153: ax.xscale
109/154: fig, ax = plt.figure()
109/155: fig= plt.figure()
109/156: ax = fig.add_subplot()
109/157: ax
109/158: ax.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/159: ax.set_xscale('log', base=2)
109/160: ax.show()
109/161: plt.show()
109/162: plt.show(ax)
109/163: plt.show()
109/164: ax = fig.add_subplot(2, 1, 1)
109/165: ax.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/166: plt.show()
109/167: plt.plot.show()
109/168: plt.plot()
109/169: plt.plot(ax)
109/170: a = [ pow(10,i) for i in range(10) ]
109/171: pyplot.subplot(2,1,1)
109/172: plt.subplot(2,1,1)
109/173: plt.subplot(10,1,1)
109/174: plt.subplot(1,1,1)
109/175: pyplot.plot(a, color='blue', lw=2)
109/176: plt.plot(a, color='blue', lw=2)
109/177: plt.yscale('log')
109/178: plt.show()
109/179: %matplotlib qt
109/180: plt.show()
109/181: plt.plot(a, color='blue', lw=2)
109/182: plt.yscale('log')
109/183: plt.show()
109/184: %matplotlib inline
109/185: plt.show()
109/186: plt.plot(a, color='blue', lw=2)
109/187: plt.yscale('log')
109/188: plt.show()
109/189: %matplotlib qt
109/190: fig = plt.figure()
109/191: ax = fig.subplot(2, 1, 1)
109/192: ax = fig.sub_plot(2, 1, 1)
109/193: ax = fig.subplots(2, 1, 1)
109/194: ax = fig.subplots(1, 1, 1)
109/195: ax = fig.subplots()
109/196: ax.plot(brackets['start'][:-1], res4.loc[idx].loc[('ZWE', 1981, 'N')]['HeadCount'])
109/197: fig.show()
109/198: ax.set_xscale('log', base=2)
109/199: fig.show()
109/200: 200 / 6
109/201: # a function to find the income bracket from mean income
109/202:
def bracket_number_from_income(i):
    for n, row in brackets.iterrows():
        if i >= row['start'] and i < row['end']:
            return n
109/203: bracket_number_from_income(1)
109/204: brackets.loc[69]
109/205: mean
109/206: res[0]['Mean']
109/207: mean = res[0]['Mean'].copy()
109/208: bracket_country_time = mean.map(bracket_number_from_income)
109/209: bracket_country_time
109/210: bct1 = np.log2(mean)
109/211: bct1
109/212: brackets
109/213: brackets_log = brackets.loc[:-1].copy()
109/214: # maybe I can find some way to calculate the bracket for a income with a math fomula.
109/215: # let's try it later.
109/216: bracket_country_time.hanans
109/217: bracket_country_time.hasnans
109/218: bct1 - bct1.shift(-1)
109/219: brackets_log
109/220: brackets_log = brackets.iloc[:-1].copy()
109/221: brackets_log
109/222: brackets_log['start'] = np.log2(brackets_log['start'])
109/223: brackets_log['end'] = np.log2(brackets_log['end'])
109/224: brackets_log
109/225: brackets_log['d'] = brackets_log.end - bracket_log.start
109/226: brackets_log['d'] = brackets_log.end - brackets_log.start
109/227: brackets_log
109/228: delta = brackets_log['d'].iloc[0]
109/229: delta
109/230:
def bracket_number_from_income(i):
    return math.floor(math.log2(i) / delta)
109/231: bracket_number_from_income(0.00781)
109/232: bracket_number_from_income(0.007812)
109/233:
def bracket_number_from_income(i):
    return (math.log2(i) + 7) / delta
109/234: bracket_number_from_income(0.007812)
109/235:
def bracket_number_from_income(i):
    return math.floor((math.log2(i) + 7) / delta)
109/236: bracket_number_from_income(0.007812)
109/237:
def bracket_number_from_income(i):
    return int((math.log2(i) + 7) / delta)
109/238: bracket_number_from_income(0.007812)
109/239: bracket_number_from_income(8192)
109/240: bracket_number_from_income(8191)
109/241: bracket_number_from_income(8190)
109/242: int(0.5)
109/243: int(0.9)
109/244: bracket_number_from_income(0.008376)
109/245: bracket_number_from_income(0.008377)
109/246: bracket_number_from_income(0.0083765)
109/247: bracket_number_from_income(brackets['start'].iloc[1])
109/248: bracket_number_from_income(brackets['start'].iloc[0])
109/249: bracket_number_from_income(brackets['start'].iloc[2])
109/250: bracket_number_from_income(brackets['start'].iloc[3])
109/251: np.int(1.5)
109/252:
def bracket_number_from_income(i):
    return int((np.log2(i) + 7) / delta)
109/253: mean
109/254: bracket_country_time = bracket_number_from_income(mean)
109/255:
def bracket_number_from_income(s):
    return ((np.log2(s) + 7) / delta).astype(int)
109/256: bracket_country_time = bracket_number_from_income(mean)
109/257: mean.hanans
109/258: mean.hasnans
109/259: mean[pd.isnull(mean)]
109/260: mean = mean.dropna()
109/261: bracket_country_time = bracket_number_from_income(mean)
109/262: bracket_country_time
109/263: bracket_country_time.to_csv('../wip/mean_position-by-country-year-coverage.csv')
109/264: res4
109/265: res5 = res4.reset_index('bracket')
109/266: res5
109/267: res5.loc[('ALB', 1981, 'N')]
109/268: bracket_country_time.loc[('ALB', 1981, 'N')]
109/269: res5.loc[('ALB', 1981, 'N')]['bracket'] - 147
109/270:
for i in res5.index:
    print(i)
    break
109/271:
for i in res5.index:
    if i in bracket_country_time.index:
        res5.loc[i]['bracket'] = res5.loc[i]['bracket'] - bracket_country_time.loc[i]
    else:
        print(f'{i} do not have mean income data')
109/272: res5 = res4.reset_index('bracket').copy()
109/273:
for i in res5.index:
    if i in bracket_country_time.index:
        res5.loc[i]['bracket'] = res5.loc[i]['bracket'] - bracket_country_time.loc[i]
    else:
        print(f'{i} do not have mean income data')
109/274: res6 = []
109/275: res5 = res4.reset_index('bracket').copy()
109/276: res5 = res4.reset_index('bracket')
109/277:
for i in res5.index:
    if i in bracket_country_time.index:
        df = res5.loc[i].copy()
        df['bracket'] = df['bracket'] - bracket_country_time.loc[i]
        res6.append(df)
    else:
        print(f'{i} do not have mean income data')
109/278: res5 = res4.reset_index('bracket')
109/279:
for i in res5.index.drop_duplicates():
    if i in bracket_country_time.index:
        df = res5.loc[i].copy()
        df['bracket'] = df['bracket'] - bracket_country_time.loc[i]
        res6.append(df)
    else:
        print(f'{i} do not have mean income data')
109/280: res6 = pd.concat(res6)
109/281: res6
109/282: res6.loc[('IND', 2018)]
109/283: res6.loc[('IND')]
109/284: !mkdir ../wip/noisyshape/
109/285:
for g, idx in res6.groupby('country'):
    df = res6.loc[idx]
    df.to_csv(f'../wip/noisyshape/{g}-shapes.csv')
109/286:
for g, idx in res6.groupby('country').groups.items():
    df = res6.loc[idx]
    df.to_csv(f'../wip/noisyshape/{g}-shapes.csv')
109/287:
for g, idx in res6.groupby('country').groups.items():
    df = res6.loc[idx]
    print(df)
    break
109/288: gs = res6.groupby('country')
109/289: gs.groups
109/290: gs.get_group('ALB')
109/291:
for g in gs:
    df = gs.get_group(g)
    print(df)
    break
109/292:
for g in gs:
    
    print(g)
    
    break
109/293:
for g, df in gs:
    df.to_csv(f'../wip/noisyshape/{g}-shape.csv')
    break
109/294:
for g, df in gs:
    df.to_csv(f'../wip/noisyshape/{g}-shape.csv')
109/295: df = gs.get_group('USA')
109/296: df
109/297: df2 = df.loc[('USA', 2000, 'N')]
109/298: df2
109/299: plt.plot(df2['bracket'], df2['HeadCount'])
109/300: mean
109/301: df = gs.get_group('SWE')
109/302: df2 = df.loc[('SWE', 2000, 'N')]
109/303: plt.plot(df2['bracket'], df2['HeadCount'])
109/304: df2 = df.loc[('SWE', 2018, 'N')]
109/305: plt.plot(df2['bracket'], df2['HeadCount'])
109/306: df = gs.get_group('AGO')
109/307: df2 = df.loc[('AGO', 2018, 'N')]
109/308: plt.plot(df2['bracket'], df2['HeadCount'])
109/309: df = gs.get_group('ZWE')
109/310: df2 = df.loc[('ZWE', 2018, 'N')]
109/311: plt.plot(df2['bracket'], df2['HeadCount'])
109/312: df2 = df.loc[('ZWE', 1990, 'N')]
109/313: plt.plot(df2['bracket'], df2['HeadCount'])
109/314: df = gs.get_group('USA')
109/315: df2 = df.loc[('USA', 1990, 'N')]
109/316: plt.plot(df2['bracket'], df2['HeadCount'])
109/317: df2 = df.loc[('USA', 1981, 'N')]
109/318: plt.plot(df2['bracket'], df2['HeadCount'])
109/319: df2
109/320: df2 = df.loc[('USA', 2018, 'N')]
109/321: df2
109/322: res4
109/323: mean
109/324: # fix error: mean income is $ per month but income bracket is $ per day
109/325:
def bracket_number_from_income(s):
    return ((np.log2(s / 30) + 7) / delta).astype(int)
109/326: res3
109/327: res4
109/328: res5
109/329: bracket_country_time
109/330: bracket_country_time = bracket_number_from_income(mean)
109/331: bracket_country_time
109/332: res6 = []
109/333:
for i in res5.index.drop_duplicates():
    if i in bracket_country_time.index:
        df = res5.loc[i].copy()
        df['bracket'] = df['bracket'] - bracket_country_time.loc[i]
        res6.append(df)
    else:
        print(f'{i} do not have mean income data')
109/334: np.all(bracket_country_time > 0)
109/335: res6 = pd.concat(res6)
109/336: res6
109/337: gs = res6.groupby('country')
109/338: df = gs.get_group('USA')
109/339: df2 = df.loc[('USA', 2018, 'N')]
109/340: plt.plot(df2['bracket'], df2['HeadCount'])
109/341: df2 = df.loc[('USA', 1990, 'N')]
109/342: plt.plot(df2['bracket'], df2['HeadCount'])
109/343:
for g, df in gs:
    df.to_csv(f'../wip/noisyshape/{g}-shape.csv')
109/344: df = gs.get_group('ZWE')
109/345: df2 = df.loc[('ZWE', 1990, 'N')]
109/346: plt.plot(df2['bracket'], df2['HeadCount'])
109/347: df = gs.get_group('SWE')
109/348: df2 = df.loc[('SWE', 2018, 'N')]
109/349: plt.plot(df2['bracket'], df2['HeadCount'])
109/350: # playing with data
109/351: fra = gs.get_group('FRA')
109/352: fra_1984 = fra.loc[('FRA', 1984, 'N')]
109/353: fra_1984
109/354: usa = gs.get_group('USA')
109/355: usa_1984 = usa.loc[('USA', 1984, 'N')]
109/356: jpy = gs.get_group('JPY')
109/357: jpy = gs.get_group('JPN')
109/358: jpy_1984 = jpy.loc[('JPN', 1984, 'N')]
109/359: plt.plot(jpy_1984['bracket'], jpy_1984['HeadCount'])
109/360: s1 = jpy_1984[['bracket', 'HeadCount']].set_index('bracket')
109/361: s1
109/362: s2 = usa_1984[['bracket', 'HeadCount']].set_index('bracket')
109/363: s2
109/364: s1 + s2
109/365: s1_, s2_ = s1.align(s2)
109/366: s1_
109/367: s1.align?
109/368: s1_, s2_ = s1.align(s2, fill_value=0)
109/369: s1_
109/370: s3 = (s1_ + s2_) / 2
109/371: s3
109/372: plt.plot(s3)
109/373: s3_ = fra_1984[['bracket', 'HeadCount']].set_index('bracket')
109/374: plt.plot(s3_)
109/375: df = pd.DataFrame({'appox': s3, 'real': s3_})
109/376: s4, s4_ = s3.align(s3_)
109/377: df = pd.DataFrame({'appox': s4, 'real': s4_}, index=s4.index)
109/378: s4
109/379: s4_
109/380: s4, s4_ = s3.align(s3_, fill_values=0)
109/381: s4, s4_ = s3.align(s3_, fill_value=0)
109/382: s4_
109/383: pd.concat([s4, s4_], axis=1)
109/384: df = pd.concat([s4, s4_], axis=1)
109/385: df.columns = ['forcast', 'real']
109/386: df.plot()
109/387: mean
109/388: mean.query("country == 'JPN'")
109/389: mean.where("country == 'JPN'")
109/390: mean
109/391: mean.loc[('JPN', 1984)]
109/392: mean.loc[('USA', 1984)]
109/393: mean.loc[('FRA', 1984)]
109/394: 806 + 1539
109/395: 2345/ 2
109/396: from scipy.optimize import curve_fit
109/397: import smoothlib
109/398: !ls
109/399: import ../scripts/smoothlib
109/400: import sys
109/401: sys.path.insert(0, '../scripts')
109/402: import smoothlib
109/403: run_smooth = smoothlib.run_smooth
109/404: res4
109/405:
def func(x):
    """function to smooth a series"""
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 10, 1)
    if std < 0.021:
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 5, 0)
        res = run_smooth(res, 5, 0)
    else:
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 5, 0)
        res = run_smooth(res, 5, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
109/406: gs = res4.groupby(['country', 'year', 'coverage_type'])
109/407: df = gs.get_group(('USA', 2018, 'N'))
109/408: df
109/409: df['HeadCount']
109/410: func(df['HeadCount'])
109/411:
def func(x):
    """function to smooth a series"""
    x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 10, 1)
    if std < 0.021:
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 5, 0)
        res = run_smooth(res, 5, 0)
    else:
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 5, 0)
        res = run_smooth(res, 5, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
109/412: func(df['HeadCount'])
109/413: from ddf_utils.str import format_float_digits
109/414: from functools import practial
109/415: from functools import partial
109/416: s = func(df['HeadCount'])
109/417: formattor = partial(format_float_digits, digits=6)
109/418: s.map(formattor)
109/419: plt.plot(s.map(formattor).astype(int))
109/420: plt.plot(s.map(formattor).astype(float))
109/421:
def process(idx, ser):
    """function to process smoothing for each series"""
    # print(idx)
    names = ['country', 'year', 'coverage_type', 'income_bracket']
    subser = ser[idx]
    res = func(subser)
    # ensure that the output has 100 records.
    idx_new = pd.MultiIndex.from_product([(idx[0],), (idx[1],), (idx[2],), range(100)], names=names)
    res.index = idx_new  # raise error if length doesn't match
    return res
109/422: ser = df['HeadCount']
109/423: ser.std()
109/424: ser1 = run_smooth(ser.values, 16, 1)
109/425: ser1 = run_smooth(ser, 16, 1)
109/426: ser1 = run_smooth(ser.reset_index(drop=True), 16, 1)
109/427: ser1
109/428: plt.plot(ser1)
109/429: ser1 = run_smooth(ser1.reset_index(drop=True), 8, 1)
109/430: plt.plot(ser1)
109/431: ser1 = run_smooth(ser1.reset_index(drop=True), 10, 0)
109/432: plt.plot(ser1)
109/433: ser1 = run_smooth(ser1.reset_index(drop=True), 10, 1)
109/434: plt.plot(ser1)
109/435:
def func(x):
    """function to smooth a series"""
    x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.021:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
109/436: s = func(df['HeadCount'])
109/437: plt.plot(s)
109/438: s
109/439: plt.plot(df['HeadCount'].values)
109/440:
def func(x):
    """function to smooth a series"""
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.021:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
109/441: s = func(df['HeadCount'].values)
109/442: df['HeadCount'].values
109/443: _s = df['HeadCount'].values
109/444: _s[_s == _s.max()]
109/445: _s.index(_s.max())
109/446: _s.where(_s == _s.max())
109/447: np.where(_s == _s.max())
109/448: np.where(_s == _s.max())[0]
109/449: import sys, importlib
109/450: importlib.reload(sys.modules['smoothlib'])
109/451: run_smooth = smoothlib.run_smooth
109/452:
def func(x):
    """function to smooth a series"""
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.021:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
109/453: s = func(df['HeadCount'])
109/454: plt.plot(s)
109/455: df = gs.get_group(('ZWE', 1981, 'N'))
109/456: s = func(df['HeadCount'])
109/457: plt.plot(s)
109/458: plt.plot(df['HeadCount'].values)
109/459: df = gs.get_group(('SWE', 2000, 'N'))
109/460: plt.plot(df['HeadCount'].values)
109/461: s = func(df['HeadCount'])
109/462: plt.plot(s)
109/463: df['HeadCount'].std()
109/464: df = gs.get_group(('AUS', 1981, 'N'))
109/465: s = func(df['HeadCount'])
109/466: plt.plot(s)
109/467: plt.plot(df['HeadCount'].values)
109/468: df['HeadCount'].describe()
109/469: res4
109/470: res4[res4['HeadCount'] > 0.1]
109/471: df = gs.get_group(('PRY', 1999, 'N'))
109/472: plt.plot(df['HeadCount'].values)
109/473: df['HeadCount'].values
109/474: s = func(df['HeadCount'])
109/475: df = gs.get_group(('DEU', 2019, 'N'))
109/476: df['HeadCount'].values
109/477: res3[0]
109/478: res2[0]
109/479: res2[135]
109/480: res2[135].loc[('DEU')]
109/481: res2[134].loc[('DEU')]
109/482: res2[136].loc[('DEU')]
114/1:
import pandas as pd
import numpy as np
import sys
import os
114/2: # step1: load all downloaded data
114/3:
def load_file_preprocess(filename):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df = df.set_index(['country', 'year', 'coverage_type'])
    return df
114/4:
for f in os.listdir('../source/'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f)
114/5: coverage_type_dtype = pd.CategoricalDtype(list('NAUR'), ordered=True)
114/6:
for f in os.listdir('../source/'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(f)
114/7:
res = {}

for f in os.listdir('../source/'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(os.path.join('../source/', f))
114/8: res[0]
114/9:
for k, df in res.items():
    if df['HeadCount'].hasnans:
        print(k)
        print(df[pd.isnull(df['HeadCount'])])
114/10:
res1 = dict()
for k, df in res.items():
    res1[k] = df.dropna(how='any', subset=['HeadCount'])
114/11: # step2: subtract and get bracket head count
114/12:
for i in range(1, 200):
    df1 = res1[i]
    df2 = res1[i-1]
    df3 = df1['HeadCount'] - df2['HeadCount']
    res2[i-1] = df3
114/13:
res2 = {}

for i in range(1, 200):
    df1 = res1[i]
    df2 = res1[i-1]
    df3 = df1['HeadCount'] - df2['HeadCount']
    res2[i-1] = df3
114/14: res2[0]
114/15:
for k, df in res2.items():
    if df['HeadCount'].hasnans:
        print(k)
        print(df[pd.isnull(df['HeadCount'])])
114/16:
for k, df in res2.items():
    if df.hasnans:
        print(k)
        print(df[pd.isnull(df)])
114/17:
res3 = []

for k, v in res2.items():
    df = v.reset_index()
    df['bracket'] = k
    df = df.set_index(['country', 'year', 'coverage_type', 'bracket'])
    res3.append(df)
114/18: res3 = pd.concat(res3)
114/19: res3
114/20: res3['HeadCount'].hasnans
114/21:
gs = res3.groupby(['country', 'year', 'coverage_type', 'bracket'])
for g, df in gs.groups:
    assert df.shape[0] == 199
114/22:
gs = res3.groupby(['country', 'year', 'coverage_type', 'bracket'])
for g, df in gs.groups.items():
    assert df.shape[0] == 199
114/23:
gs = res3.groupby(['country', 'year', 'coverage_type'])
for g, df in gs.groups.items():
    assert df.shape[0] == 199
114/24:
sys.path.insert(0, '../scripts')
import smoothlib
import matplotlib.pyplot as plt
%matplotlib inline
114/25: run_smooth = smoothlib.run_smooth
114/26:
def func(x):
    """function to smooth a series"""
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.0105:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/27: plt.rcParams['figure.fig_size']
114/28: plt.rcParams['figure.figSize']
114/29: plt.rcParams['figure']
114/30: plt.rcParams
114/31: plt.rcParams['figure.figsize'] = [8, 12]
114/32: df = gs.get_group(('DEU', 2019, 'N'))
114/33: df
114/34: df['HeadCount'].values
114/35: df = gs.get_group(('AUS', 1981, 'N'))
114/36: df['HeadCount'].values
114/37: df = gs.get_group(('KOR', 2012, 'N'))
114/38: df['HeadCount'].values
114/39: df['HeadCount'].describe()
114/40: df = gs.get_group(('AUS', 1981, 'N'))
114/41: df['HeadCount'].describe()
114/42: df = gs.get_group(('DEU', 2019, 'N'))
114/43: df['HeadCount'].describe()
114/44: res4 = res3.copy()
114/45: res4.loc[res4['HeadCount'] < 0, 'HeadCount'] = 0
114/46: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/47: df = gs.get_group(('DEU', 2019, 'N'))
114/48: df['HeadCount'].describe()
114/49: df['HeadCount'].sum()
114/50: plt.plot(df['HeadCount'].values)
114/51:
import matplotlib.pyplot as plt
%matplotlib inline
114/52: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/53: df = gs.get_group(('DEU', 2019, 'N'))
114/54:
df['HeadCount'].describe()  

# maximum is bigger than 0.9! we can assume this data is not correct.
114/55: plt.plot(df['HeadCount'].values)
114/56: plt.rcParams['figure.figsize'] = [12, 8]
114/57: plt.plot(df['HeadCount'].values)
114/58:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.1]
114/59:
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df['HeadCount'].values)
114/60: res4['HeadCount'].describe()
114/61:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.01]
114/62:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.1]
114/63:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.005]
114/64: res4[res4['HeadCount']>0].describe()
114/65:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.0115]
114/66:
# see how many records are bigger than the mean 0.02..
res4[res4['HeadCount'] > 0.02]
114/67:
# see how many records are bigger than 0.1..
res4[res4['HeadCount'] > 0.1]
114/68: df = gs.get_group(('PRY', 1999, 'N'))
114/69: s = func(df['HeadCount'].values)
114/70: plt.plot(s)
114/71: df['HeadCount'].sum()
114/72: df = gs.get_group(('DEU', 2019, 'N'))
114/73:
df['HeadCount'].describe()  

# maximum is bigger than 0.9! we can assume this data is not correct.
114/74: df['HeadCount'].sum()
109/483: df['HeadCount'].values
109/484: df['HeadCount'].sum()
109/485: df['HeadCount'].sum() - df['HeadCount'].max()
109/486: s = df['HeadCount']
109/487: s.loc[s == s.max()] = np.nan
109/488: s = df['HeadCount'].copy()
109/489: s.loc[s == s.max()] = np.nan
109/490: s
109/491: plt.plot(s.values)
114/75: res4 = res3.copy()
114/76: res4.loc[res4['HeadCount'] < 0, 'HeadCount'] = np.nan  # make negative values to nan
114/77: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/78: df.loc[df['HeadCount']
114/79: df['HeadCount'].sum()
114/80:
# look for those sum bigger than 1
for g, df in gs.groups.items():
    if df['HeadCount'].sum() > 1:
        print(g)
114/81:
# look for those sum bigger than 1
for g, df in gs.groups.items():
    if df.sum() > 1:
        print(g)
114/82:
gs = res3.groupby(['country', 'year', 'coverage_type'])
for g in gs.groups.keys():
    df = gs.get_group(g)
    assert df.shape[0] == 199
114/83:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df['HeadCount'].sum() > 1:
        print(g)
114/84: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/85: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/86:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df['HeadCount'].sum() > 1:
        print(g)
114/87:
df = gs.get_group(('AGO', 2000, 'N'))
plt.plot(df['HeadCount'].values)
114/88: res4 = res3.copy()
114/89:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('AGO', 2000, 'N'))
plt.plot(df['HeadCount'].values)
114/90:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('AGO', 2000, 'N'))
plt.plot(df['HeadCount'].values[70:100])
114/91: df['HeadCount'].values[70:100]
114/92:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('AGO', 2004, 'N'))
plt.plot(df['HeadCount'])
114/93:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('AGO', 2004, 'N'))
plt.plot(df['HeadCount'].values)
114/94: df['HeadCount'].values[50:75]
114/95:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('BDI', 1981, 'N'))
plt.plot(df['HeadCount'].values)
114/96: df['HeadCount'].values
114/97:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('ARM', 2000, 'N'))
plt.plot(df['HeadCount'].values)
114/98:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('AUS', 2004, 'N'))
plt.plot(df['HeadCount'].values)
114/99: df['HeadCount'].values
114/100:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('BLR', 1986, 'N'))
plt.plot(df['HeadCount'].values)
114/101: df['HeadCount'].values
114/102:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('CHN', 1989, 'A'))
plt.plot(df['HeadCount'].values)
114/103: df['HeadCount'].values
114/104: gs = res3.groupby(['country', 'year', 'coverage_type'])
114/105:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df['HeadCount'].sum() > 1:
        print(g)
114/106:
gs = res4.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('CAN', 2018, 'N'))
plt.plot(df['HeadCount'].values)
114/107: df['HeadCount'].sum()
114/108:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df['HeadCount'].sum() > 1.1:
        print(g)
109/492: df
109/493: df = gs.get_group(('DEU', 2019, 'N'))
109/494: s = df['HeadCount']
109/495: s
109/496: s.hasnans
109/497: s2 = s.values
109/498: s2
109/499: np.where(s2 == np.nan)
109/500: np.where(pd.isnull(s2))
109/501: where = np.where(pd.isnull(s2))
109/502: todrop = []
109/503:
for i in where:
    todrop.append(i)
    todrop.append(i+1)
109/504: s
109/505: s.iloc[todrop] = np.nan
109/506: s.iloc[todrop]
109/507: todrop
109/508:
for i in where.tolist():
    todrop.append(i)
    todrop.append(i+1)
109/509: where
109/510: np.where?
109/511: where = np.where(pd.isnull(s2.values))
109/512: s2
109/513: where = np.where(pd.isnull(s2.tolist()))
109/514: where
109/515: where = np.where(pd.isnull(s2), 0, 1)
109/516: where
109/517: where = np.where(pd.isnull(s2))
109/518:
for i in where[0]:
    todrop.append(i)
    todrop.append(i+1)
109/519: todrop
109/520: todrop = list()
109/521:
for i in where[0]:
    todrop.append(i)
    todrop.append(i+1)
109/522: s2
109/523: s
109/524: s.iloc[todrop]
109/525: s.iloc[todrop] = np.nan
109/526: s = df['HeadCount'].copy()
109/527: s.iloc[todrop] = np.nan
109/528: s
109/529: s.values
109/530: s.sum()
114/109: res4 = res3.copy()
114/110: res4.loc[res4['HeadCount'] < 0, 'HeadCount'] = np.nan  # make negative values to nan
114/111: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/112:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = list()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.append(w)
            todrop.append(w+1)
        s.iloc[todrop] = np.nan
        df['HeadCount'] = s
114/113:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = list()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.append(w)
            todrop.append(w+1)
        s.iloc[todrop] = np.nan
        res5.append(s)
    else:
        res5.append(s)
114/114: res5 = list()
114/115:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = list()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.append(w)
            todrop.append(w+1)
        s.iloc[todrop] = np.nan
        res5.append(s)
    else:
        res5.append(s)
114/116: res5 = pd.concat(res5)
114/117: gs = res5.groupby(['country', 'year', 'coverage_type'])
114/118:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('CAN', 2018, 'N'))
plt.plot(df['HeadCount'].values)
114/119: res5
114/120:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('CAN', 2018, 'N'))
plt.plot(df.values)
114/121:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2018, 'N'))
plt.plot(df.values)
114/122:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.values)
114/123:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('CHN', 1982, 'A'))
plt.plot(df.values)
114/124:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df['HeadCount'].sum() > 1.1:
        print(g)
114/125:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1.1:
        print(g)
114/126:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('STP', 1999, 'A'))
plt.plot(df.values)
114/127:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('STP', 1999, 'N'))
plt.plot(df.values)
114/128: df.sum()
114/129: gs = res4.groupby(['country', 'year', 'coverage_type'])
114/130: res5 = list()
114/131:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = set()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.add(w-1)
            todrop.add(w)
            todrop.add(w+1)
        s.iloc[todrop] = np.nan
        res5.append(s)
    else:
        res5.append(s)
114/132: res5 = list()
114/133:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = set()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.add(w-1)
            todrop.add(w)
            todrop.add(w+1)
        s.iloc[list(todrop)] = np.nan
        res5.append(s)
    else:
        res5.append(s)
114/134: res5 = pd.concat(res5)
114/135: gs = res5.groupby(['country', 'year', 'coverage_type'])
114/136: res5
114/137:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1.1:
        print(g)
114/138:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('STP', 1999, 'N'))
plt.plot(df.values)
114/139:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.values)
114/140:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df.values)
114/141: gs = res5.groupby(['country', 'year', 'coverage_type'])
114/142: df = gs.get_group(('PRY', 1999, 'N'))
114/143: s = func(df['HeadCount'].values)
114/144: s = func(df.values)
114/145: df
114/146: df.interpolate?
114/147: df.interpolate()
114/148: df.hasnans
114/149: df.interpolate().hasnans
114/150:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df.interpolate())
114/151:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df.interpolate().values)
114/152:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.interpolate().values)
114/153:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.0105:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/154: s = func(df)
114/155: plt.plot(s)
114/156: df.std()
114/157:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.012:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/158: s = func(df)
114/159: plt.plot(s)
114/160: df.interpolate().std()
114/161:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 2)
    if std < 0.012:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/162: s = func(df)
114/163: plt.plot(s)
114/164:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 2)
    if std < 0.012:
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/165: s = func(df)
114/166: plt.plot(s)
114/167:
plt.plot(s)
plt.plot(df)
114/168:
plt.plot(s)
plt.plot(df.values)
114/169: df = gs.get_group(('USA', 1999, 'N'))
114/170: df.std()
114/171: s = func(df)
114/172:
plt.plot(s)
plt.plot(df.values)
114/173: df = gs.get_group(('DEU', 2019, 'N'))
114/174: s = func(df)
114/175:
plt.plot(s)
plt.plot(df.values)
114/176: df.std()
114/177: df = gs.get_group(('AUS', 2019, 'N'))
114/178: df.std()
114/179: s = func(df)
114/180:
plt.plot(s)
plt.plot(df.values)
114/181: df = gs.get_group(('SWE', 2019, 'N'))
114/182: df.std()
114/183: s = func(df)
114/184:
plt.plot(s)
plt.plot(df.values)
114/185:
plt.plot(s)
# plt.plot(df.values)
114/186:
plt.plot(s)
plt.plot(df.values)
114/187:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.012:
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/188: df = gs.get_group(('SWE', 2019, 'N'))
114/189: df.std()
114/190: s = func(df)
114/191:
plt.plot(s)
plt.plot(df.values)
114/192:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    res = run_smooth(x, 20, 1)
    if std < 0.012:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/193: s = func(df)
114/194:
plt.plot(s)
plt.plot(df.values)
114/195:
plt.plot(s)
# plt.plot(df.values)
114/196:
plt.plot(s)
plt.plot(df.values)
114/197:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/198: s = func(df)
114/199:
plt.plot(s)
plt.plot(df.values)
114/200:
plt.plot(s)
plt.plot(df.values, alpha=.2)
114/201:
plt.plot(s)
plt.plot(df.values, alpha=.6)
114/202:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/203: df = gs.get_group(('USA', 2019, 'N'))
114/204: df.std()
114/205: s = func(df)
114/206:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/207: df = gs.get_group(('CHN', 2019, 'A'))
114/208: df.std()
114/209: s = func(df)
114/210:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/211:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        #res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/212: s = func(df)
114/213:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/214: df = gs.get_group(('ZwE', 2019, 'N'))
114/215: df = gs.get_group(('ZWE', 2019, 'N'))
114/216: df.std()
114/217: s = func(df)
114/218:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/219:
plt.plot(s)
# plt.plot(df.values, alpha=.5)
114/220:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 3)
        #res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/221: s = func(df)
114/222:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/223: df = gs.get_group(('ZWE', 2000, 'N'))
114/224: df.std()
114/225: s = func(df)
114/226:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/227:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/228: df = gs.get_group(('ZWE', 2000, 'N'))
114/229: df.std()
114/230: s = func(df)
114/231:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/232: df = gs.get_group(('AUS', 2000, 'N'))
114/233: df.std()
114/234: s = func(df)
114/235:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/236:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/237: s = func(df)
114/238:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/239: df = gs.get_group(('AUS', 1990, 'N'))
114/240: df.std()
114/241: s = func(df)
114/242:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/243: df = gs.get_group(('SWE', 1990, 'N'))
114/244: df.std()
114/245: s = func(df)
114/246:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/247: df = gs.get_group(('AGO', 1990, 'N'))
114/248: df.std()
114/249: s = func(df)
114/250:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/251:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/252: df = gs.get_group(('USA', 1990, 'N'))
114/253: df.std()
114/254: s = func(df)
114/255:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/256: df = gs.get_group(('DEU', 2019, 'N'))
114/257: df.std()
114/258: s = func(df)
114/259:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/260:
plt.plot(s)
# plt.plot(df.values, alpha=.5)
114/261: df = gs.get_group(('AGO', 2019, 'N'))
114/262: df.std()
114/263: s = func(df)
114/264:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/265: df = gs.get_group(('AUS', 2019, 'N'))
114/266: df.std()
114/267: s = func(df)
114/268:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/269: df = gs.get_group(('BLD', 2019, 'N'))
114/270: df = gs.get_group(('BLR', 2019, 'N'))
114/271: df.std()
114/272: s = func(df)
114/273:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/274: df = gs.get_group(('KOR', 2019, 'N'))
114/275: df.std()
114/276: s = func(df)
114/277:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/278: df = gs.get_group(('LNG', 2019, 'N'))
114/279: df = gs.get_group(('LNK', 2019, 'N'))
114/280: df = gs.get_group(('ARM', 2019, 'N'))
114/281: df.std()
114/282: s = func(df)
114/283:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/284: df = gs.get_group(('BEN', 2019, 'N'))
114/285: df.std()
114/286: s = func(df)
114/287:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/288: df = gs.get_group(('BEN', 2010, 'N'))
114/289: df.std()
114/290: s = func(df)
114/291:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/292: df = gs.get_group(('BEN', 2018, 'N'))
114/293: df.std()
114/294: s = func(df)
114/295:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/296: df = gs.get_group(('BEN', 2017, 'N'))
114/297: df.std()
114/298: s = func(df)
114/299:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/300: df = gs.get_group(('BFA', 2017, 'N'))
114/301: df.std()
114/302: s = func(df)
114/303:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/304: df = gs.get_group(('BOL', 2017, 'N'))
114/305: df.std()
114/306: s = func(df)
114/307:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/308: df = gs.get_group(('CIV', 2000, 'N'))
114/309: df.std()
114/310: s = func(df)
114/311:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/312: df = gs.get_group(('COG', 2000, 'N'))
114/313: df.std()
114/314: s = func(df)
114/315:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/316:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/317: s = func(df)
114/318:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/319:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/320: s = func(df)
114/321:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/322:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/323:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/324: s = func(df)
114/325:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/326:
plt.plot(s)
# plt.plot(df.values, alpha=.5)
114/327: df = gs.get_group(('USA', 2000, 'N'))
114/328: df.std()
114/329: s = func(df)
114/330:
plt.plot(s)
# plt.plot(df.values, alpha=.5)
114/331:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/332: df = gs.get_group(('MAR', 2000, 'N'))
114/333: df.std()
114/334: s = func(df)
114/335:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/336: df = gs.get_group(('MOZ', 2000, 'N'))
114/337: df.std()
114/338: s = func(df)
114/339:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/340:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 1)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/341: s = func(df)
114/342:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/343:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 1)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/344: s = func(df)
114/345:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/346:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
114/347: s = func(df)
114/348:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/349: df = gs.get_group(('SWE', 2000, 'N'))
114/350: df.std()
114/351: s = func(df)
114/352:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/353: df = gs.get_group(('PAK', 2000, 'N'))
114/354: df.std()
114/355: s = func(df)
114/356:
plt.plot(s)
plt.plot(df.values, alpha=.5)
114/357: s
114/358:
def process(ser):
    idx = ser.index
    s_new = func(ser.values)
    return pd.Series(s_new, index=idx)
114/359: process(df)
114/360:
def process(ser):
    idx = ser.index
    s_new = func(ser)
    return pd.Series(s_new, index=idx)
114/361: process(df)
114/362: df
114/363: s
114/364:
def process(ser):
    idx = ser.index
    s_new = func(ser)
    return s_new
114/365: process(df)
114/366: pd.Series(s)
114/367: type(s)
114/368:
def process(ser):
    idx = ser.index
    s_new = func(ser)
    s_new.index = idx
    return s_new
114/369: process(df)
114/370: gs.apply(process)
109/531: gs
109/532: len(gs)
109/533: 6645 * 5
109/534: 6645 * 5 / 60
109/535: 6645 * 5 / 60 / 5
109/536: 6645 / 5
109/537: 6645 / 60
114/371: from multiprocessing import Pool
114/372:
with Pool(2) as p:
    res6 = p.map(process, [d1, d2])
114/373:
d1 = gs.get_group(('PAK', 2000, 'N'))
d2 = gs.get_group(('USA', 2000, 'N'))
114/374:
with Pool(2) as p:
    res6 = p.map(process, [d1, d2])
114/375: pd.concat(res6)
114/376:
for g in gs:
    print(g.head())
    break
114/377:
for g in gs:
    print(gs.get_group(g).head())
    break
114/378:
for g in gs:
    print(g)
    break
114/379:
for g, df in gs:
    print(df.head())
    break
114/380: to_smooth = list(gs.values())
114/381:
to_smooth = list()
for g, df in gs:
    to_smooth.append(df)
114/382: len(to_smooth)
114/383:
with Pool(7) as p:
    res6 = p.map(process, to_smooth)
109/538: 6639 / 7
109/539: 6639 / 7 * 3 / 60
114/384: res6
114/385: len(res6)
115/1:
import pandas as pd
import numpy as np
import sys
import os
115/2: # step 8: calculate shape
109/540:
def bracket_number_from_income(s):
    return ((np.log2(s / 30) + 7) / delta).astype(int)
115/3:
def bracket_number_from_income(s):
    return ((np.log2(s / 30) + 7) / delta).astype(int)
115/4: mean = pd.read_csv('../wip/mean_income-by-country-year-coverage_type.csv')
115/5: mean
115/6: mean['Mean'].hasnans
115/7: res7 = pd.read_csv('../wip/smoothmountain/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/8: res7
115/9: res7 = res7.set_index(['country', 'year', 'coverage_type'])
115/10: res7 = res7.set_index(['country', 'year', 'coverage_type'])
115/11: res7
115/12: res7 = pd.read_csv('../wip/smoothmountain/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/13: res7 = res7.set_index(['country', 'year', 'coverage_type'])
115/14: res7
115/15: mean
115/16: mean = mean.set_index(['country', 'year', 'coverage_type'])
115/17: gs = res7.groupby(['country', 'year', 'coverage_type'])
115/18:
res8 = []
for g, df in gs:
    df_ = df.copy()
    g_ = (g[0].lower(), g[1], g[2].lower())
    m = mean.loc[g_, 'Mean']
115/19:
res8 = []
for g, df in gs:
    df_ = df.copy()
    g_ = (g[0].upper(), g[1], g[2].upper())
    m = mean.loc[g_, 'Mean']
115/20:
res8 = []
for g, df in gs:
    df_ = df.copy()
    g_ = (g[0].upper(), g[1], g[2].upper())
    m = mean.loc[g_, 'Mean']
    b = bracket_number_from_income(m)
    df_['bracket'] = df_['bracket'] - b
    res8.append(df_)
109/541: delta
109/542: all_brackets
109/543: brackets_log
115/21:
all_brackets = np.logspace(-7, 13, 200, endpoint=True, base=2)
brackets = pd.DataFrame({'start': all_brackets, 'end': pd.Series(all_brackets).shift(-1)})
brackets_log = brackets.iloc[:-1].copy()
brackets_log['start'] = np.log2(brackets_log['start'])
brackets_log['end'] = np.log2(brackets_log['end'])
brackets_log['d'] = brackets_log.end - brackets_log.start
delta = brackets_log['d'].iloc[0]
115/22:
res8 = []
for g, df in gs:
    df_ = df.copy()
    g_ = (g[0].upper(), g[1], g[2].upper())
    m = mean.loc[g_, 'Mean']
    b = bracket_number_from_income(m)
    df_['bracket'] = df_['bracket'] - b
    res8.append(df_)
115/23: res8 = pd.concat(res8)
115/24: res8
115/25: !mkdir ../wip/smoothshape
115/26: res8 = pd.read_csv('../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/27: res8 = pd.read_csv('../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/28: res8.to_csv('../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/29: res8
115/30:
res7 = pd.read_csv(
    '../wip/smoothmountain/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv',
    dtype={'population_percentage': str})
115/31: res7 = res7.set_index(['country', 'year', 'coverage_type'])
115/32: gs = res7.groupby(['country', 'year', 'coverage_type'])
115/33:
res8 = []
for g, df in gs:
    df_ = df.copy()
    g_ = (g[0].upper(), g[1], g[2].upper())
    m = mean.loc[g_, 'Mean']
    b = bracket_number_from_income(m)
    df_['bracket'] = df_['bracket'] - b
    res8.append(df_)
115/34: res8 = pd.concat(res8)
115/35: res8
115/36: res8.to_csv('../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv')
115/37: res8.to_csv('../../../ddf--datapoints--income_mountain_relative_bracket_shape_for_log--by--country--year--coverage_type--relative_bracket.csv')
115/38: res8
115/39:
res8 = res8.reset_index()
res8.columns
115/40:
res8 = res8.reset_index()
res8.columns = ['country', 'year', 'coverage_type', 'relative_bracket', 'income_mountain_relative_bracket_shape_for_log']
115/41: res8
115/42: res8 = res8.drop(columns=['index'])
115/43:
# res8 = res8.reset_index()
res8.columns = ['country', 'year', 'coverage_type', 'relative_bracket', 'income_mountain_relative_bracket_shape_for_log']
115/44: res8.to_csv('../../../ddf--datapoints--income_mountain_relative_bracket_shape_for_log--by--country--year--coverage_type--relative_bracket.csv')
115/45: res7
115/46: res7 = res7.reset_index()
115/47: res7.columns
115/48: res7.columns = ['country', 'year', 'coverage_type', 'income_bracket_200', 'income_mountain_200_bracket_shape_for_log']
115/49: coverage_type_dtype_2 = pd.CategoricalDtype(list('naur'), ordered=True)
115/50:
res9 = res8.copy()
res9['coverage_type'] = res9['coverage_type'].astype(coverage_type_dtype_2)
115/51: res9.sort_values(by=['country', 'year', 'coverage_type', 'income_bracket_relative'])
115/52: res9.sort_values(by=['country', 'year', 'coverage_type', 'relative_bracket'])
115/53: res9.groupby(by=['country', 'year', 'relative_bracket']).first()
115/54: res10 = res9.groupby(by=['country', 'year', 'relative_bracket']).first()
115/55: res10 = res10.drop(columns=['coverage_type'])
115/56: res10.to_csv('../../ddf--datapoints--income_mountain_200bracket_shape_for_log--by--country--year--relative_bracket.csv')
115/57: res10.to_csv('../../ddf--datapoints--income_mountain_relative_bracket_shape_for_log--by--country--year--relative_bracket.csv')
115/58: res11 = res7.copy()
115/59: res11
115/60: res12 = res11.groupby(by=['country', 'year', 'income_bracket_200']).first()
115/61: res12
115/62:
res11 = res7.copy()
res11['coverage_type'] = res11['coverage_type'].astype(coverage_type_dtype_2)
115/63: res12 = res11.groupby(by=['country', 'year', 'income_bracket_200']).first()
115/64: res12
115/65: res12 = res12.drop(columns=['coverage_type'])
115/66: res12.to_csv('../../ddf--datapoints--income_mountain_200bracket_shape_for_log--by--country--year--income_bracket_200.csv')
115/67: rb = pd.DataFrame({'index': range(-200, 200), 'relative_bracket': range(-200, 200)})
115/68: rb
115/69: rb = pd.DataFrame({'relative_bracket': range(-200, 200), 'name': range(-200, 200)})
115/70: rb = rb.set_index('index')
115/71: rb = rb.set_index('relative_bracket')
115/72: rb.to_csv('../../ddf--entities--relative_bracket.csv')
115/73: coverage_type_dtype = pd.CategoricalDtype(list('NAUR'), ordered=True)
115/74: # step1: load all downloaded data
115/75:
def load_file_preprocess(filename):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df = df.set_index(['country', 'year', 'coverage_type'])
    return df
115/76:
res = {}

for f in os.listdir('../source/'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(os.path.join('../source/', f))
115/77: res[0]
115/78: res[0]
115/79: res[0].loc[('XKX')]
115/80: res7.columns = ['country', 'year', 'coverage_type', 'income_bracket_200', 'income_mountain_200bracket_shape_for_log']
115/81: res12
115/82: res12.columns
115/83: res12.columns = ['income_mountain_200bracket_shape_for_log']
115/84: res12.to_csv('../../ddf--datapoints--income_mountain_200bracket_shape_for_log--by--country--year--income_bracket_200.csv')
115/85: s1 = res7
115/86: gs = res8.grupby(['country', 'year', 'coverage_type'])
115/87: gs = res8.groupby(['country', 'year', 'coverage_type'])
115/88: s1 = gs.get_group(('GRC', 2008, 'N'))
115/89: s1 = gs.get_group(('grc', 2008, 'n'))
115/90: s1
115/91: gs = res6.groupby(['country', 'year', 'coverage_type'])
115/92: gs = res8.groupby(['country', 'year', 'coverage_type'])
115/93: c = 'income_mountain_relative_bracket_shape_for_log'
115/94:
import matplotlib.pyplot as plt
%matplotlib inline
115/95: plt.rcParams['figure.figsize'] = [12, 8]
115/96: plt.plot(s1[c].values)
115/97: s1
115/98: s1[c]
115/99: s1[c].values
115/100: res8[c] = res8[c].astype(float)
115/101: gs = res8.groupby(['country', 'year', 'coverage_type'])
115/102: s1 = gs.get_group(('grc', 2008, 'n'))
115/103: s1[c].values
115/104: plt.plot(s1[c].values)
115/105: mean
115/106:
s1 = gs.get_group(('grc', 2008, 'n'))
s2 = gs.get_group(('esp', 2008, 'n'))
s3_expect = gs.get_group(('jpn', 2008, 'n'))
115/107: s1[c]
115/108:
plt.plot(s1[c].values)
plt.plot(s2[c].values)
115/109:
s1 = gs.get_group(('grc', 2008, 'n')).set_index('relative_bracket')
s2 = gs.get_group(('esp', 2008, 'n')).set_index('relative_bracket')
s3_expect = gs.get_group(('jpn', 2008, 'n')).set_index('relative_bracket')
115/110:
plt.plot(s1[c])
plt.plot(s2[c])
115/111:
plt.plot(s1[c])
plt.plot(s2[c])
plt.plot(s3_expect[c])
115/112:
m1 = mean.loc[('GRC', 2008, 'N')]
m2 = mean.loc[('ESP', 2008, 'N')]
115/113: m1
115/114: m2
115/115:
m1 = mean.loc[('GRC', 2008, 'N')]
m2 = mean.loc[('ESP', 2008, 'N')]
m3_expect = mean.loc[('JPN', 2008, 'N')]
115/116: m3_expect
115/117:
s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')
s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')
s3_expect = gs.get_group(('chn', 2016, 'a')).set_index('relative_bracket')
115/118: s1[c]
115/119:
plt.plot(s1[c])
plt.plot(s2[c])
plt.plot(s3_expect[c])
115/120:
plt.plot(s1[c])
plt.plot(s2[c])
plt.plot(s3_expect[c])
plt.legend()
115/121:
ax, _ = plt.subplot()
ax.plot(s1[c], name='1')
ax.plot(s2[c])
ax.plot(s3_expect[c])
ax.legend()
115/122:
ax = plt.subplot()
ax.plot(s1[c], name='1')
ax.plot(s2[c])
ax.plot(s3_expect[c])
ax.legend()
115/123:
ax = plt.subplot()
ax.plot(s1[c])
ax.plot(s2[c])
ax.plot(s3_expect[c])
ax.legend()
115/124:
plt.plot(s1[c], label='s1')
plt.plot(s2[c], label='s2')
plt.plot(s3_expect[c], label='expected s3')
plt.legend()
115/125:
m1 = mean.loc[('RUS', 1999, 'N')]
m2 = mean.loc[('MUS', 2004, 'N')]
m3_expect = mean.loc[('CHN', 2016, 'A')]
115/126: m1
115/127: m2
115/128: m3_expect
115/129:
s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')[c]
s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')[c]
s3_expect = gs.get_group(('chn', 2016, 'a')).set_index('relative_bracket')[c]
115/130: s1_, s2_ = s1.align(s2)
115/131:
s1_, s2_ = s1.align(s2)
s1_ = s1_.fillna(0)
s2_ = s2_.fillna(0)
s3 = s1 + s2 / 2
115/132:
# plt.plot(s1[c], label='s1')
# plt.plot(s2[c], label='s2')
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/133:
s1_, s2_ = s1.align(s2)
s1_ = s1_.fillna(0)
s2_ = s2_.fillna(0)
s3 = (s1 + s2) / 2
115/134:
# plt.plot(s1[c], label='s1')
# plt.plot(s2[c], label='s2')
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/135: s3
115/136: s3.index[0]
115/137: s3.index.values
115/138: new_idx = s3.index.values + 102
115/139:
new_idx = s3.index.values + 102
s3.index = pd.Index(new_idx)
115/140:
new_idx = s3_expect.index.values + 102
s3_expect.index = pd.Index(new_idx)
115/141:
# plt.plot(s1[c], label='s1')
# plt.plot(s2[c], label='s2')
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/142:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def get_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def trying(x, neighbors):
    s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')[c]
    s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')[c]
    ss = list()
    for n in neighbors:
        ss.append(gs.get_group(n).set_index('relative_bracket')[c])
    
    s3_expect = get_data(x)
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        
    return s3, s3_expect
115/143:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/144:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def do_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def trying(x, neighbors):
    s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')[c]
    s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')[c]
    ss = list()
    for n in neighbors:
        ss.append(gs.get_group(n).set_index('relative_bracket')[c])
    
    s3_expect = get_data(x)
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        
    return s3, s3_expect
115/145:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/146:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/147:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def do_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def reset_x_axis(x_):
    x = x_.copy()
    new_idx = x.index.values - x.index.values[0]
    x.index = pd.Index(new_idx)
    return x

def trying(x, neighbors):
    s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')[c]
    s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')[c]
    ss = list()
    for n in neighbors:
        ss.append(gs.get_group(n).set_index('relative_bracket')[c])
    
    s3_expect = get_data(x)
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        
    return reset_x_axis(s3), reset_x_axis(s3_expect)
115/148:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/149:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/150:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def get_mean(x):
    return mean.loc[(x[0].upper(), x[1], x[2].upper()), 'Mean']

def do_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def reset_x_axis(x_, m):
    x = x_.copy()
    mb = bracket_number_from_income(m)
    new_idx = x.index.values + mb
    x.index = pd.Index(new_idx)
    return x

def trying(x, neighbors):
    s1 = gs.get_group(('rus', 1999, 'n')).set_index('relative_bracket')[c]
    s2 = gs.get_group(('mus', 2004, 'n')).set_index('relative_bracket')[c]
    ss = list()
    for n in neighbors:
        ss.append(gs.get_group(n).set_index('relative_bracket')[c])
    
    s3_expect = get_data(x)
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        
    return reset_x_axis(s3), reset_x_axis(s3_expect)
115/151:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/152:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def get_mean(x):
    return mean.loc[(x[0].upper(), x[1], x[2].upper()), 'Mean']

def do_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def reset_x_axis(x_, m):
    x = x_.copy()
    mb = bracket_number_from_income(m)
    new_idx = x.index.values + mb
    x.index = pd.Index(new_idx)
    return x

def trying(x, neighbors):
    s3_expect = get_data(x)
    m_expect = get_mean(x)
    
    mean = [get_mean(neighbors[0])]
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        mean.append(get_mean(n))
        
    mean = np.sum(mean) / len(mean)
        
    return reset_x_axis(s3, mean), reset_x_axis(s3_expect, m_expect)
115/153:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/154:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/155: s3
115/156: s3_expect
115/157: s3
115/158:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n'),
    ('bgr', 2004, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/159:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/160:
x = ('chn', 2016, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n'),
    ('bgr', 2004, 'n'),
    ('kaz', 1990, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/161:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/162: s3_expect.sum()
115/163: s3.sum()
115/164:
x = ('ind', 2000, 'a')
neighbors = [
    ('rus', 1999, 'n'),
    ('mus', 2004, 'n'),
    ('bgr', 2004, 'n'),
    ('kaz', 1990, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/165:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/166:
x = ('ind', 2000, 'a')
neighbors = [
    ('stp', 2000, 'n'),
    ('bgd', 2007, 'n'),
    ('npl', 2010, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/167:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/168:
x = ('bra', 1990, 'n')
neighbors = [
    ('pan', 1990, 'n'),
    ('bwa', 1996, 'n'),
    ('nam', 2014, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/169:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/170:
# great example

x = ('bra', 1990, 'n')
neighbors = [
    ('pan', 1990, 'n'),
    ('bwa', 1996, 'n'),
    ('nam', 2014, 'n')
]
115/171:
x = ('bgd', 2014, 'n')
neighbors = [
    ('sdn', 2009, 'n'),
    ('uzb', 2003, 'n'),
    ('aze', 1997, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/172:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/173:
x = ('gtm', 2014, 'n')
neighbors = [
    ('ago', 2014, 'n'),
    ('bol', 2014, 'n'),
    ('phl', 2012, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/174:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/175:
x = ('eth', 2011, 'n')
neighbors = [
    ('sle', 2011, 'n'),
    ('lbr', 2011, 'n'),
    ('chn', 1990, 'a')
]

s3, s3_expect = trying(x, neighbors)
115/176:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/177:
gs = res8.groupby(['country', 'year', 'coverage_type'])

def get_data(x):
    return gs.get_group(x).set_index('relative_bracket')[c]

def get_mean(x):
    return mean.loc[(x[0].upper(), x[1], x[2].upper()), 'Mean']

def do_average(s1, s2):
    s1_, s2_ = s1.align(s2)
    s1_ = s1_.fillna(0)
    s2_ = s2_.fillna(0)
    s3 = (s1_ + s2_) / 2
    return s3

def reset_x_axis(x_, m):
    x = x_.copy()
    mb = bracket_number_from_income(m)
    new_idx = x.index.values + mb
    x.index = pd.Index(new_idx)
    return x

def trying(x, neighbors):
    s3_expect = get_data(x)
    m_expect = get_mean(x)
    
    mean = [get_mean(neighbors[0])]
    s3 = get_data(neighbors[0])
    for n in neighbors[1:]:
        s3 = do_average(s3, get_data(n))
        mean.append(get_mean(n))
        
    mean = np.sum(mean) / len(mean)
    print(mean, m_expect)
        
    return reset_x_axis(s3, mean), reset_x_axis(s3_expect, m_expect)
115/178:
x = ('eth', 2011, 'n')
neighbors = [
    ('sle', 2011, 'n'),
    ('lbr', 2011, 'n'),
    ('chn', 1990, 'a')
]

s3, s3_expect = trying(x, neighbors)
115/179: bracket_number_from_income(63)
115/180: bracket_number_from_income(86)
115/181: bracket_number_from_income(63)
115/182:
# low gini, not very good
x = ('eth', 2011, 'n')
neighbors = [
    ('sle', 2011, 'n'),
    ('lbr', 2011, 'n'),
    ('chn', 1990, 'a')
]
115/183:
x = ('bra', 1990, 'n')
neighbors = [
    ('pan', 1990, 'n'),
    ('bwa', 1996, 'n'),
    ('nam', 2014, 'n')
]

s3, s3_expect = trying(x, neighbors)
115/184: bracket_number_from_income(262)
115/185: bracket_number_from_income(282)
115/186:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
115/187:
x = ('eth', 2011, 'n')
neighbors = [
    ('sle', 2011, 'n'),
    ('lbr', 2011, 'n'),
    ('chn', 1990, 'a')
]

s3, s3_expect = trying(x, neighbors)
115/188: bracket_number_from_income(63)
115/189:
plt.plot(s3_expect, label='expected s3')
plt.plot(s3, label='calculated s3')
plt.legend()
116/1: import pandas as pd
116/2: import numpy as np
116/3: import os
116/4: import os.path as osp
116/5: income = '../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
116/6: gini = '../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
116/7: income_file = '../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
116/8: gini_file = '../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
116/9: income = pd.read_csv(income_file)
116/10: mean = pd.read_csv(mean_file)
116/11: gini = pd.read_csv(gini_file)
116/12: mean
116/13: income
116/14: gini
116/15: income = income.set_index(['country', 'time'])
116/16: gini = gini.set_index(['country', 'year'])
116/17: gini = gini.set_index(['country', 'time'])
116/18: gini
116/19: income_, gini_ = income.align(gini)
116/20: income_
116/21: merge = pd.concat([income, gini], axis=1)
116/22: merge
116/23: merge[pd.isnull(merge)]
116/24: merge.loc[pd.isnull(merge)]
116/25: merge
116/26: povcal = './wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
116/27: povcal_file = './wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
116/28: povcal = pd.read_csv(povcal_file)
116/29: povcal
116/30: povcal = povcal.set_index(['country', 'year', 'coverage_type'])
116/31: povcal
116/32: # get all available country/year pair from povcal
116/33: povcal = povcal.reset_index()
116/34: povcal[['country', 'year']].unique()
116/35: povcal[['country', 'year']]
116/36: povcal[['country', 'year']].drop_duplicates()
116/37: # convert xkx to kos. Same country, but povcal and open-numbers use different code
116/38: povcal['country'].translate({'xkx': 'kos'})
116/39: povcal['country'].rename({'xkx': 'kos'})
116/40: povcal['country'] = povcal['country'].rename({'xkx': 'kos'})
116/41: povcal[povcal['country'] == 'kos']
116/42: povcal[povcal['country'] == 'xkx']
116/43: povcal['country'] = povcal['country'].replace('xkx', 'kos')
116/44: povcal[povcal['country'] == 'xkx']
116/45: povcal[povcal['country'] == 'kos']
116/46: unique_country_year = povcal[['country', 'year']].drop_duplicates()
116/47:
for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    i = merge.loc[r, 'mincpcap_cppp']
    g = merge.loc[r, 'gini']
116/48:
for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    if r in merge.index:
        i = merge.loc[r, 'mincpcap_cppp']
        g = merge.loc[r, 'gini']
116/49: known_income = list()
116/50: known_gini = list()
116/51:
for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    if r in merge.index:
        i = merge.loc[r, 'mincpcap_cppp']
        g = merge.loc[r, 'gini']
    else:
        i = np.nan
        g = np.nan
    known_income.append(i)
    known_gini.append(g)
116/52: known_country_year_df = pd.DataFrame({'income': known_income, 'gini': known_gini}, index=unique_country_year)
116/53: known_country_year_df
116/54: idx = pd.MultiIndex.from_frame(unique_country_year)
116/55: idx
116/56: known_country_year_df = pd.DataFrame({'income': known_income, 'gini': known_gini}, index=idx)
116/57: known_country_year_df
116/58: known_country_year_df.to_csv('wip/income_gini_for_known_shape_countries.csv')
116/59:
def get_min_distance_loc(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + np.power(known_country_year_df['gini'] - gini), 2)
    return res.sort_values(ascending=True)
116/60: get_min_distance_loc(5.3, 53)
116/61:
def get_min_distance_loc(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + np.power(known_country_year_df['gini'] - gini, 2), 2)
    return res.sort_values(ascending=True)
116/62: get_min_distance_loc(5.3, 53)
116/63: np.power(known_country_year_df['income'] - 5.3, 2)
116/64: np.power(known_country_year_df['income'] - 5.3, 2) + np.power(known_country_year_df['gini'] - 5.3, 2)
116/65: r1 = np.power(known_country_year_df['income'] - 5.3, 2) + np.power(known_country_year_df['gini'] - 5.3, 2)
116/66: r2 = np.sqrt(r1, 2)
116/67: r2 = np.sqrt(r1.values, 2)
116/68: r2 = np.sqrt(r1.dropna(), 2)
116/69: r2 = np.sqrt(r1)
116/70: r2
116/71: r2 = np.sqrt(r1.dropna())
116/72: r2
116/73: r2 = np.sqrt(r1)
116/74: r2.dropna()
116/75: r2.sort_values(ascending=True)
116/76:
def get_min_distance_loc(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + np.power(known_country_year_df['gini'] - gini, 2))
    return res.sort_values(ascending=True)
116/77: get_min_distance_loc(5.3, 53)
116/78: res = get_min_distance_loc(5.3, 53)
116/79: res.loc[res.index.drop_duplicates(subset=['country'], keep='first')]
116/80: res.loc[res.index.drop_duplicates(subset=['country'], keep='first')]
116/81: res.index.drop_duplicates?
116/82: ecs = list()
116/83:
for i, v in res.iteritems():
    if i in ecs:
        continue
    ecs.append(i[0])
    if len(ecs) >= 5:
        break
116/84: ecs
116/85:
for i, v in res.iteritems():
    print(i, v)
    if i in ecs:
        continue
    ecs.append(i[0])
    if len(ecs) >= 5:
        break
116/86:
for i, v in res.iteritems():
    if i[0] in ecs:
        continue
    ecs.append(i[0])
    if len(ecs) >= 5:
        break
116/87: ecs = list()
116/88:
for i, v in res.iteritems():
    if i[0] in ecs:
        continue
    ecs.append(i[0])
    if len(ecs) >= 5:
        break
116/89: ecs
116/90: neis = list()
116/91: ecs = list()
116/92:
for i, v in res.iteritems():
    if i[0] in ecs:
        continue
    ecs.append(i[0])
    neis.append(i)
    if len(ecs) >= 5:
        break
116/93: neis
116/94: res[neis[1]]
116/95: known_country_year_df[neis[1]]
116/96: known_country_year_df.loc[neis[1]]
116/97:
def get_neibours(income, gini):
    neis = list()
    ecs = list()
    res = get_min_distance_loc(income, gini)  # should rename to get_distance
    for i, v in res.iteritems():
        if i[0] in ecs:
            continue
        ecs.append(i[0])
        neis.append(i)
        if len(ecs) >= 5:
            break
    return neis
116/98: get_neibours(5.3, 53)
116/99: # next, merging shapes and calculate mean
116/100: res
116/101: res = res.dropna()
116/102: res
116/103: known_country_year_df
116/104: known_country_year_df = known_country_year_df.dropna(how='any')
116/105: known_country_year_df
116/106: merge
116/107: res = dict()
116/108:
for i, row in merge.iterrows():
    if row['mincpcap_cppp'] and row['gini'] and i not in known_country_year_df.index:
        res[i] = get_neibours(row['mincpcap_cppp'], row['gini'])
116/109: len(res)
116/110: res.next()
116/111: next(res)
116/112: next(res.keys())
116/113: res[('ago', 1989)]
116/114: res[('ago', 1890)]
116/115: %matplotlib qt
116/116: # making a plot for available gini/income pair and all gini/income pair
116/117: merge
116/118: merge_ = merge.dropna(how='any')
116/119: plt.plot(merge_['mincpcap_pcpp'], merge_['gini'])
116/120: import matplotlib.pyplot as plt
116/121: plt.plot(merge_['mincpcap_pcpp'], merge_['gini'])
116/122: plt.plot(merge_['mincpcap_cppp'], merge_['gini'])
116/123: plt.scatter(merge_['mincpcap_cppp'], merge_['gini'])
116/124: plt.scatter(merge_['mincpcap_cppp'], merge_['gini'], size=1)
116/125: plt.scatter(merge_['mincpcap_cppp'], merge_['gini'], linewidth=1)
116/126: plt.scatter(merge_['mincpcap_cppp'], merge_['gini'], linewidth=.1)
116/127: plt.scatter?
116/128: plt.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=.1)
116/129: known_country_year_df
116/130: ax = plt.subplot(1, 1, 1)
116/131: ax.plot(known_country_year_df['income'], known_country_year_df['gini'])
116/132: ax = plt.subplot(1, 1, 1)
116/133: ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=.1)
116/134: ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=.1, alpha=.5, color='grey')
116/135: ax.set_xscale('log')
116/136:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=.3)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=.3, alpha=.5, color='grey')
    ax.set_xscale('log')
    return ax
116/137: do_plot()
116/138:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    return ax
116/139: do_plot()
116/140:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='black')
    ax.set_xscale('log')
    return ax
116/141: do_plot()
116/142:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1, color='cyan')
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    return ax
116/143: do_plot()
116/144:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    return ax
116/145: do_plot()
116/146: known_country_year_df
116/147: merge
116/148: merge_
116/149: # time to write a function to merge shapes and get mean income!\
116/150:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.algin(s2, fill_valus=0)
    s3 = (s1_ + s2_) / 2
    return s3
116/151: shapes_file = 'wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
116/152: shapes = pd.read_csv(shapes_file)
116/153: shapes
116/154: coverage_type_dtype = pd.CategoricalDtype(list('naur'), ordered=True)
116/155: shapes = pd.read_csv(shapes_file, dtype={'coverage_type': coverage_type_dtype})
116/156: shapes
116/157: shapes_nc = shapes.groupby(['country', 'year', 'bracket']).first()
116/158: shapes_nc
116/159: shapes_nc['coverage_type'].unique()
116/160: shapes_nc[shapes_nc['coverage_type'] == 'r']
116/161: shapes
116/162: shapes = shapes.sort_values(by=['country', 'year', 'bracket', 'coverage_type'])
116/163: shapes
116/164: shapes_nc = shapes.groupby(['country', 'year', 'bracket']).first()
116/165: shapes_nc[shapes_nc['coverage_type'] == 'r']
116/166: sss = shapes_nc[shapes_nc['coverage_type'] == 'r'].reset_index()
116/167: sss
116/168: sss.loc[sss.country == 'chn']
116/169: shapes
116/170: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981)]
116/171: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.coverage_type == 'a')]
116/172: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.coverage_type == 'a')]['population_percentage'].values
116/173: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.bracket == 128)]['population_percentage'].values
116/174: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.bracket == 128)]
116/175: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.bracket > 128)]
116/176: shapes.loc[(shapes.country == 'chn') & (shapes.year == 1981) & (shapes.bracket > 100)]
116/177: shapes_nc
116/178: shapes_nc.loc(('chn', 1981))
116/179: shapes_nc.loc[('chn', 1981)]
117/1:
import pandas as pd
import numpy as np
117/2:
import matplotlib.pyplot as plt

%matplotlib inline
117/3:
import os
import os.path as osp
117/4:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
117/5:
income = pd.read_csv(income_file)
gini = pd.read_csv(gini_file)
shapes = pd.read_csv(shapes_file)
117/6:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['country', 'time'])
shapes = pd.read_csv(shapes_file)
117/7: income_, gini_ = income.align(gini)
117/8: merge = pd.concat([income, gini], axis=1)
117/9: merge
117/10:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
shapes['country'].replace({'xkx': 'kos'})
117/11:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
shapes['country'] = shapes['country'].replace({'xkx': 'kos'})
117/12:
# get all available country/year pair from povcal

unique_country_year = shapes[['country', 'year']].drop_duplicates()
117/13:
known_income = list()
known_gini = list()

for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    if r in merge.index:
        i = merge.loc[r, 'mincpcap_cppp']
        g = merge.loc[r, 'gini']
    else:
        i = np.nan
        g = np.nan
    known_income.append(i)
    known_gini.append(g)
117/14:
idx = pd.MultiIndex.from_frame(unique_country_year)
known_country_year_df = pd.DataFrame({'income': known_income, 'gini': known_gini}, index=idx)
117/15: known_country_year_df
117/16: known_country_year_df.to_csv('../wip/income_gini_for_known_shape_countries.csv')
117/17:
def get_distances(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + np.power(known_country_year_df['gini'] - gini, 2))
    return res.sort_values(ascending=True)
117/18:
def get_neighbors(income, gini):
    neis = list()
    ecs = list()
    res = get_distances(income, gini)
    for i, v in res.iteritems():
        if i[0] in ecs:
            continue
        ecs.append(i[0])
        neis.append(i)
        if len(ecs) >= 5:
            break
    return neis
117/19: get_neibours(5.3, 53)
117/20: get_neighbors(5.3, 53)
117/21: # next, merging shapes and calculate mean
117/22: # making a plot for available gini/income pair and all gini/income pair
117/23: merge_ = merge.dropna(how='any')
117/24:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=.3)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=.3, alpha=.5, color='grey')
    ax.set_xscale('log')
    return ax
117/25:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    return ax
117/26: do_plot()
117/27: plt.rcParams['figure.figsize'] = (8, 15)
117/28: do_plot()
117/29: plt.rcParams['figure.figsize'] = (15, 8)
117/30: do_plot()
117/31: plt.rcParams['figure.figsize'] = (15, 10)
117/32: do_plot()
117/33:
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['figure.dpi'] = 300
117/34: do_plot()
117/35:
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['figure.dpi'] = 196
117/36: do_plot()
117/37: gini
117/38: income
117/39:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    ax.xlabel('income')
    ax.ylabel('gini')
    return ax
117/40:
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['figure.dpi'] = 196
117/41: do_plot()
117/42:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1)
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    ax.set_xlabel('income')
    ax.set_ylabel('gini')
    return ax
117/43:
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['figure.dpi'] = 196
117/44: do_plot()
117/45:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1, color='#AA00FF')
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    ax.set_xlabel('income')
    ax.set_ylabel('gini')
    return ax
117/46: do_plot()
117/47:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.algin(s2, fill_valus=0)
    s3 = (s1_ + s2_) / 2
    return s3
117/48: reduce
117/49: from functools import reduce
117/50: reduce?
117/51:
def merge_nshapes(s_list):
    reduce(merge_2shapes, s_list)
117/52:
def merge_nshapes(s_list):
    return reduce(merge_2shapes, s_list)
117/53: shapes
117/54: shapes_ = shapes.set_index(['country', 'year'])
117/55: x = pd.Series(range(1000000))
117/56: timeit 9 in x.values
117/57: timeit 9 in x.array
117/58: timeit 9 in x.values
117/59:
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print('using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
    return df_nc
117/60: get_shape(('chn', 1981))
117/61:
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print('using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
            return df_nc
117/62: get_shape(('chn', 1981))
117/63: slist = get_neighbors(5.3, 53)
117/64: df_list = list(map(get_shape, slist))
117/65: merge_nshapes(df_list)
117/66:
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print('using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')[['population_percentage']]
            return df_nc
117/67: slist = get_neighbors(5.3, 53)
117/68: df_list = list(map(get_shape, slist))
117/69: merge_nshapes(df_list)
117/70:
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print('using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
            return df_nc
117/71:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.align(s2, fill_valus=0)
    s3 = (s1_ + s2_) / 2
    return s3
117/72:
def merge_nshapes(s_list):
    return reduce(merge_2shapes, s_list)
117/73: df_list = list(map(get_shape, slist))
117/74: merge_nshapes(df_list)
117/75:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.align(s2, fill_values=0)
    s3 = (s1_ + s2_) / 2
    return s3
117/76: merge_nshapes(df_list)
117/77:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.align(s2, fill_value=0)
    s3 = (s1_ + s2_) / 2
    return s3
117/78:
def merge_nshapes(s_list):
    return reduce(merge_2shapes, s_list)
117/79: merge_nshapes(df_list)
117/80: res = merge_nshapes(df_list)
117/81: plt.plot(res)
117/82:
def bracket_number_from_income(s):
    return ((np.log2(s / 30) + 7) / delta).astype(int)
117/83: slist = get_neighbors(4, 30)
117/84: df_list = list(map(get_shape, slist))
117/85: res = merge_nshapes(df_list)
117/86: plt.plot(res)
117/87: slist = get_neighbors(6, 40)
117/88: df_list = list(map(get_shape, slist))
117/89: res = merge_nshapes(df_list)
117/90: plt.plot(res)
117/91: slist = get_neighbors(6, 50)
117/92: df_list = list(map(get_shape, slist))
117/93: res = merge_nshapes(df_list)
117/94: plt.plot(res)
117/95: shapes_ = shapes.set_index(['country', 'year']).sort_index()
117/96: get_shape(('chn', 1981))
117/97: slist = get_neighbors(6, 50)
117/98: df_list = list(map(get_shape, slist))
117/99:
def get_income_gini(idx):
    return known_country_year_df.loc[idx]
117/100: get_income_gini(('ago', 1981))
117/101:
def get_income_gini(idx):
    i = known_country_year_df.loc[idx, 'income']
    g = known_country_year_df.loc[idx, 'gini']
    return i, g
117/102:
def get_estimated_shape(idx):
    # 1. if the idx is in known shapes, just return the known shape
    # 2. if not, return the estimated shape
    if idx in shapes_.index:
        return get_shape(idx)
    else:
        i, g = get_income_gini(idx)
        nei = get_neighbors(i, g)
        slist = list(map(get_shape, nei))
        return merge_nshapes(slist)
117/103: get_shape(('ago', 1981))
117/104: get_estimated_shape(('ago', 1981))
117/105: get_estimated_shape(('ago', 1900))
117/106: merge
117/107:
def get_income_gini(idx):
    i = merge.loc[idx, 'mincpcap_cppp']
    g = merge.loc[idx, 'gini']
    return i, g
117/108: get_estimated_shape(('ago', 1900))
117/109: res = get_estimated_shape(('ago', 1900))
117/110: plt.plot(res)
117/111:
res1 = get_estimated_shape(('ago', 1981))
res2 = get_shape(('ago', 1981))
117/112:
plt.plot(res1)
plt.plot(res2)
117/113: get_neighbors(('ago', 1981))
117/114: get_neighbors(5.5, 53)
117/115:
get_estimated_shape_2(income, gini):
    # get estimated shape based on income and gini
    nei = get_neighbours(income, gini)
    slist = list(map(get_shape, nei))
    return merge_nshapes(slist)
117/116:
def get_estimated_shape_2(income, gini):
    # get estimated shape based on income and gini
    nei = get_neighbours(income, gini)
    slist = list(map(get_shape, nei))
    return merge_nshapes(slist)
117/117:
res1 = get_estimated_shape_2(5.5, 53)
res2 = get_shape(('ago', 1981))
117/118:
def get_estimated_shape_2(income, gini):
    # get estimated shape based on income and gini
    nei = get_neighbors(income, gini)
    slist = list(map(get_shape, nei))
    return merge_nshapes(slist)
117/119:
res1 = get_estimated_shape_2(5.5, 53)
res2 = get_shape(('ago', 1981))
117/120:
plt.plot(res1)
plt.plot(res2)
117/121:
res1 = get_estimated_shape_2(5.5461, 53.8)
res2 = get_shape(('ago', 1981))
117/122: get_neighbors(5.5461, 53.8)
117/123:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
117/124:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/125:
def get_neighbors(income, gini, n=5):
    neis = list()
    ecs = list()
    res = get_distances(income, gini)
    for i, v in res.iteritems():
        if i[0] in ecs:
            continue
        ecs.append(i[0])
        neis.append(i)
        if len(ecs) >= n:
            break
    return neis
117/126:
def get_estimated_shape(idx, neighbours=5):
    # 1. if the idx is in known shapes, just return the known shape
    # 2. if not, return the estimated shape
    if idx in shapes_.index:
        return get_shape(idx)
    else:
        i, g = get_income_gini(idx)
        nei = get_neighbors(i, g, neighbours)
        slist = list(map(get_shape, nei))
        return merge_nshapes(slist)
117/127:
def get_estimated_shape(idx, neighbours=5):
    # 1. if the idx is in known shapes, just return the known shape
    # 2. if not, return the estimated shape
    if idx in shapes_.index:
        return get_shape(idx)
    else:
        i, g = get_income_gini(idx)
        nei = get_neighbors(i, g, neighbours_n)
        slist = list(map(get_shape, nei))
        return merge_nshapes(slist)
117/128:
def get_estimated_shape_2(income, gini, neighbours=5):
    # get estimated shape based on income and gini
    nei = get_neighbors(income, gini, neighbours)
    slist = list(map(get_shape, nei))
    return merge_nshapes(slist)
117/129: get_neighbors(5.5461, 53.8, 3)
117/130:
res1 = get_estimated_shape_2(5.5461, 53.8, 3)
res2 = get_shape(('ago', 1981))
117/131:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/132:
res1 = get_estimated_shape_2(5.5461, 53.8, 10)
res2 = get_shape(('ago', 1981))
117/133:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/134:
def get_neighbors(income, gini, n=5):
    neis = list()
    ecs = list()
    res = get_distances(income, gini)
    for i, v in res.iteritems():
        if i[0] in ecs:
            continue
        ecs.append(i[0])
        neis.append(i)
        if len(ecs) >= n:
            break
    if len(ecs) < n:
        print('can not get enough points')
    return neis
117/135: get_income_gini(('usa', 2000))
117/136:
res1 = get_estimated_shape_2(63.56, 40.5, 3)
res2 = get_shape(('usa', 2000))
117/137:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/138:
res1 = get_estimated_shape_2(63.56, 40.5, 5)
res2 = get_shape(('usa', 2000))
117/139:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/140:
res1 = get_estimated_shape_2(63.56, 40.5, 10)
res2 = get_shape(('usa', 2000))
117/141:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/142:
res1 = get_estimated_shape_2(63.56, 40.5, 2)
res2 = get_shape(('usa', 2000))
117/143:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/144:
res1 = get_estimated_shape_2(63.56, 40.5, 3)
res2 = get_shape(('usa', 2000))
117/145:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/146:
res1 = get_estimated_shape_2(63.56, 40.5, 5)
res2 = get_shape(('usa', 2000))
117/147:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/148: get_income_gini(('bra', 1990))
117/149:
i, g = get_income_gini(('bra', 1990))
get_neighbors(i, g)
117/150:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(('bra', 1990))
117/151:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/152:
s0 = get_shape(('bra', 1990))
nei = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]
slist = list(map(get_shape, nei))
s3 = merge_nshapes(slist)
117/153:
plt.plot(s3, label='estimated')
plt.plot(s0, label='actual')
plt.legend()
117/154:
s0 = get_shape(('bra', 1990))
nei_ = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]
nei = [('gab', 1981), ('sur', 2002), ('bol', 1998)]
slist = list(map(get_shape, nei))
s3 = merge_nshapes(slist)
117/155:
plt.plot(s3, label='estimated')
plt.plot(s0, label='actual')
plt.legend()
117/156:
for n in nei:
    print(get_income_gini(n))
117/157:
for n in nei_:
    print(get_income_gini(n))
117/158: np.log(1)
117/159:
def get_distances(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + 
                  np.log(np.power(known_country_year_df['gini']) - np.log(gini), 2))
    return res.sort_values(ascending=True)
117/160:
i, g = get_income_gini(('bra', 1990))
get_neighbors(i, g)
117/161:
def get_distances(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(known_country_year_df['income'] - income, 2) + 
                  np.power(np.log(known_country_year_df['gini']) - np.log(gini), 2))
    return res.sort_values(ascending=True)
117/162:
i, g = get_income_gini(('bra', 1990))
get_neighbors(i, g)
117/163:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(('bra', 1990))
117/164:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/165:
s0 = get_shape(('bra', 1990))
nei_ = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]
nei = [('sur', 1998), ('col', 2000), ('chl', 1984), ('bol', 2004)]
slist = list(map(get_shape, nei))
s3 = merge_nshapes(slist)
117/166:
for n in nei:
    print(get_income_gini(n))
117/167:
for n in nei_:
    print(get_income_gini(n))
117/168:
s0 = get_shape(('bra', 1990))
nei_ = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]
nei = [('sur', 1998), ('col', 2000), ('chl', 1984)]
slist = list(map(get_shape, nei))
s3 = merge_nshapes(slist)
117/169:
for n in nei:
    print(get_income_gini(n))
117/170:
for n in nei_:
    print(get_income_gini(n))
117/171:
s0 = get_shape(('bra', 1990))
nei_ = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]
nei = [('sur', 1998), ('col', 2000), ('chl', 1984)]
slist = list(map(get_shape, nei_))
s3 = merge_nshapes(slist)
117/172:
plt.plot(s3, label='estimated')
plt.plot(s0, label='actual')
plt.legend()
117/173:
# my hand pick neighbours
nei = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]

for n in nei:
    print(get_income_gini(n))
117/174:
# neighbours with least distances
nei = [('sur', 1998), 
       ('col', 2000), 
       ('chl', 1984)]

for n in nei_:
    print(get_income_gini(n))
117/175:
# neighbours with least distances
nei = [('sur', 1998), 
       ('col', 2000), 
       ('chl', 1984)]

for n in nei:
    print(get_income_gini(n))
117/176:
# income/gini for some point
get_income_gini(('bra', 1990))
117/177:
# my hand pick neighbours
nei = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]

for n in nei:
    print(get_income_gini(n))
117/178:
# neighbours with least distances
nei = [('sur', 1998), 
       ('col', 2000), 
       ('chl', 1984)]

for n in nei:
    print(get_income_gini(n))
117/179:
# income/gini for some point
get_income_gini(('bra', 1990))
117/180:
# neighbours with least distances
nei = [('sur', 1998), 
       ('col', 2000), 
       ('chl', 1984)]

for n in nei:
    print(get_income_gini(n))
117/181:
# my hand pick neighbours
nei = [('pan', 1990),
       ('bwa', 1996),
       ('nam', 2014)
      ]

for n in nei:
    print(get_income_gini(n))
117/182:
def get_estimated_shape_2(income, gini, neighbours=5):
    # get estimated shape based on income and gini
    nei = get_neighbors(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
117/183:
x = ('bra', 2001)
i, g = get_income_gini(x)
117/184:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
117/185:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/186:
x = ('bra', 1990)
i, g = get_income_gini(x)
117/187:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
117/188:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/189:
x = ('swe', 1990)
i, g = get_income_gini(x)
117/190:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
117/191:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/192:
x = ('swe', 2000)
i, g = get_income_gini(x)
117/193:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
117/194:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/195:
x = ('ago', 2010)
i, g = get_income_gini(x)
117/196:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
117/197:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/198:
res1 = get_estimated_shape_2(i, g, 3)
res2 = get_shape(x)
117/199:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/200:
x = ('ago', 1981)
i, g = get_income_gini(x)
117/201:
res1 = get_estimated_shape_2(i, g, 3)
res2 = get_shape(x)
117/202:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/203:
x = ('zwe', 1981)
i, g = get_income_gini(x)
117/204:
res1 = get_estimated_shape_2(i, g, 3)
res2 = get_shape(x)
117/205:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/206:
res1 = get_estimated_shape_2(i, g, 4)
res2 = get_shape(x)
117/207:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/208:
res1 = get_estimated_shape_2(i, g, 9)
res2 = get_shape(x)
117/209:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/210:
res1 = get_estimated_shape_2(i, g, 2)
res2 = get_shape(x)
117/211:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/212:
x = ('chn', 1981)
i, g = get_income_gini(x)
117/213:
res1 = get_estimated_shape_2(i, g, 2)
res2 = get_shape(x)
117/214:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/215:
res1 = get_estimated_shape_2(i, g, 9)
res2 = get_shape(x)
117/216:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
117/217:
res1 = get_estimated_shape_2(i, g, 2)
res2 = get_shape(x)
117/218:
res1 = get_estimated_shape_2(i, g, 3)
res2 = get_shape(x)
117/219:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/1:
import pandas as pd
import numpy as np
119/2:
import matplotlib.pyplot as plt

%matplotlib inline
119/3:
import os
import os.path as osp
119/4:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
119/5:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['country', 'time'])
shapes = pd.read_csv(shapes_file)
119/6:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
shapes['country'] = shapes['country'].replace({'xkx': 'kos'})
119/7: merge = pd.concat([income, gini], axis=1)
119/8: merge
119/9:
# get all available country/year pair from povcal

unique_country_year = shapes[['country', 'year']].drop_duplicates()
119/10:
known_income = list()
known_gini = list()

for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    if r in merge.index:
        i = merge.loc[r, 'mincpcap_cppp']
        g = merge.loc[r, 'gini']
    else:
        i = np.nan
        g = np.nan
    known_income.append(i)
    known_gini.append(g)
119/11:
idx = pd.MultiIndex.from_frame(unique_country_year)
known_country_year_df = pd.DataFrame({'income': known_income, 'gini': known_gini}, index=idx)
119/12: known_country_year_df
119/13: known_country_year_df.to_csv('../wip/income_gini_for_known_shape_countries.csv')
119/14: np.log(1)
119/15:
def get_distances(income, gini):
    # get country/year pair which is close to the input income gini.
    # note: result country will not be duplicated. because one country don't change
    # often enough for a few years, so we avoid having dulicated country and nearby years
    res = np.sqrt(np.power(np.log(known_country_year_df['income']) - np.log(income), 2) + 
                  np.power(known_country_year_df['gini'] - gini, 2))
    return res.sort_values(ascending=True)
119/16: # TODO: add get_distance by rectangle
119/17:
def get_neighbors_n_countries(income, gini, n=5, radius=5):
    neis = list()
    ecs = set()
    res = get_distances(income, gini)
    res = res[(res < radius)]
    for i, v in res.iteritems():
        ecs.add(i[0])
        neis.append(i)
        if len(ecs) >= n:
            break
    if len(ecs) < n:
        print(f'can not get enough countries, only {len(ecs)} countries selected')
    return neis
119/18:
def get_neighbors(income, gini, n=10, radius=5):
    """
    """
    neis = list()
    res = get_distances(income, gini)
    res = res[(res < radius)]
    for i, v in res.iteritems():
        neis.append(i)
        if len(neis) >= n:
            break
    if len(neis) < n:
        print(f'can not get enough points, only {len(neis)} points selected')
    return neis
119/19: get_neighbors(5.3, 53)
119/20: get_neighbors(5.3, 53, 20)
119/21: get_neighbors(2, 40, 20)
119/22: # making a plot for available gini/income pair and all gini/income pair
119/23: merge_ = merge.dropna(how='any')
119/24:
def do_plot():
    ax = plt.subplot(1, 1, 1)
    ax.scatter(known_country_year_df['income'], known_country_year_df['gini'], s=1, color='#AA00FF')
    ax.scatter(merge_['mincpcap_cppp'], merge_['gini'], s=1, alpha=.1, color='grey')
    ax.set_xscale('log')
    ax.set_xlabel('income')
    ax.set_ylabel('gini')
    return ax
119/25:
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['figure.dpi'] = 196
119/26: do_plot()
119/27: # next, merging shapes and calculate mean
119/28: from functools import reduce
119/29:
def merge_2shapes(s1, s2):
    s1_, s2_ = s1.align(s2, fill_value=0)
    s3 = (s1_ + s2_) / 2
    return s3
119/30:
# this is wrong, don't do this
# def merge_nshapes(s_list):
#     return reduce(merge_2shapes, s_list)
119/31:
def merge_nshapes(s_list):
    n = len(s_list)
    res = pd.concat(s_list, axis=1).fillna(0)
    return res.mean(axis=1)
119/32: shapes
119/33: shapes_ = shapes.set_index(['country', 'year']).sort_index()
119/34:
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print('using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
            return df_nc
119/35: get_shape(('chn', 1981))
119/36: slist = get_neighbors(6, 50)
119/37: df_list = list(map(get_shape, slist))
119/38: res = merge_nshapes(df_list)
119/39: plt.plot(res)
119/40:
def bracket_number_from_income(s):
    return ((np.log2(s / 30) + 7) / delta).astype(int)
119/41:
def get_income_gini(idx):
    i = merge.loc[idx, 'mincpcap_cppp']
    g = merge.loc[idx, 'gini']
    return i, g
119/42:
def get_estimated_shape(idx, neighbours=5):
    # 1. if the idx is in known shapes, just return the known shape
    # 2. if not, return the estimated shape
    if idx in shapes_.index:
        return get_shape(idx)
    else:
        i, g = get_income_gini(idx)
        nei = get_neighbors(i, g, neighbours_n)
        slist = list(map(get_shape, nei))
        return merge_nshapes(slist)
119/43:
def get_estimated_shape_2(income, gini, neighbours=5):
    # get estimated shape based on income and gini
    nei = get_neighbors(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/44:
def get_estimated_shape_3(income, gini, neighbours=10):
    # get estimated shape based on income and gini, don't limit country exist only once
    nei = get_neighbors_no_country_limit(income, gini, neighbours+1, radius=2.5)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/45: get_estimated_shape_3(i, g, 100)
119/46:
def get_estimated_shape_3(income, gini, neighbours=10):
    # get estimated shape based on income and gini, don't limit country exist only once
    nei = get_neighbors_n_countries(income, gini, neighbours+1, radius=2.5)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/47: get_estimated_shape_3(i, g, 100)
119/48:
x = ('moz', 2001)
i, g = get_income_gini(x)
119/49:
res1 = get_estimated_shape_3(i, g, 60)
res2 = get_shape(x)
119/50:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/51:
res1 = get_estimated_shape_2(i, g, 60)
res2 = get_shape(x)
119/52:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/53:
x = ('cez', 2001)
i, g = get_income_gini(x)
119/54:
x = ('gmt', 2001)
i, g = get_income_gini(x)
119/55:
x = ('geo', 2001)
i, g = get_income_gini(x)
119/56:
res1 = get_estimated_shape_2(i, g, 60)
res2 = get_shape(x)
119/57:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/58:
res1 = get_estimated_shape_2(i, g, 70)
res2 = get_shape(x)
119/59:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/60:
res1 = get_estimated_shape_2(i, g, 50)
res2 = get_shape(x)
119/61:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/62:
res1 = get_estimated_shape_2(i, g, 10)
res2 = get_shape(x)
119/63:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/64:
res1 = get_estimated_shape_2(i, g, 5)
res2 = get_shape(x)
119/65:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/66:
res1 = get_estimated_shape_3(i, g, 5)
res2 = get_shape(x)
119/67:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/68:
def get_estimated_shape_3(income, gini, neighbours=10):
    # get estimated shape based on income and gini, don't limit country exist only once
    nei = get_neighbors_n_countries(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/69:
res1 = get_estimated_shape_3(i, g, 5)
res2 = get_shape(x)
119/70:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/71:
x = ('geo', 1990)
i, g = get_income_gini(x)
119/72:
res1 = get_estimated_shape_3(i, g, 5)
res2 = get_shape(x)
119/73:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/74:
res1 = get_estimated_shape_2(i, g, 50)
res2 = get_shape(x)
119/75:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/76:
res1 = get_estimated_shape_2(i, g, 70)
res2 = get_shape(x)
119/77:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/78:
x = ('geo', 1990)
i, g = get_income_gini(x)
res1 = get_estimated_shape_2(i, g, 70)
res2 = get_shape(x)
119/79:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/80:
x = ('geo', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_2(i, g, 70)
res2 = get_shape(x)
119/81:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/82:
x = ('geo', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_2(i, g, 100)
res2 = get_shape(x)
119/83:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/84:
x = ('geo', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_2(i, g, 200)
res2 = get_shape(x)
119/85:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/86:
x = ('geo', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/87:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/88:
x = ('yog', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/89:
x = ('yem', 1992)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/90:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/91:
x = ('yem', 1989)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/92:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/93:
x = ('yem', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/94:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/95:
x = ('yem', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 10)
res2 = get_shape(x)
119/96:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/97:
x = ('zwe', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 10)
res2 = get_shape(x)
119/98:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/99:
x = ('zwe', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 20)
res2 = get_shape(x)
119/100:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/101:
x = ('zwe', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 10)
res2 = get_shape(x)
119/102:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/103:
x = ('geo', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 10)
res2 = get_shape(x)
119/104:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/105:
def get_neighbors_n_countries(income, gini, n=5, radius=5, only_once=False):
    neis = list()
    ecs = set()
    res = get_distances(income, gini)
    res = res[(res < radius)]
    for i, v in res.iteritems():
        if only_once and i[0] in ecs:
            continue
        ecs.add(i[0])
        neis.append(i)
        if len(ecs) >= n:
            break
    if len(ecs) < n:
        print(f'can not get enough countries, only {len(ecs)} countries selected')
    return neis
119/106:
def get_estimated_shape_3(income, gini, neighbours=5):
    # get estimated shape based on income and gini
    nei = get_neighbors(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/107:
def get_estimated_shape_4(income, gini, neighbours=10):
    # get estimated shape based on income and gini, don't limit country exist only once
    nei = get_neighbors_n_countries(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/108:
def get_estimated_shape_3(income, gini, neighbours=5):
    # get estimated shape based on income and gini
    nei = get_neighbors_n_countries(income, gini, neighbours+1)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/109:
def get_estimated_shape_4(income, gini, neighbours=10):
    # get estimated shape based on income and gini
    nei = get_neighbors_n_countries(income, gini, neighbours+1, only_once=True)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/110:
x = ('geo', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/111:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/112:
x = ('geo', 2017)
i, g = get_income_gini(x)
res1 = get_estimated_shape_3(i, g, 10)
res2 = get_shape(x)
119/113:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/114:
x = ('geo', 1990)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/115:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/116:
x = ('geo', 1990)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 30)
res2 = get_shape(x)
119/117:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/118:
x = ('geo', 1995)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 30)
res2 = get_shape(x)
119/119:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/120:
x = ('geo', 1995)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/121:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/122:
x = ('geo', 1990)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/123:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/124:
# x = ('geo', 1990) is outliner, can be checked later
x = ('usa', 1990)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/125:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/126:
# x = ('geo', 1990) is outliner, can be checked later
x = ('usa', 2000)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
119/127:
plt.plot(res1, label='estimated')
plt.plot(res2, label='actual')
plt.legend()
119/128:
def get_estimated_shape_4(income, gini, neighbours=10):
    # get estimated shape based on income and gini
    nei = get_neighbors_n_countries(income, gini, neighbours+1, only_once=True)
    print(nei)
    slist = list(map(get_shape, nei[1:]))
    return merge_nshapes(slist)
119/129:
# x = ('geo', 1990) is outliner, can be checked later
x = ('usa', 2000)
i, g = get_income_gini(x)
res1 = get_estimated_shape_4(i, g, 10)
res2 = get_shape(x)
120/1:
import os
import os.path as osp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

%matplotlib inline
120/2:
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['figure.dpi'] = 96
120/3:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
120/4:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])
gini.index.names = ['country', 'time']
shapes = pd.read_csv(shapes_file)
120/5:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
shapes['country'] = shapes['country'].replace({'xkx': 'kos'})
120/6:
# create a sorted version
shapes_ = shapes.set_index(['country', 'year']).sort_index()
120/7: merge = pd.concat([income, gini], axis=1)
120/8:
# get all available country/year pair from povcal

unique_country_year = shapes[['country', 'year']].drop_duplicates()

known_income = list()
known_gini = list()

for _, row in unique_country_year.iterrows():
    r = (row['country'], row['year'])
    if r in merge.index:
        i = merge.loc[r, 'mincpcap_cppp']
        g = merge.loc[r, 'gapminder_gini']
    else:
        i = np.nan
        g = np.nan
    known_income.append(i)
    known_gini.append(g)
    
idx = pd.MultiIndex.from_frame(unique_country_year)
known_country_year_df = pd.DataFrame({'income': known_income, 'gini': known_gini}, index=idx)
120/9: known_country_year_df
120/10:
# calculate distances from one point to all known income/gini points as in 2D plane.
# the distance is calculated by sqrt(gini_distance^2 + income_distance^2)
# and gini_distance and income_distance are standarized so that 1 means +/- 10% 

def get_distances(income, gini):
    """distance in 2D plane"""
    gini_distances = (known_country_year_df['gini'] - gini) / gini * 10
    # income: maybe better to use log scale?
    income_distances = (np.log(known_country_year_df['income']) - np.log(income)) * 10
    # income_distances = (known_country_year_df['income'] - income) / income * 10
    
    
    res = np.sqrt(np.power(income_distances, 2) + 
                  np.power(gini_distances, 2))
    return res.sort_values(ascending=True).dropna()
120/11:
# for example
get_distances(5.3, 53)
120/12: # TODO: add get_neighbors by rectangle
120/13:
# get n neighbor countries
# radius: max distance allowed
# only_once: only include one country for once
# n: max countries allowed
def get_neighbors_n_countries_circle(income, gini, n=5, radius=1.414, only_once=False):
    neis = list()
    ecs = set()
    res = get_distances(income, gini)
    res = res[(res < radius) & (res > 0)]  # droping res= 0, because exact match means it's same point
    for i, v in res.iteritems():
        if only_once and i[0] in ecs:
            continue
        ecs.add(i[0])
        neis.append(i)
        if len(ecs) >= n:
            break
    if len(ecs) < n:
        print(f'can not get enough countries, only {len(ecs)} countries selected')
    return neis
120/14: get_neighbors_n_countries_circle(5.3, 53, 5)
120/15: get_neighbors_n_countries_circle(5.3, 53, 5, only_once=True)
120/16:
# get n neighbor points
# radius: max distance allowed
# n: max points allowed
def get_neighbors_n_points_circle(income, gini, n=10, radius=1.414):
    neis = list()
    res = get_distances(income, gini)
    res = res[(res < radius) & (res > 0)]
    for i, v in res.iteritems():
        neis.append(i)
        if len(neis) >= n:
            break
    if len(neis) < n:
        print(f'can not get enough points, only {len(neis)} points selected')
    return neis
120/17: get_neighbors_n_points_circle(5.3, 53, 5)
120/18: get_neighbors_n_points_circle(5.3, 53, 6)
120/19: merge
120/20:
merge_ = merge.dropna(how='any')
merge_.columns =  ['income', 'gini']
120/21:
p1 = merge_['income'] > 5.3 - 5
p2 = merge_['income'] < 5.3 + 5

p3 = merge_['gini'] > 53 - 5
p4 = merge_['gini'] < 53 + 5

np.all([p1, p2, p3, p4])
120/22:
p1 = merge_['income'] > 5.3 - 5
p2 = merge_['income'] < 5.3 + 5

p3 = merge_['gini'] > 53 - 5
p4 = merge_['gini'] < 53 + 5

merge_[p1 & p2 & p3 & p4]
120/23:
merge_ = known_country_year_df.dropna(how='any')
merge_.columns =  ['income', 'gini']
120/24:
p1 = merge_['income'] > 5.3 - 5
p2 = merge_['income'] < 5.3 + 5

p3 = merge_['gini'] > 53 - 5
p4 = merge_['gini'] < 53 + 5

merge_[p1 & p2 & p3 & p4]
120/25:
p1 = merge_['income'] > 5.3 - 5
p2 = merge_['income'] < 5.3 + 5

p3 = merge_['gini'] > 53 - 5
p4 = merge_['gini'] < 53 + 5

merge_[p1 & p2 & p3 & p4].index.values
120/26:
def get_neighbors_rectangle(income, gini, n=0, plus_minus=2):
    p1 = kcy['income'] > income - plus_minus
    p2 = kcy['income'] < income + plus_minus
    
    p3 = kcy['gini'] > gini - plus_minus
    p4 = kcy['gini'] < gini + plus_minus
    
    return kcy[p1 & p2 & p3 & p4].index.tolist()
120/27: get_neighbors_rectangle(5.3, 53, 1)
120/28:
kcy = known_country_year_df.dropna(how='any')
kcy.columns =  ['income', 'gini']
120/29:
p1 = kcy['income'] > 5.3 - 5
p2 = kcy['income'] < 5.3 + 5

p3 = kcy['gini'] > 53 - 5
p4 = kcy['gini'] < 53 + 5

kcy[p1 & p2 & p3 & p4].index.values
120/30:
def get_neighbors_rectangle(income, gini, plus_minus=2, n=0):
    p1 = kcy['income'] > income - plus_minus
    p2 = kcy['income'] < income + plus_minus
    
    p3 = kcy['gini'] > gini - plus_minus
    p4 = kcy['gini'] < gini + plus_minus
    
    return kcy[p1 & p2 & p3 & p4].index.tolist()
120/31: get_neighbors_rectangle(5.3, 53, 1)
120/32:
def get_income_gini(idx):
    i = merge.loc[idx, 'mincpcap_cppp']
    g = merge.loc[idx, 'gapminder_gini']
    return i, g
120/33:
# function for merging shapes
# s_list: a list of shapes objects (pd.Series)
def merge_nshapes(s_list):
    n = len(s_list)
    res = pd.concat(s_list, axis=1).fillna(0)
    return res.mean(axis=1)
120/34:
# get shape from known shapes
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
            if t in 'ur':
                print(f'{idx}: using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
            return df_nc
120/35:
def get_estimated_shape(idx, method, use_known_shape=False, **kwargs):
    # 1. if the idx is in known shapes and use_known_shape=True, just return the known shape
    # 2. if not, return the estimated shape by given method. kwargs will be used as the given
    # method's parameters.
    if idx in shapes_.index:
        if use_known_shape:
            return get_shape(idx)
        else:
            i, g = get_income_gini(idx)
            nei = method(i, g, **kwargs)
            nei = [x for x in nei if x != idx]  # remove itself from neighbours
            print(f'using {len(nei)} shapes')
            # print(nei)
            slist = list(map(get_shape, nei))
            return merge_nshapes(slist)
    else:
        i, g = get_income_gini(idx)
        nei = method(i, g, **kwargs)
        print(f'using {len(nei)} shapes')
        slist = list(map(get_shape, nei))
        return merge_nshapes(slist)
120/36:
# example usage:
x = ('usa', 1990)
i, g = get_income_gini(x)
res1 = get_shape(x)
res2 = get_estimated_shape(x, get_neighbors_n_countries, n=10)
120/37:
# example usage:
x = ('usa', 1990)
i, g = get_income_gini(x)
res1 = get_shape(x)
res2 = get_estimated_shape(x, get_neighbors_n_countries_circle, n=10)
120/38:
# function for telling difference between estimated and actual shapes
def mean_square_for_2shapes(s1, s2):
    s1_, s2_ = s1.align(s2)
    rms = np.sqrt(np.sum(np.power(s1_ - s2_, 2)))
    return rms
120/39: mean_square_for_2shapes(res1, res2)
120/40:
def do_plot(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):
        print(f'n = {n}')
        plt.subplot(2, 2, i+1)
        res2 = get_estimated_shape(x, method, n=n, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"n={n}, rms={rms:.6f}")
        plt.legend()
120/41:
def do_plot2(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):
        print(f'n = {n}')
        plt.subplot(2, 2, i+1)
        res2 = get_estimated_shape(x, method, plus_minus=n, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"n={n}, rms={rms:.6f}")
        plt.legend()
120/42:
ns = [0.5, 1, 2, 5]
x = ('bra', 1990)


do_plot2(x, ns, get_neighbors_rectangle)
120/43:
ns = [0.5, 1, 2, 5]
x = ('mwi', 1991)


do_plot2(x, ns, get_neighbors_rectangle)
120/44: ## question: is gini more important or income more important?
120/45:
def get_neighbors_rectangle2(income, gini, delta_i=2, delta_g=2, n=0):
    
    if not detla_i and not delta_g:
        return kcy.index.tolist()
    
    if delta_i:    
        p1 = kcy['income'] > income - delta_i
        p2 = kcy['income'] < income + delta_i
        if delta_g:    
            p3 = kcy['gini'] > gini - delta_g
            p4 = kcy['gini'] < gini + delta_g    
            return kcy[p1 & p2 & p3 & p4].index.tolist()
        else:
            return kcy[p1 & p2].index.tolist()
    else:
        p3 = kcy['gini'] > gini - delta_g
        p4 = kcy['gini'] < gini + delta_g    
        return kcy[p3 & p4].index.tolist()
120/46:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(2, 2, i+1)
        res2 = get_estimated_shape(x, method, plus_minus=n, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
120/47:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot2(x, ns, get_neighbors_rectangle)
120/48:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot2(x, ns, get_neighbors_rectangle2)
120/49:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(2, 2, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
120/50:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot2(x, ns, get_neighbors_rectangle2)
120/51:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/52:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(2, 2, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
120/53:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/54:
def get_neighbors_rectangle2(income, gini, delta_i=2, delta_g=2, n=0):
    
    if not delta and not delta_g:
        return kcy.index.tolist()
    
    if delta_i:    
        p1 = kcy['income'] > income - delta_i
        p2 = kcy['income'] < income + delta_i
        if delta_g:    
            p3 = kcy['gini'] > gini - delta_g
            p4 = kcy['gini'] < gini + delta_g    
            return kcy[p1 & p2 & p3 & p4].index.tolist()
        else:
            return kcy[p1 & p2].index.tolist()
    else:
        p3 = kcy['gini'] > gini - delta_g
        p4 = kcy['gini'] < gini + delta_g    
        return kcy[p3 & p4].index.tolist()
120/55:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/56:
def get_neighbors_rectangle2(income, gini, delta_i=2, delta_g=2, n=0):
    
    if not delta_i and not delta_g:
        return kcy.index.tolist()
    
    if delta_i:    
        p1 = kcy['income'] > income - delta_i
        p2 = kcy['income'] < income + delta_i
        if delta_g:    
            p3 = kcy['gini'] > gini - delta_g
            p4 = kcy['gini'] < gini + delta_g    
            return kcy[p1 & p2 & p3 & p4].index.tolist()
        else:
            return kcy[p1 & p2].index.tolist()
    else:
        p3 = kcy['gini'] > gini - delta_g
        p4 = kcy['gini'] < gini + delta_g    
        return kcy[p3 & p4].index.tolist()
120/57:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/58:
ns = [
    (0.5, None),
    (0.5, 1),
    (None, 1),
    (None, 0.5)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/59:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(3, 3, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
120/60:
ns = [
    (0.5, None),
    (1, None),
    (5, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/61:
plt.rcParams['figure.figsize'] = (12, 10)
plt.rcParams['figure.dpi'] = 96
120/62:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/63:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/64:
# get shape from known shapes
def get_shape(idx):
    df = shapes_.loc[idx]
    for t in 'naur':
        if t in df['coverage_type'].values:
#             if t in 'ur':
#                 print(f'{idx}: using urban/rural data')
            df_nc = df[df['coverage_type'] == t]
            df_nc = df_nc.set_index('bracket')['population_percentage']
            return df_nc
120/65:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('swe', 2015)


do_plot3(x, ns, get_neighbors_rectangle2)
120/66:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/67:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/68:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('swe', 2015)


do_plot3(x, ns, get_neighbors_rectangle2)
120/69: kyc['gini'].describe()
120/70: kcy['gini'].describe()
120/71: kcy['income'].describe()
120/72:
kcy = known_country_year_df.dropna(how='any')
kcy.columns =  ['income', 'gini']
kcy['income'] = np.log2(kcy['income'])
120/73:
kcy = known_country_year_df.dropna(how='any').copy()
kcy.columns =  ['income', 'gini']
kcy['income'] = np.log2(kcy['income'])
120/74:
p1 = kcy['income'] > 5.3 - 1
p2 = kcy['income'] < 5.3 + 1

p3 = kcy['gini'] > np.log2(53) - 1
p4 = kcy['gini'] < np.log2(53) + 1

kcy[p1 & p2 & p3 & p4].index.values
120/75:
p1 = kcy['income'] > np.log2(5.3) - 1
p2 = kcy['income'] < np.log2(5.3) + 1

p3 = kcy['gini'] > 53 - 1
p4 = kcy['gini'] < 53 + 1

kcy[p1 & p2 & p3 & p4].index.values
120/76:
def get_neighbors_rectangle2(income, gini, delta_i=2, delta_g=2, n=0):
    
    if not delta_i and not delta_g:
        return kcy.index.tolist()
    
    income = np.log(income)
    
    if delta_i:    
        p1 = kcy['income'] > income - delta_i
        p2 = kcy['income'] < income + delta_i
        if delta_g:    
            p3 = kcy['gini'] > gini - delta_g
            p4 = kcy['gini'] < gini + delta_g    
            return kcy[p1 & p2 & p3 & p4].index.tolist()
        else:
            return kcy[p1 & p2].index.tolist()
    else:
        p3 = kcy['gini'] > gini - delta_g
        p4 = kcy['gini'] < gini + delta_g    
        return kcy[p3 & p4].index.tolist()
120/77:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/78:
ns = [
    (0.5, None),
    (1, None),
    (2, None),
    (None, 0.5),
    (1, 0.5),
    (5, 0.5),
    (None, 5),
    (1, 5),
    (5, 5)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/79: get_neighbors_rectangle2(5.3, 53, 1, 5)
120/80: kcy
120/81: kcy['income'].describe()
120/82:
ns = [
    (0.2, None),
    (0.5, None),
    (1, None),
    (None, 2),
    (0.5, 2),
    (1, 2),
    (None, 5),
    (0.5, 5),
    (1, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/83:
ns = [
    (0.2, None),
    (0.5, None),
    (1, None),
    (None, 2),
    (0.5, 2),
    (1, 2),
    (None, 5),
    (0.5, 5),
    (1, 5)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/84:
ns = [
    (0.2, None),
    (0.5, None),
    (1, None),
    (None, 2),
    (0.5, 2),
    (1, 2),
    (None, 5),
    (0.5, 5),
    (1, 5)
]
x = ('swe', 2015)


do_plot3(x, ns, get_neighbors_rectangle2)
120/85:
ns = [
    (0.2, None),
    (0.5, None),
    (1, None),
    (None, 2),
    (0.5, 2),
    (1, 2),
    (None, 5),
    (0.5, 5),
    (1, 5)
]
x = ('tjk', 2000)


do_plot3(x, ns, get_neighbors_rectangle2)
120/86:
ns = [
    (0.2, None),
    (0.5, None),
    (1, None),
    (None, 2),
    (0.5, 2),
    (1, 2),
    (None, 5),
    (0.5, 5),
    (1, 5)
]
x = ('caf', 1993)


do_plot3(x, ns, get_neighbors_rectangle2)
120/87:
ns = [
    (0.2, 1),
    (0.5, 1),
    (1, 1),
    (0.1, 2),
    (0.5, 2),
    (1, 2),
    (0.1, 5),
    (0.5, 5),
    (1, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/88:
ns = [
    (0.2, 10),
    (0.5, 10),
    (1, 10),
    (0.1, 2),
    (0.5, 2),
    (1, 2),
    (0.1, 5),
    (0.5, 5),
    (1, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/89:
ns = [
    (0.2, 10),
    (0.5, 10),
    (1, 10),
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/90:
ns = [
    (0.5, 10),
    (1, 10),
    (2, 10),
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/91:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 10),
    (1, 10),
    (2, 10),
    (0.5, 5),
    (1, 5),
    (2, 5)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/92:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10),
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/93:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/94:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('swe', 2015)


do_plot3(x, ns, get_neighbors_rectangle2)
120/95:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('tjk', 2000)


do_plot3(x, ns, get_neighbors_rectangle2)
120/96:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('caf', 1993)


do_plot3(x, ns, get_neighbors_rectangle2)
120/97:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('tha', 1989)


do_plot3(x, ns, get_neighbors_rectangle2)
120/98:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('tha', 1981)


do_plot3(x, ns, get_neighbors_rectangle2)
120/99:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    plt.title(f"{x[0]}, {x[1]}")
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(3, 3, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
120/100:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/101:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(3, 3, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
        
    plt.subtitle(f"{x[0]}, {x[1]}")
120/102:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/103:
def do_plot3(x, ns, method, **kwargs):
    i, g = get_income_gini(x)
    print(x)
    print(f"i = {i}, g = {g}")
    res1 = get_shape(x)
    
    for i, n in enumerate(ns):          
        di, dg = n
        print(f'delta_i = {di}, delta_g = {dg}')
        plt.subplot(3, 3, i+1)
        res2 = get_estimated_shape(x, method, delta_i=di, delta_g=dg, **kwargs)
        rms = mean_square_for_2shapes(res1, res2)
        plt.plot(res1, label='actual')
        plt.plot(res2, label='estimated')
        plt.title(f"delta_i={di}, delta_g={dg}")
        plt.legend()
        
    plt.suptitle(f"{x[0]}, {x[1]}")
120/104:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('mwi', 1991)


do_plot3(x, ns, get_neighbors_rectangle2)
120/105:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('bra', 1990)


do_plot3(x, ns, get_neighbors_rectangle2)
120/106:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('swe', 2015)


do_plot3(x, ns, get_neighbors_rectangle2)
120/107:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('tjk', 2000)


do_plot3(x, ns, get_neighbors_rectangle2)
120/108:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('caf', 1993)


do_plot3(x, ns, get_neighbors_rectangle2)
120/109:
ns = [
    (0.5, 2),
    (1, 2),
    (2, 2),
    (0.5, 5),
    (1, 5),
    (2, 5),
    (0.5, 10),
    (1, 10),
    (2, 10)
]
x = ('tha', 1981)


do_plot3(x, ns, get_neighbors_rectangle2)
120/110: get_neighbors_rectangle(5.3, 53, 0.5, 5)
120/111: get_neighbors_rectangle2(5.3, 53, 0.5, 5)
120/112: get_neighbors_rectangle2(3.4, 61, 0.5, 5)
120/113: get_neighbors_rectangle2(3.4, 61, 0.5, 2)
120/114: known_country_year_df
120/115: known_country_year_df['income'] = np.log2(known_country_year_df['income'])
120/116: known_country_year_df.to_csv('../wip/income_gini_for_known_shape_countries.csv')
122/1:
import pandas as pd
import numpy as np
122/2:
%load_ext autoreload
%autoreload 2
122/3:
import sys
sys.path.insert(0, '../scripts/')
122/4:
%load_ext autoreload
%autoreload 1
%aimport shapeslib
122/5: %aimport shapeslib
122/6: get_distances = shapeslib.get_distances
122/7: known_shapes = pd.read_csv('../wip/income_gini_for_known_shape_countries.csv')
122/8: known_shapes
122/9: known_shapes = pd.read_csv('../wip/income_gini_for_known_shape_countries.csv').set_index(['country', 'year'])
122/10: known_shapes
122/11: get_distances(5.3, 53, know_shapes)
122/12: get_distances(5.3, 53, known_shapes)
122/13: get_distances(5.3, 53)
122/14: get_neighbors = shapeslib.get_neighbors
122/15: get_neighbors(5.3, 53, 10)
122/16: get_neighbors(3, 61, 10)
122/17:
shapes_file = shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
122/18: known_shapes
122/19:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
shapes['country'] = shapes['country'].replace({'xkx': 'kos'})
122/20:
# convert xkx to kos. Same country, but povcal and open-numbers use different code
known_shapes['country'] = known_shapes['country'].replace({'xkx': 'kos'})
122/21:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
122/22:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
122/23:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])
gini.index.names = ['country', 'time']
122/24:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])
gini.index.names = ['country', 'time']

income.columns = ['income']
gini.columns = ['gini']
122/25: income_gini = pd.concat([income, gini], axis=1)
122/26: income_gini
122/27:
import matplotlib.pyplot as plt

%matplotlib inline
122/28:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
122/29: shapeslib.get_average_shape(('ago', 1980), shapes, income_gini)
122/30: res = shapeslib.get_average_shape(('ago', 1980), shapes, income_gini)
122/31: plt.plot(res)
122/32: res = shapeslib.get_average_shape(('geo', 1980), shapes, income_gini)
122/33: plt.plot(res)
122/34: res = shapeslib.get_average_shape(('swe', 2015), shapes, income_gini)
122/35: plt.plot(res)
122/36: res = shapeslib.get_average_shape(('mwi', 1990), shapes, income_gini)
122/37: plt.plot(res)
122/38: shapes
122/39:
def get_first_known_shape(country):
    df = shapes.loc[country]
    first_year = df.index.get_level_values('year').iloc[0]
    return shapeslib.get_shape((country, first_year), shapes)
122/40: get_first_known_shape('ago')
122/41:
def get_first_known_shape(country):
    df = shapes.loc[country]
    first_year = df.index.get_level_values('year')[0]
    return shapeslib.get_shape((country, first_year), shapes)
122/42: get_first_known_shape('ago')
122/43:
def get_first_known_shape(country):
    df = shapes.loc[country]
    first_year = df.index.get_level_values('year')[0]
    print(first_year)
    return shapeslib.get_shape((country, first_year), shapes)
122/44: get_first_known_shape('ago')
122/45:
def get_first_known_shape(country):
    df = shapes.loc[country]
    if df.empty:
        print('no shapes for country')
        return None
    first_year = df.index.get_level_values('year')[0]
    print(first_year)
    return shapeslib.get_shape((country, first_year), shapes)
122/46: get_first_known_shape('att')
122/47:
def get_first_known_shape(country):
    try:
        df = shapes.loc[country]
    except KeyError:
        print('no shapes for country')
        return None
    first_year = df.index.get_level_values('year')[0]
    print(first_year)
    return shapeslib.get_shape((country, first_year), shapes)
122/48: get_first_known_shape('att')
122/49:
def get_first_known_shape(country):
    try:
        df = shapes.loc[country]
    except KeyError:
        print(f'no shapes for country {country}')
        return None
    first_year = df.index.get_level_values('year')[0]
    print(first_year)
    return shapeslib.get_shape((country, first_year), shapes)
122/50: get_first_known_shape('att')
122/51: get_first_known_shape('aze')
122/52:
xs = [1, 2, 3]
ys = [4, 5, 6]

[(x, y) for x in xs, y in ys]
122/53:
xs = [1, 2, 3]
ys = [4, 5, 6]

[(x, y) for x in xs for y in ys]
122/54:
s0 = get_first_known_shape('aze')
s1 = shapeslib.get_average_shape(('aze', 1980))
122/55:
s0 = get_first_known_shape('aze')
s1 = shapeslib.get_average_shape(('aze', 1980), shapes, income_gini)
122/56:
plt.plot(s0)
plt.plot(s1)
122/57:
s0 = get_first_known_shape('aze')
s1 = shapeslib.get_average_shape(('aze', 1970), shapes, income_gini)
122/58:
plt.plot(s0)
plt.plot(s1)
122/59: s2 = shapeslib.merge_nshapes_with_weights([s0, s1], [0.8, 0.2])
122/60:
plt.plot(s0, label='1981')
plt.plot(s1, label='1970-average')
plt.plot(s2, label='mixed')

plt.legend()
122/61: s2 = shapeslib.merge_nshapes_with_weights([s0, s1], [0.8, 0.2])
122/62:
plt.plot(s0, label='1981')
plt.plot(s1, label='1970-average')
plt.plot(s2, label='mixed')

plt.legend()
122/63:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_average_shape(('mwi', 1970), shapes, income_gini)
122/64: s2 = shapeslib.merge_nshapes_with_weights([s0, s1], [0.8, 0.2])
122/65:
plt.plot(s0, label='1981')
plt.plot(s1, label='1970-average')
plt.plot(s2, label='mixed')

plt.legend()
122/66:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_average_shape(('mwi', 1970), shapes, income_gini)
s3 = shapeslib.get_average_shape(('mwi', 1960), shapes, income_gini)
122/67:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')

plt.legend()
122/68:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], [0.8, 0.2])
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], [0.5, 0.5])
122/69:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')

plt.legend()
122/70: 0.2 / 20
122/71:
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        w = (1981 - y) * 0.01
        return (1-w, w)
122/72: get_weights(1980)
122/73:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1961))
122/74:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')

plt.legend()
122/75: 1960 - 1870
122/76: get_weights(1961)
122/77: 0.8 / 90
122/78: 0.9 / 90
122/79: 0.1 / 20
122/80:
# the linear version
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        step = 0.005
        w = (1981 - y) * step
        return (1-w, w)
    if y > 1870 and y <= 1960:
        step = 0.01
        w = (1961 - y) * step
    else:
        return (1, 0)
122/81: get_weights(1961)
122/82: get_weights(1871)
122/83:
# the linear version
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        step = 0.005
        w = (1981 - y) * step
        return (1-w, w)
    if y > 1870 and y <= 1960:
        step = 0.01
        w = (1961 - y) * step
        return (1-w, w)
    else:
        return (1, 0)
122/84: get_weights(1871)
122/85: get_weights(1870)
122/86: get_weights(1872)
122/87:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_average_shape(('mwi', 1970), shapes, income_gini)
s3 = shapeslib.get_average_shape(('mwi', 1960), shapes, income_gini)
s5 = shapeslib.get_average_shape(('mwi', 1920), shapes, income_gini)
s7 = shapeslib.get_average_shape(('mwi', 1900), shapes, income_gini)
s9 = shapeslib.get_average_shape(('mwi', 1871), shapes, income_gini)
s11 = shapeslib.get_average_shape(('mwi', 1820), shapes, income_gini)
122/88:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/89:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')
plt.plot(s6, label='1920')
plt.plot(s8, label='1900')
plt.plot(s10, label='1871')
plt.plot(s12, label='1820')

plt.legend()
122/90:
plt.plot(s0, label='1981')
#plt.plot(s2, label='1970')
#plt.plot(s4, label='1960')
plt.plot(s6, label='1920')
plt.plot(s8, label='1900')
plt.plot(s10, label='1871')
plt.plot(s12, label='1820')

plt.legend()
122/91:
# the linear version
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        step = 0.005
        w = (1981 - y) * step
        return (1-w, w)
    if y > 1870 and y <= 1960:
        step = 0.01
        w = (1961 - y) * step
        return (1-w, w)
    else:
        return (0, 1)
122/92:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/93:
plt.plot(s0, label='1981')
#plt.plot(s2, label='1970')
#plt.plot(s4, label='1960')
plt.plot(s6, label='1920')
plt.plot(s8, label='1900')
plt.plot(s10, label='1871')
plt.plot(s12, label='1820')

plt.legend()
122/94:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')
plt.plot(s6, label='1920')
plt.plot(s8, label='1900')
plt.plot(s10, label='1871')
plt.plot(s12, label='1820')

plt.legend()
122/95:
# the linear version
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        step = 0.005
        w = (1981 - y) * step
        return (1-w, w)
    if y > 1870 and y <= 1960:
        step = 0.01
        w = (1961 - y) * step
        return (0.9-w, w)
    else:
        return (0, 1)
122/96: get_weights(1872)
122/97:
# the linear version
def get_weights(y):
    if y > 1960 and y <= 1980:  # 1961 - 1980
        step = 0.005
        w = (1981 - y) * step
        return (1-w, w)
    if y > 1870 and y <= 1960:
        step = 0.01
        w = 0.1 + (1961 - y) * step
        return (1-w, w)
    else:
        return (0, 1)
122/98: get_weights(1872)
122/99: get_weights(1871)
122/100: get_weights(1870)
122/101:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/102:
plt.plot(s0, label='1981')
plt.plot(s2, label='1970')
plt.plot(s4, label='1960')
plt.plot(s6, label='1920')
plt.plot(s8, label='1900')
plt.plot(s10, label='1871')
plt.plot(s12, label='1820')

plt.legend()
122/103:
all_brackets = np.logspace(-7, 13, 200, endpoint=True, base=2)
brackets = pd.DataFrame({'start': all_brackets, 'end': pd.Series(all_brackets).shift(-1)})
brackets_log = brackets.iloc[:-1].copy()
brackets_log['start'] = np.log2(brackets_log['start'])
brackets_log['end'] = np.log2(brackets_log['end'])
brackets_log['d'] = brackets_log.end - brackets_log.start
delta = brackets_log['d'].iloc[0]
122/104: brackets_log
122/105: d
122/106: delta
122/107:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
122/108: s0
122/109: s0.index + 1
122/110:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_estimated_mountain(('mwi', 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain(('mwi', 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain(('mwi', 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain(('mwi', 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain(('mwi', 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain(('mwi', 1820), shapes, income_gini)
122/111:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_average_shape(('mwi', 1970), shapes, income_gini)
s3 = shapeslib.get_average_shape(('mwi', 1960), shapes, income_gini)
s5 = shapeslib.get_average_shape(('mwi', 1920), shapes, income_gini)
s7 = shapeslib.get_average_shape(('mwi', 1900), shapes, income_gini)
s9 = shapeslib.get_average_shape(('mwi', 1871), shapes, income_gini)
s11 = shapeslib.get_average_shape(('mwi', 1820), shapes, income_gini)
122/112:
s0 = get_first_known_shape('mwi')
s1 = shapeslib.get_estimated_mountain(('mwi', 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain(('mwi', 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain(('mwi', 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain(('mwi', 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain(('mwi', 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain(('mwi', 1820), shapes, income_gini)
122/113:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/114:
country = 'ago'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/115:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/116:
country = 'chn'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/117:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/118: # next, standardize shape to 0-200 and resample to 0-50
122/119:
country = 'zwe'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/120:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/121:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/122:
country = 'bra'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/123:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/124:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/125:
def merge_povcal_and_average_shape(povcal_shape, average_shape, weight_povcal_shape, weight_average_shape):
    pass
122/126:
country = 'bra'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/127:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/128: s1
122/129:
country = 'bra'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/130:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/131: s1
122/132: s3
122/133: s5
122/134: s11
122/135: s9
122/136: s9.resample?
122/137: s9[::3]
122/138:
plt.plot(s9[::3])
plt.plot(s9)
122/139:
plt.plot(s9[::4])
plt.plot(s9)
122/140: s9.loc[194:200] = 0
122/141: s9
122/142: s9.reindex(pd.Index(range(200)), method='bfill', fill_value=0)
122/143: s9 = s9.reindex(pd.Index(range(200)), method='bfill', fill_value=0)
122/144:
plt.plot(s9[::4])
plt.plot(s9)
122/145: s9.split(50)
122/146: s9.values.split(50)
122/147: np.split(s9, 50)
122/148: np.split(s9, 50).apply(np.mean)
122/149: map(np.split(s9, 50), np.mean)
122/150: map(np.mean, np.split(s9, 50))
122/151: list(map(np.mean, np.split(s9, 50)))
122/152: s9_ = list(map(np.mean, np.split(s9, 50)))
122/153:
plt.plot(s9_)
plt.plot(s9)
122/154: s9_ = pd.Series(list(map(np.mean, np.split(s9, 50))), index=s9.index.values[::4])
122/155:
plt.plot(s9_)
plt.plot(s9)
122/156: s9_ = pd.Series(list(map(lambda x: max(x), np.split(s9, 50))), index=s9.index.values[::4])
122/157:
plt.plot(s9_)
plt.plot(s9)
122/158:
country = 'ind'
s0 = get_first_known_shape(country)
s1 = shapeslib.get_estimated_mountain((country, 1970), shapes, income_gini)
s3 = shapeslib.get_estimated_mountain((country, 1960), shapes, income_gini)
s5 = shapeslib.get_estimated_mountain((country, 1920), shapes, income_gini)
s7 = shapeslib.get_estimated_mountain((country, 1900), shapes, income_gini)
s9 = shapeslib.get_estimated_mountain((country, 1871), shapes, income_gini)
s11 = shapeslib.get_estimated_mountain((country, 1820), shapes, income_gini)
122/159:
s2 = shapeslib.merge_nshapes_with_weights([s0, s1], get_weights(1970))
s4 = shapeslib.merge_nshapes_with_weights([s0, s3], get_weights(1960))
s6 = shapeslib.merge_nshapes_with_weights([s0, s5], get_weights(1920))
s8 = shapeslib.merge_nshapes_with_weights([s0, s7], get_weights(1900))
s10 = shapeslib.merge_nshapes_with_weights([s0, s9], get_weights(1871))
s12 = shapeslib.merge_nshapes_with_weights([s0, s11], get_weights(1820))
122/160:
# plt.plot(s0, label='1981')
plt.plot(s1, label='1970')
plt.plot(s3, label='1960')
plt.plot(s5, label='1920')
plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/161:
# plt.plot(s0, label='1981')
# plt.plot(s1, label='1970')
# plt.plot(s3, label='1960')
# plt.plot(s5, label='1920')
# plt.plot(s7, label='1900')
plt.plot(s9, label='1871')
plt.plot(s11, label='1820')

plt.legend()
122/162: s9_ = pd.Series(list(map(lambda x: x[-1], np.split(s9, 50))), index=s9.index.values[::4])
122/163: s9 = s9.reindex(pd.Index(range(200)), method='bfill', fill_value=0)
122/164: s9_ = pd.Series(list(map(lambda x: x[-1], np.split(s9, 50))), index=s9.index.values[::4])
122/165: s9_ = pd.Series(list(map(lambda x: x.values[-1], np.split(s9, 50))), index=s9.index.values[::4])
122/166:
plt.plot(s9_)
plt.plot(s9)
122/167:
plt.plot(s9[::4])
plt.plot(s9)
122/168: all_brackets
122/169: all(all_brackets)
122/170: len(all_brackets)
124/1: (13 + 7) / 201
124/2:
import pandas as pd
import numpy as np
124/3:
all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
brackets = pd.DataFrame({'start': all_brackets, 'end': pd.Series(all_brackets).shift(-1)})
brackets_log = brackets.iloc[:-1].copy()
brackets_log['start'] = np.log2(brackets_log['start'])
brackets_log['end'] = np.log2(brackets_log['end'])
brackets_log['d'] = brackets_log.end - brackets_log.start
delta = brackets_log['d'].iloc[0]
124/4: delta
124/5: (13 + 7) / 200
125/1:
import pandas as pd
import numpy as np
import sys
import os
125/2: coverage_type_dtype = pd.CategoricalDtype(list('NAUR'), ordered=True)
125/3: # step1: load all downloaded data
125/4:
def load_file_preprocess(filename):
    usecols = [
        'CountryCode', 'CountryName', 'CoverageType', 'RequestYear',
        'HeadCount', 'ReqYearPopulation', 'Mean'
    ]
    df = pd.read_csv(filename, usecols=usecols,
                     dtype={'CoverageType': coverage_type_dtype})
    df = df.rename(
        columns={
            'CountryCode': 'country',
            'CoverageType': 'coverage_type',
            'RequestYear': 'year'
        })
    df = df.set_index(['country', 'year', 'coverage_type'])
    return df
125/5:
res = {}

for f in os.listdir('../source/'):
    if f.endswith('.csv'):
        fn = f.split('.')[0]
        bracket = fn.lstrip('0')
        if bracket == '':
            bracket = 0
        else:
            bracket = int(bracket)
        res[bracket] = load_file_preprocess(os.path.join('../source/', f))
125/6: res[0]
125/7: # check nans
125/8:
for k, df in res.items():
    if df['HeadCount'].hasnans:
        print(k)
        print(df[pd.isnull(df['HeadCount'])])
125/9: # So all of India 2018/19 are NaNs in it. let's drop them for now
125/10:
res1 = dict()
for k, df in res.items():
    res1[k] = df.dropna(how='any', subset=['HeadCount'])
125/11: # step2: subtract and get bracket head count
125/12:
res2 = {}

for i in range(1, 200):
    df1 = res1[i]
    df2 = res1[i-1]
    df3 = df1['HeadCount'] - df2['HeadCount']
    res2[i-1] = df3
125/13: res2[0]
125/14: # double check nans
125/15:
for k, df in res2.items():
    if df.hasnans:
        print(k)
        print(df[pd.isnull(df)])
125/16: # ok, looks good
125/17: # step 3: create df with bracket info
125/18:
res3 = []

for k, v in res2.items():
    df = v.reset_index()
    df['bracket'] = k
    df = df.set_index(['country', 'year', 'coverage_type', 'bracket'])
    res3.append(df)
125/19: res3 = pd.concat(res3)
125/20: res3
125/21: res3['HeadCount'].hasnans
125/22: # assume each group has 199 datapoints
125/23:
gs = res3.groupby(['country', 'year', 'coverage_type'])
for g in gs.groups.keys():
    df = gs.get_group(g)
    assert df.shape[0] == 199
125/24: # step 4: fix negative values, fix sum of all brackets > 1
125/25:
import matplotlib.pyplot as plt
%matplotlib inline
125/26: plt.rcParams['figure.figsize'] = [12, 8]
125/27: res4 = res3.copy()
125/28: res4.loc[res4['HeadCount'] < 0, 'HeadCount'] = np.nan  # make negative values to nan
125/29: # also, drop all big noise. Notes on slack discussion
125/30: gs = res4.groupby(['country', 'year', 'coverage_type'])
125/31: res5 = list()
125/32:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = set()
    if s.hasnans and s.sum() > 1:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.add(w-1)
            todrop.add(w)
            todrop.add(w+1)
        s.iloc[list(todrop)] = np.nan
        res5.append(s)
    else:
        res5.append(s)
125/33: res5 = pd.concat(res5)
125/34: gs = res5.groupby(['country', 'year', 'coverage_type'])
125/35: res5
125/36:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1.1:
        print(g)
125/37:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('STP', 1999, 'N'))
plt.plot(df.values)
125/38:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.values)
125/39:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df.values)
125/40:
# before smoothing we need to interpolate the values
# now we try if interpolating is ok
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.interpolate().values)
125/41: # step 5: smoothing them
125/42:
sys.path.insert(0, '../scripts')
import smoothlib
125/43: run_smooth = smoothlib.run_smooth
125/44:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/45: gs = res5.groupby(['country', 'year', 'coverage_type'])
125/46: df = gs.get_group(('PAK', 2000, 'N'))
125/47: df.std()
125/48: s = func(df)
125/49: df = gs.get_group(('BRA', 2000, 'N'))
125/50: df.std()
125/51: s = func(df)
125/52:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/53: df = gs.get_group(('BRA', 1981, 'N'))
125/54: df.std()
125/55: s = func(df)
125/56:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/57: df = gs.get_group(('BRA', 1990, 'N'))
125/58: df.std()
125/59: s = func(df)
125/60:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/61:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/62: s = func(df)
125/63:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/64:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/65: s = func(df)
125/66:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/67:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 2)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/68: s = func(df)
125/69:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/70:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/71: s = func(df)
125/72:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/73: df = gs.get_group(('PAK', 1990, 'N'))
125/74: df.std()
125/75: s = func(df)
125/76:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/77: df = gs.get_group(('PAK', 2000, 'N'))
125/78: s = func(df)
125/79:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/80: df = gs.get_group(('BRA', 2000, 'N'))
125/81: df.std()
125/82: s = func(df)
125/83:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/84:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/85: s = func(df)
125/86:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/87:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/88: s = func(df)
125/89:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/90: df = gs.get_group(('USA', 2000, 'N'))
125/91: df.std()
125/92: s = func(df)
125/93:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/94: df = gs.get_group(('MWI', 2000, 'N'))
125/95: df.std()
125/96: s = func(df)
125/97:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/98: df = gs.get_group(('BRA', 1990, 'N'))
125/99: df.std()
125/100: s = func(df)
125/101:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/102: df = gs.get_group(('BRA', 1980, 'N'))
125/103: df = gs.get_group(('BRA', 1981, 'N'))
125/104: df.std()
125/105: s = func(df)
125/106:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/107: df = gs.get_group(('SUR', 1982, 'U'))
125/108: df.std()
125/109: s = func(df)
125/110:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/111:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/112: s = func(df)
125/113:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/114:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/115: s = func(df)
125/116:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/117:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/118: s = func(df)
125/119:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/120:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/121:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/122:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 1)
    if std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/123: s = func(df)
125/124:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/125:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
    if std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/126: s = func(df)
125/127:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/128:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 1)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/129:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
        res = run_smooth(res, 8, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/130: s = func(df)
125/131:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/132:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 4)
        res = run_smooth(res, 20, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/133: s = func(df)
125/134:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/135:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 6)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
        res = run_smooth(res, 20, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/136: s = func(df)
125/137:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/138:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 6)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/139:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 6)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 16, 3)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/140: s = func(df)
125/141:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/142:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 6)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/143: df = gs.get_group(('BRA', 1982, 'N'))
125/144: df.std()
125/145: s = func(df)
125/146:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/147:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 4)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/148:
df = gs.get_group(('BRA', 1982, 'N'))
df = gs.get_group(('SUR', 1982, 'U'))
125/149: s = func(df)
125/150:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/151:
df = gs.get_group(('BRA', 1982, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/152: df.std()
125/153: s = func(df)
125/154:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/155:
df = gs.get_group(('BRA', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/156: df.std()
125/157: s = func(df)
125/158:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/159:
df = gs.get_group(('TIJ', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/160:
df = gs.get_group(('TJK', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/161: df.std()
125/162: s = func(df)
125/163:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/164:
df = gs.get_group(('AGO', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/165: df.std()
125/166: s = func(df)
125/167:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/168:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 25, 5)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 20, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/169:
df = gs.get_group(('AGO', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/170: df.std()
125/171: s = func(df)
125/172:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/173:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 20, 5)
        res = run_smooth(res, 10, 2)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 25, 3)
        res = run_smooth(res, 20, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/174: gs = res5.groupby(['country', 'year', 'coverage_type'])
125/175:
df = gs.get_group(('AGO', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/176: df.std()
125/177: s = func(df)
125/178:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/179:
# df = gs.get_group(('AGO', 1990, 'N'))
df = gs.get_group(('SUR', 1982, 'U'))
125/180: s = func(df)
125/181:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/182:
# df = gs.get_group(('AGO', 1990, 'N'))
df = gs.get_group(('PAK', 1982, 'U'))
125/183: df.std()
125/184:
# df = gs.get_group(('AGO', 1990, 'N'))
df = gs.get_group(('PAK', 1982, 'N'))
125/185: df.std()
125/186: s = func(df)
125/187:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/188:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 20, 5)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/189:
# df = gs.get_group(('AGO', 1990, 'N'))
df = gs.get_group(('SUR', 1982, 'U'))
125/190: df.std()
125/191: s = func(df)
125/192:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/193:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 30, 5)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/194: df.std()
125/195: s = func(df)
125/196:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/197:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.009:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/198: df.std()
125/199: s = func(df)
125/200:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/201:
df = gs.get_group(('AGO', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/202: df.std()
125/203: s = func(df)
125/204:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/205:
df = gs.get_group(('BRA', 1990, 'N'))
# df = gs.get_group(('SUR', 1982, 'U'))
125/206: df.std()
125/207: s = func(df)
125/208:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/209:
# df = gs.get_group(('BRA', 1990, 'N'))
df = gs.get_group(('SUR', 1982, 'U'))
125/210: df.std()
125/211: s = func(df)
125/212:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/213:
# df = gs.get_group(('BRA', 1990, 'N'))
df = gs.get_group(('SUR', 1983, 'U'))
125/214: df.std()
125/215: s = func(df)
125/216:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/217:
# df = gs.get_group(('BRA', 1990, 'N'))
df = gs.get_group(('SUR', 1984, 'U'))
125/218: df.std()
125/219: s = func(df)
125/220:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/221:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
#     if std < 0.009:
#         res = run_smooth(x, 20, 6)
#         res = run_smooth(res, 16, 2)
#         res = run_smooth(res, 16, 1)
#         res = run_smooth(res, 10, 1)
#         res = run_smooth(res, 10, 0)
    if std < 0.012:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/222:
# df = gs.get_group(('BRA', 1990, 'N'))
df = gs.get_group(('SUR', 1984, 'U'))
125/223:
# df = gs.get_group(('BRA', 1990, 'N'))
df = gs.get_group(('SUR', 1983, 'U'))
125/224: df.std()
125/225: s = func(df)
125/226:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/227:
df = gs.get_group(('BRA', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/228: df.std()
125/229:
df = gs.get_group(('USA', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/230: df.std()
125/231: s = func(df)
125/232:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/233:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
#     if std < 0.009:
#         res = run_smooth(x, 20, 6)
#         res = run_smooth(res, 16, 2)
#         res = run_smooth(res, 16, 1)
#         res = run_smooth(res, 10, 1)
#         res = run_smooth(res, 10, 0)
    if std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/234: s = func(df)
125/235:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/236:
df = gs.get_group(('SWE', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/237: df.std()
125/238: s = func(df)
125/239:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/240:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.010:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    if std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/241:
#df = gs.get_group(('SWE', 1990, 'N'))
df = gs.get_group(('SUR', 1983, 'U'))
125/242: df.std()
125/243: s = func(df)
125/244:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/245:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        x = x.interpolate()
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # x = x.reset_index(drop=True)
    # run smoothing
    std = x.std()
    if std < 0.010:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    res = res / res.sum()
    return res
125/246: s = func(df)
125/247:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/248:
df = gs.get_group(('BRA', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/249: df.std()
125/250: s = func(df)
125/251:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/252:
df = gs.get_group(('AGO', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/253: df.std()
125/254: s = func(df)
125/255:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/256:
df = gs.get_group(('SWE', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/257: df.std()
125/258: s = func(df)
125/259:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/260:
df = gs.get_group(('SWZ', 1990, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/261: df.std()
125/262: s = func(df)
125/263:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/264:
df = gs.get_group(('SWZ', 2000, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/265: df.std()
125/266: s = func(df)
125/267:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/268:
df = gs.get_group(('CHN', 1980, 'A'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/269:
df = gs.get_group(('CHN', 1981, 'A'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/270: df.std()
125/271: s = func(df)
125/272:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/273:
df = gs.get_group(('CHN', 2000, 'A'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/274: df.std()
125/275: s = func(df)
125/276:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/277:
df = gs.get_group(('IND', 1981, 'A'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/278: df.std()
125/279: s = func(df)
125/280:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/281:
df = gs.get_group(('ZWE', 1981, 'A'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/282:
df = gs.get_group(('ZWE', 1981, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/283: df.std()
125/284: s = func(df)
125/285:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/286:
df = gs.get_group(('GEO', 1981, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/287: df.std()
125/288: s = func(df)
125/289:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/290: res5
125/291: res5.loc['SUR']
125/292: res5.loc['SUR'].index.get_level_values('coverage_type').unique()
125/293: res4 = res3.copy()
125/294: res4.loc[res4['HeadCount'] < 0, 'HeadCount'] = np.nan  # make negative values to nan
125/295: # also, drop all big noise. Notes on slack discussion
125/296: gs = res4.groupby(['country', 'year', 'coverage_type'])
125/297: res5 = list()
125/298:
for g in gs.groups.keys():
    df = gs.get_group(g)
    s = df['HeadCount'].copy()
    todrop = set()
    if s.hasnans:  # if negative values exists and sum bigger than 1
        where = np.where(pd.isnull(s))[0]
        for w in where:
            todrop.add(w-1)
            todrop.add(w)
            todrop.add(w+1)
        s.iloc[list(todrop)] = np.nan
        res5.append(s)
    else:
        res5.append(s)
125/299: res5 = pd.concat(res5)
125/300: gs = res5.groupby(['country', 'year', 'coverage_type'])
125/301: res5
125/302:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1.1:
        print(g)
125/303:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1:
        print(g)
125/304:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('STP', 1999, 'N'))
plt.plot(df.values)
125/305:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('DEU', 2019, 'N'))
plt.plot(df.values)
125/306:
gs = res5.groupby(['country', 'year', 'coverage_type'])
df = gs.get_group(('PRY', 1999, 'N'))
plt.plot(df.values)
125/307: gs = res5.groupby(['country', 'year', 'coverage_type'])
125/308:
df = gs.get_group(('GEO', 1981, 'N'))
# df = gs.get_group(('SUR', 1983, 'U'))
125/309: df.std()
125/310: s = func(df)
125/311:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/312:
# df = gs.get_group(('GEO', 1981, 'N'))
df = gs.get_group(('SUR', 1983, 'U'))
125/313: df.std()
125/314: s = func(df)
125/315:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/316:
df = gs.get_group(('AGO', 1981, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/317: df.std()
125/318: s = func(df)
125/319:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/320:
df = gs.get_group(('MYS', 1981, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/321: df.std()
125/322: s = func(df)
125/323:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/324:
df = gs.get_group(('MLT', 1981, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/325: df.std()
125/326: s = func(df)
125/327:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/328:
df = gs.get_group(('STR', 1981, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/329: df.std()
125/330:
df = gs.get_group(('STP', 1999, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/331: df.std()
125/332: s = func(df)
125/333:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/334:
df = gs.get_group(('EGY', 1999, 'N'))
#df = gs.get_group(('SUR', 1983, 'U'))
125/335: df.std()
125/336: s = func(df)
125/337:
plt.plot(s)
plt.plot(df.values, alpha=.5)
125/338:
# look for those sum bigger than 1
for g in gs.groups.keys():
    df = gs.get_group(g)
    if df.sum() > 1.1:
        print(g)
127/1: import pandas as pd
127/2: pd.read_csv('ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv', dtype=str)
127/3: a = pd.read_csv('ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv', dtype=str)
127/4: a.drop(columns='Unnamed: 0')
127/5: a = a.drop(columns='Unnamed: 0')
127/6: a.to_csv('ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv', index=False)
126/1:
shapes_file = shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
126/2:
import pandas as pd
import numpy as np
126/3:
import sys
sys.path.insert(0, '../scripts/')
126/4:
import matplotlib.pyplot as plt

%matplotlib inline
126/5:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
126/6:
%load_ext autoreload
%autoreload 1
126/7: %aimport shapeslib
126/8:
shapes_file = shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
126/9:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
126/10:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
126/11:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])
gini.index.names = ['country', 'time']

income.columns = ['income']
gini.columns = ['gini']
126/12:
all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
brackets = pd.DataFrame({'start': all_brackets, 'end': pd.Series(all_brackets).shift(-1)})
brackets_log = brackets.iloc[:-1].copy()
brackets_log['start'] = np.log2(brackets_log['start'])
brackets_log['end'] = np.log2(brackets_log['end'])
brackets_log['d'] = brackets_log.end - brackets_log.start
delta = brackets_log['d'].iloc[0]
126/13: delta
126/14: delta = 0.01
126/15:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
126/16: income_gini = pd.concat([income, gini], axis=1)
126/17: income_gini
126/18: get_distances = shapeslib.get_distances
126/19: get_distances(5.3, 53)
126/20: get_neighbors = shapeslib.get_neighbors
126/21: get_neighbors(3, 61, 10)
126/22:
def get_first_known_shape(country):
    try:
        df = shapes.loc[country]
    except KeyError:
        print(f'no shapes for country {country}')
        return None
    first_year = df.index.get_level_values('year')[0]
    # print(first_year)
    return shapeslib.get_shape((country, first_year), shapes)
126/23: income_gini
126/24: unknown_shapes = dict()
126/25: income_gini = income_gini.dropna(how='any')
126/26: income_gini = income_gini.dropna(how='any').sort_index()
126/27: shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini)
126/28: shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini)[:200]
126/29: shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[:200]
126/30: shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[0:200]
126/31: shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[0:199]
126/32: s0 = shapes.loc[('ago', 1981)]
126/33:
plt.plot(s0)
plt.plot(s1)
126/34: s0
126/35: s0['population_percentage']
126/36: plt.plot(s0['population_percentage'])
126/37: plt.plot(s0['population_percentage'].values)
126/38:
plt.plot(s0['population_percentage'].values)
plt.plot(s1)
126/39: s1 = shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[0:199]
126/40:
plt.plot(s0['population_percentage'].values)
plt.plot(s1)
126/41: s1 = shapeslib.get_estimated_mountain(('ago', 1900), shapes, income_gini).loc[0:199]
126/42: s0 = shapes.loc[('ago', 1981)]
126/43:
plt.plot(s0['population_percentage'].values)
plt.plot(s1)
126/44: s1 = shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[0:199]
126/45: s0
126/46:
s0 = shapeslib.get_shape(('ago', 1981), shapes)
i = income_gini.loc[('ago', 1981), 'income']
126/47: s0
126/48: s0.index = s0.index + shapeslib.bracket_number_from_income(i)
126/49: s0
126/50: s1
126/51:
plt.plot(s0)
plt.plot(s1)
126/52:
shape_known_list = shapes.index.values
for i in income_gini.index.values:
    if i not in shape_known_list:
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/53:
shape_known_list = shapes.index.values
for i in income_gini.index.values:
    if i not in shape_known_list:
        print(i)
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/54:
shape_known_list = shapes.index.values
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
    if i not in shape_known_list:
        print(i)
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/55:
unknown_shapes = dict()
shape_known_list = shapes.index.values
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if counrty in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
    if i not in shape_known_list:
        print(i)
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/56:
unknown_shapes = dict()
shape_known_list = shapes.index.values
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
    if i not in shape_known_list:
        print(i)
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/57:
unknown_shapes = dict()
shape_known_list = shapes.index.values
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
        
    if i not in shape_known_list:
        print(i)
        unknow_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/58:
unknown_shapes = dict()
shape_known_list = shapes.index.values
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
        
    if i not in shape_known_list:
        print(i)
        unknown_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/59:
unknown_shapes = dict()
shape_known_list = shapes.index.values.tolist()
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
        continue
        
    if i not in shape_known_list:
        print(i)
        unknown_shapes[i] = shapeslib.get_estimated_mountain(i, shapes, income_gini).loc[0:199]
126/60:
i, g = shapeslib.get_income_gini(('are', 1961), income_gini)
shapeslib.get_neighbors(i, g)
126/61:
i, g = shapeslib.get_income_gini(('are', 1961), income_gini)
shapeslib.get_neighbors(i, g, 100)
126/62:
i, g = shapeslib.get_income_gini(('are', 2040), income_gini)
shapeslib.get_neighbors(i, g, 100)
126/63:
i, g = shapeslib.get_income_gini(('are', 2040), income_gini)
shapeslib.get_neighbors(i, g, 50)
126/64:
i, g = shapeslib.get_income_gini(('are', 1972), income_gini)
shapeslib.get_estimated_mountain(('are', 1972), shapes, income_gini)
126/65:
i, g = shapeslib.get_income_gini(('are', 1972), income_gini)
s1  =shapeslib.get_estimated_mountain(('are', 1972), shapes, income_gini)
126/66: plt.plot(s1)
126/67: s1
126/68: plt.plot(s1[::4])
126/69: s1[::4]
126/70: len(s1[::4])
124/6: pd.MultiIndex([('ago', 1990), range(100)])
124/7: pd.MultiIndex?
124/8: pd.MultiIndex.from_product(['ago', 1990, range(100)])
124/9: pd.MultiIndex.from_product(['ago', 1990, list(range(100))])
124/10: pd.MultiIndex.from_product([['ago'], [1990], list(range(100))])
128/1:
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, '../scripts/')
128/2:
import matplotlib.pyplot as plt

%matplotlib inline
128/3:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
128/4:
%load_ext autoreload
%autoreload 1
128/5:
%aimport shapeslib
%aimport smoothlib
128/6: source = pd.read_csv('../wip/preprocessed.csv')
128/7: source
128/8: source = source.set_index(['country', 'year', 'coverage_type', 'bracket'])
128/9: source.loc[('CHN', 2002, 'A')]
128/10: chn = source.loc[('CHN', 2002, 'A')]
128/11: plt.plot(chn)
128/12: chn = source.loc[('CHN', 1983, 'A')]
128/13: plt.plot(chn)
128/14: shapeslib.run_smooth(chn.values, 10, 1)
128/15: smoothlib.run_smooth(chn.values, 10, 1)
128/16: chn.values
128/17: chn
128/18: chn.values
128/19: chn.values.tolist()
128/20: chn
128/21: chn['HeadCount'].values
128/22: chn = source.loc[('CHN', 1983, 'A')]['HeadCount']
128/23: plt.plot(chn)
128/24: smoothlib.run_smooth(chn.values, 10, 1)
128/25: s1 = smoothlib.run_smooth(chn.values, 10, 1)
128/26: s1
128/27: %timeit s1 = smoothlib.run_smooth(chn.values, 10, 1)
128/28: plt.plot(s1)
128/29: 6639 * 66 / 1000
129/1:
import pandas as pd
import numpy as np
129/2:
import sys
sys.path.insert(0, '../scripts/')
129/3:
import matplotlib.pyplot as plt

%matplotlib inline
129/4:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
129/5:
%load_ext autoreload
%autoreload 1
129/6: %aimport shapeslib
129/7:
shapes_file = shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
129/8:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
129/9:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
129/10: delta = 0.01
129/11:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
129/12:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
129/13:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
129/14: income_gini = pd.concat([income, gini], axis=1)
129/15: income_gini
129/16: income_gini = income_gini.dropna(how='any').sort_index()
129/17: s1 = shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini).loc[0:199]
129/18:
s0 = shapeslib.get_shape(('ago', 1981), shapes)
i = income_gini.loc[('ago', 1981), 'income']
129/19: s0.index = s0.index + shapeslib.bracket_number_from_income(i)
129/20: s0
129/21: s1
129/22:
plt.plot(s0)
plt.plot(s1)
129/23: from multiprocessing import Pool
129/24: from functools import partial
129/25:
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
129/26: shape_known_list = shapes.index.values.tolist()
129/27:
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/28:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/29: len(income_gini.index.values)
129/30:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/31:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/32:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/33:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False)
129/34: income, gini = shapeslib.get_income_gini(i)
129/35: income, gini = shapeslib.get_income_gini(i, income_gini)
129/36:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, shapes, resample=False)
129/37:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, shapes, resample=False)
129/38:
income, gini = shapeslib.get_income_gini(i, income_gini)
_, nei = shapeslib.get_neighbors(income, gini, 50)
129/39:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, nei, shapes, resample=False)
129/40:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, nei, shapes, resample=False)
129/41:
%%time
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, nei, shapes, resample=False)
129/42:
%%timeit
i = ('ago', 1800)
shapeslib.get_estimated_mountain2(i, income, gini, nei, shapes, resample=False)
129/43: nei
129/44: shapes
129/45: shapes_noc = shapes.copy()
129/46: shapes_noc
129/47: shapes_noc = shapes_noc.set_index(['bracket'], append=True)
129/48: shapes_noc
129/49:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage'] != c]
129/50: shapes_noc.groupby(['country', 'year']).apply(applyfunc)
129/51:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] != c]
129/52: shapes_noc.groupby(['country', 'year']).apply(applyfunc)
129/53:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] != c]['population_percentage']
129/54: shapes_noc.groupby(['country', 'year']).apply(applyfunc)
129/55: shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/56: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/57: shapes_noc2 = shapes_noc2.sort_index()
129/58: shapes_noc2
129/59: shapes_noc2.index
129/60: shapes_noc2.reset_index(level=0, drop=True)
129/61: shapes
129/62: shapes_noc2.reset_index(level=0, drop=True).sort_index()
129/63: shapes_noc
129/64:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
129/65: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/66: shapes_noc2 = shapes_noc2.sort_index()
129/67: shapes_noc2.reset_index(level=0, drop=True).sort_index()
129/68: shapes_noc2.loc[nei]
129/69: nei
129/70: shapes_noc2 = shapes_noc2.reset_index('bracket')
129/71: shapes_noc2
129/72: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/73: shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
129/74: shapes_noc2 = shapes_noc2.reset_index('bracket')
129/75: shapes_noc2
129/76: shapes_noc2.loc[nei]
129/77: shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').sum()
129/78: shapes_noc2.loc[nei]['bracket'].describe()
129/79: shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()
129/80: shapeslib.get_average_shape(i, shapes, income_gini)
129/81: shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/82:
shapes_file = shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
129/83:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
129/84: shapes_noc = shapes.copy()
129/85: shapes_noc = shapes_noc.set_index(['bracket'], append=True)
129/86: shapes_noc
129/87: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/88: shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
129/89: shapes_noc2 = shapes_noc2.reset_index('bracket')
129/90: shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/91: shapeslib.get_average_shape(i, shapes, income_gini)
129/92: s2 = shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/93: shapeslib.get_average_shape(i, shapes, income_gini)
129/94: s3 = shapeslib.get_average_shape(i, shapes, income_gini)
129/95:
plt.plot(s2)
plt.plot(s3)
129/96:
#plt.plot(s2)
plt.plot(s3)
129/97: s2 - s3
129/98: s2 - s3.mean()
129/99: (s2 - s3).mean()
129/100: (s2 - s3).max()
129/101: (s2 - s3).min()
129/102: (s2 - s3).abs().mean()
129/103: plt.plot(s2)
129/104:
%timeit
s2 = shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/105:
%%timeit
s2 = shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/106:
%%timeit
s3 = shapeslib.get_average_shape(i, shapes, income_gini)
129/107: plt.plot(s2[20:20])
129/108: plt.plot(s2.loc[20:20])
129/109: plt.plot(s2.loc[20:50])
129/110:
plt.plot(s2.loc[20:50])
plt.plot(s3.loc[20:50])
129/111: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)['population_percentage']
129/112: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)['population_percentage']
129/113: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
129/114: shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
129/115: shapes_noc2
129/116: nei_shapes = [shapes_noc2.loc[x] for x in nei]
129/117: nei_shapes[0]
129/118: shapeslib.merge_nshapes(nei_shapes)
129/119:
%%timeit
nei_shapes = [shapes_noc2.loc[x] for x in nei]
shapeslib.merge_nshapes(nei_shapes)
129/120:
%%timeit
nei_shapes = [shapes_noc2.loc[x] for x in nei]
shapeslib.merge_nshapes(nei_shapes)
129/121: np.sum(nei_shapes[:3])
129/122: np.sum(nei_shapes[:3], axis=1)
129/123: np.sum(nei_shapes[:3], axis=0)
129/124: nei_shapes[0]
129/125: nei_shapes[0] + nei_shapes[1] + nei_shapes[2]
129/126:  nei_shapes[0].add?
129/127:
n1, n2 = nei_shapes[0].align(nei_shapes[1])
n1 + n2
129/128: n1, n2 = nei_shapes[0].align(nei_shapes[1], fill_value=0)
129/129:
n1, n2 = nei_shapes[0].align(nei_shapes[1], fill_value=0)
n1 + n2
129/130: (n1 + n2) / 2
129/131: pd.concat([nei_shapes[0], nei_shapes[1]]).fillna(0).mean(axis=1)
129/132: pd.concat([nei_shapes[0], nei_shapes[1]]).fillna(0).mean()
129/133: pd.concat([nei_shapes[0], nei_shapes[1]]).fillna(0).mean(axis=0)
129/134: pd.concat([nei_shapes[0], nei_shapes[1]]).fillna(0)
129/135: pd.concat([nei_shapes[0], nei_shapes[1]], axis=1).fillna(0)
129/136: pd.concat([nei_shapes[0], nei_shapes[1]], axis=1).fillna(0).mean(axis=1)
129/137: shapes_noc2.loc[nei[:2]].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/138: shapes_noc3 = shapes_noc2.reset_index()
129/139: shapes_noc3
129/140: shapes_noc3 = shapes_noc3.set_index(['country', 'year', 'bracket'])
129/141: shapes_noc3 = shapes_noc2.reset_index()
129/142: shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
129/143: shapes_noc3.loc[nei[:2]].set_index('bracket').groupby('bracket').mean()['population_percentage']
129/144: shapes_noc3.loc[nei[:2]].set_index('bracket')
129/145: shapes_noc3.loc[nei[:2]].set_index('bracket').index.values
129/146: shapes_noc3.loc[nei[:2]].set_index('bracket').groupby('bracket').sum() / 2
129/147:
%%timeit
s2 = shapes_noc2.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
129/148:
%%timeit
s2 = shapes_noc3.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
129/149: shapes_noc2
129/150: shapes_noc2.loc[('ago', 1981, )]
129/151: shapes_noc2.loc[('ago', 1981)]
129/152: shapes_noc2.loc[[('ago', 1981, ), ('ago', 1982, )]]
129/153: shapes_noc2.loc[[('ago', 1981, :), ('ago', 1982, :)]]
129/154: shapes_noc2.loc[[('ago', 1981, slice(:)), ('ago', 1982, :)]]
129/155: shapes_noc2.loc[[('ago', 1981, slice(None)), ('ago', 1982, slice(None))]]
129/156: shapes_noc2.loc[(('ago', 1981, slice(None)), ('ago', 1982, slice(None)))]
129/157: shapes_noc2.loc[(('ago', 1981, slice(None)), ('ago', 1982, slice(None))), :]
129/158: shapes_noc2.loc[(('ago', 1981, [0, 1]), ('ago', 1982, [0, 1]))]
129/159: shapes_noc2.loc[(('ago', 1981), ('ago', 1982))]
129/160: shapes_noc2.loc[(nei, slice(None))]
129/161: shapes_noc2.loc[(nei, slice(0, 10))]
129/162:
idx = pd.IndexSlice
shapes_noc2.loc[idx[nei, :]]
129/163:
idx = pd.IndexSlice
shapes_noc2.loc[idx['ago', :, :]]
129/164: shapes_noc2.index.get_level_values?
129/165: shapes_noc2.index.get_locs?
129/166: shapes_noc2.index.get_loc_level?
129/167: nei
129/168: pd.MultiIndex.from_tuples(nei)
129/169: idx = pd.MultiIndex.from_tuples(nei)
129/170: shapes_noc2.loc[shapes_noc2.index.intersection(idx)]
129/171: shapes_noc
129/172: shapes_noc2
129/173: shapes_noc4 = shapes_noc2.reset_index()
129/174: shapes_noc4
129/175: shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
129/176: shapes_noc4
129/177: shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
129/178: shapes_noc4.loc[nei]
129/179: nei
129/180: shapes_noc4.loc[(nei)]
129/181: shapes_noc4.loc[nei[0]]
129/182: shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)]
129/183: shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
129/184:
%%timeit
shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
129/185:
%%timeit
shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
129/186:
%%timeit
shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
129/187: idx = pd.IndexSlice
129/188:
%%timeit
shapes_noc4.loc[idx[nei, :]].groupby('bracket').sum() / 50
129/189:
# %%timeit
shapes_noc4.loc[idx[nei, :]].groupby('bracket').sum() / 50
129/190:
%%timeit
shapes_noc4.loc[idx[nei, :]].groupby('bracket').sum() / 50
129/191:
%%timeit
s2 = shapes_noc3.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
129/192:
%%timeit
s2 = shapes_noc3.loc[nei].groupby('bracket').sum()['population_percentage'] / len(nei)
129/193:
%%timeit
s2 = shapes_noc3.loc[nei].groupby('bracket').sum()['population_percentage'] / len(nei)
129/194:
%%timeit
s2 = shapes_noc3.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
129/195:
%%timeit
shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
129/196:
%%timeit
shapes_noc3.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
129/197: shapes_noc4.loc[('ago', 1900)]
129/198: shapes_noc4.loc[('ago', 1981)]
130/1:
import pandas as pd
import numpy as np
130/2:
import sys
sys.path.insert(0, '../scripts/')
130/3:
import matplotlib.pyplot as plt

%matplotlib inline
130/4:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
130/5:
%load_ext autoreload
%autoreload 1
130/6: %aimport shapeslib
130/7:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
130/8:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
130/9:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
130/10: delta = 0.01
130/11:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
130/12: res7 = pd.read_csv('../wip/income_mountain_200brackets.csv')
130/13: res7 = res7.sort_values(by=['country', 'year', 'bracket'])
130/14: res7
130/15: np.all(res7['income_mountain'] >= 0)
130/16: res8 = res7.iloc[::4]
130/17: res8 = res8.set_index(['country', 'year', 'bracket']).sort_index()
130/18: res7 = res7.set_index(['country', 'year', 'bracket']).sort_index()
130/19: plt.plot(res7.loc[('sle', 1984)])
130/20: res8
130/21: pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
130/22: pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
130/23: pop
130/24:
res9 = []
gs = res8.groupby(['country', 'year'])
for g, df in gs:
    p = pop.loc[g]
    res9.append(df['income_mountain'] * p)
130/25: res10 = pd.concat(res9)
130/26: res10
130/27:
# res10[res10 == res10.min()]
res10.loc[('sle', 1984)]
130/28: plt.plot(res10.loc[('zwe', 2040)])
130/29:
def makeit(ser):
    res = ser.copy()
    c, y, b = ser.index[0]
    if res.min() < 0:    # FIXME:
        # print(c, y, end=' ')
        # print('has negatives')
        res = res - res.min()
        res = res / res.sum()
    res = res.astype(int).astype(str)
    res_combined = ', '.join(res.values)
    # print(ser)
    # c, y, b = ser.index[0]
    return np.array(res_combined)
130/30:
gs = res10.groupby(['country', 'year'])
df = gs.get_group(('ago', 1981))
130/31: makeit(df)
130/32: res11 = res10.groupby(['country', 'year']).apply(makeit)
130/33: res11
130/34: res11.name = 'income_mountain_50bracket_shape_for_log'
130/35: res11.to_csv('../../ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
131/1: esitmate = pd.read_csv('../wip/income_mountain_200brackets.csv')
131/2:
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, '../scripts/')
131/3:
import matplotlib.pyplot as plt

%matplotlib inline
131/4:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
131/5:
%load_ext autoreload
%autoreload 1
131/6:
%aimport shapeslib
%aimport smoothlib
131/7: esitmate = pd.read_csv('../wip/income_mountain_200brackets.csv')
131/8: estimate
131/9: esitmate
131/10: est = esitmate.set_index(['country', 'year', 'bracket'])
131/11: est.loc[('chn', 1983)]
131/12: est = esitmate.set_index(['country', 'year', 'bracket']).sort_index()
131/13: est.loc[('chn', 1983)]
131/14: plt.plot(est.loc[('chn', 1983)]['income_mountain'])
131/15: est.loc[('chn', 1983)
131/16: est.loc[('chn', 1983)]
131/17: esitmate
130/36:
import pandas as pd
import numpy as np
130/37:
import sys
sys.path.insert(0, '../scripts/')
130/38:
import matplotlib.pyplot as plt

%matplotlib inline
130/39:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
130/40:
%load_ext autoreload
%autoreload 1
130/41: %aimport shapeslib
130/42:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
130/43:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
130/44:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
130/45: delta = 0.01
130/46:
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
130/47:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
130/48: income_gini = pd.concat([income, gini], axis=1)
130/49: income_gini
130/50: income_gini = income_gini.dropna(how='any').sort_index()
130/51: s1 = shapeslib.get_estimated_mountain(('ago', 1980), shapes, income_gini)
130/52:
s0 = shapeslib.get_shape(('ago', 1981), shapes)
i = income_gini.loc[('ago', 1981), 'income']
130/53: s0.index = s0.index + shapeslib.bracket_number_from_income(i)
130/54: s0
130/55: s1
130/56:
plt.plot(s0)
plt.plot(s1)
130/57: shapes
130/58: shapes_noc = shapes.copy()
130/59: shapes_noc = shapes_noc.set_index(['bracket'], append=True)
130/60: shapes_noc
130/61:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
130/62: shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
130/63: shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
130/64: shapes_noc2
130/65:
i = ('ago', 1980)
income, gini = shapeslib.get_income_gini(i, income_gini)
_, nei = shapeslib.get_neighbors(income, gini, 50)
130/66:
%%timeit
nei_shapes = [shapes_noc2.loc[x] for x in nei]
shapeslib.merge_nshapes(nei_shapes)
130/67: shapes_noc3 = shapes_noc2.reset_index()
130/68: shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
130/69:
%%timeit
shapes_noc3.loc[nei].set_index('bracket').groupby('bracket').sum()['population_percentage'] / len(nei)
130/70:
%%timeit
s3 = shapeslib.get_average_shape(i, shapes, income_gini)
130/71: shapes_noc4 = shapes_noc2.reset_index()
130/72: shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
130/73: shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
130/74: idx = pd.IndexSlice
130/75:
%%timeit
shapes_noc4.loc[shapes_noc4.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
130/76: income_gini_noc = income_gini.reset_index(drop=True).drop_duplicates()
130/77: income_gini_noc
130/78:
def get_distances_res(v):
    i = v[0]
    g = v[1]
    cno, neis = shapeslib.get_neighbors(i, g)
    return (i, g), (cno, neis)

with Pool(11) as p:
    res_distances = p.map(get_distances_res, income_gini_noc.values)
130/79: from multiprocessing import Pool
130/80:
def get_distances_res(v):
    i = v[0]
    g = v[1]
    cno, neis = shapeslib.get_neighbors(i, g)
    return (i, g), (cno, neis)

with Pool(6) as p:
    res_distances = p.map(get_distances_res, income_gini_noc.values)
130/81: import json
132/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
132/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
132/3:
%load_ext autoreload
%autoreload 1
132/4: %aimport shapeslib
132/5:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
132/6:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
132/7:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
132/8:
delta = 0.01
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    # delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
132/9:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
132/10: income_gini = pd.concat([income, gini], axis=1)
132/11: income_gini = income_gini.dropna(how='any').sort_index()
132/12:
shapes_noc = shapes.copy()
shapes_noc = shapes_noc.set_index(['bracket'], append=True)
132/13:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
132/14:
shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
132/15:
shapes_noc3 = shapes_noc2.reset_index()
shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
132/16:
shapes_noc4 = shapes_noc2.reset_index()
shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
132/17:
from multiprocessing import Pool
import json
from funtools import partial
132/18:
from multiprocessing import Pool
import json
from functools import partial
132/19:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.read(fp)
132/20:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
132/21:
def get_average_shape(c, y, shapes, neighbours):
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
132/22:
def get_nearest_known_shape(country, year, known_shapes):
    try:
        df = known_shapes.loc[country]
    except KeyError:
        return None
    
    if year > 2018:
        nearest = df.index.get_level_values(0)[-1]
    else:
        nearest = df.index.get_level_values(0)[0]
    return df.loc[nearest]
132/23:
def get_estimated_mountain2(idx, income, known_shapes, known_shapes_2, neighbours, n=50, resample=False):
    country, year = idx
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    average_shape = get_average_shape(c, y, known_shapes_2, neighbours)
    weights = all_weights[year]
    mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], weights)
    bracket = bracket_number_from_income(income)
    mixed_shape.index = mixed_shape.index + bracket
    # if mixed_shape.index[-1] != 200:
    #     new_idx = pd.Index.from_
    #     mixed_shape = mixed_shape.reindex(new_idx)
    # return mixed_shape.loc[0:200]
    res = mixed_shape.loc[0:199]
    if len(res) != 200:
        # print(f'not enough points in mixed shape: {idx}')
        new_idx = pd.Index(range(200))
        res = mixed_shape.reindex(new_idx, fill_value=0)
    if resample:
        return res[::4]
    else:
        return res
132/24: get_nearest_known_shape('ago', 1921, shapes_noc2)
132/25:
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
132/26: shape_known_list = shapes.index.values.tolist()
132/27:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in skip_list:
        return None
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain2(i, income, shapes_noc2, shapes_noc4, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
132/28: run = partial(process, skip_list=skip_list, shape_known_list=shape_known_list)
132/29: # begins at 14:26
132/30: # begins at 14:27
132/31:
with Pool(11) as p:
    res = p.map(run, income_gini.index.values)
132/32: # begins at 15:33
132/33:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
132/34: !date
132/35: res
133/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
133/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
133/3:
%load_ext autoreload
%autoreload 1
133/4: %aimport shapeslib
133/5:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
133/6:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
133/7:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
133/8:
delta = 0.01
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    # delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
133/9:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
133/10: income_gini = pd.concat([income, gini], axis=1)
133/11: income_gini = income_gini.dropna(how='any').sort_index()
133/12:
shapes_noc = shapes.copy()
shapes_noc = shapes_noc.set_index(['bracket'], append=True)
133/13:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
133/14:
shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
133/15:
shapes_noc3 = shapes_noc2.reset_index()
shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
133/16:
shapes_noc4 = shapes_noc2.reset_index()
shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
133/17:
from multiprocessing import Pool
import json
from functools import partial
133/18:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
133/19:
def get_average_shape(c, y, shapes, neighbours):
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
133/20:
def get_nearest_known_shape(country, year, known_shapes):
    try:
        df = known_shapes.loc[country]
    except KeyError:
        return None
    
    if year > 2018:
        nearest = df.index.get_level_values(0)[-1]
    else:
        nearest = df.index.get_level_values(0)[0]
    return df.loc[nearest]
133/21:
def get_estimated_mountain2(idx, income, known_shapes, known_shapes_2, neighbours, n=50, resample=False):
    country, year = idx
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    average_shape = get_average_shape(c, y, known_shapes_2, neighbours)
    weights = all_weights[year]
    mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], weights)
    bracket = bracket_number_from_income(income)
    mixed_shape.index = mixed_shape.index + bracket
    # if mixed_shape.index[-1] != 200:
    #     new_idx = pd.Index.from_
    #     mixed_shape = mixed_shape.reindex(new_idx)
    # return mixed_shape.loc[0:200]
    res = mixed_shape.loc[0:199]
    if len(res) != 200:
        # print(f'not enough points in mixed shape: {idx}')
        new_idx = pd.Index(range(200))
        res = mixed_shape.reindex(new_idx, fill_value=0)
    if resample:
        return res[::4]
    else:
        return res
133/22: get_nearest_known_shape('ago', 1921, shapes_noc2)
133/23:
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
133/24: shape_known_list = shapes.index.values.tolist()
133/25:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in skip_list:
        return None
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain2(i, income, shapes_noc2, shapes_noc4, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
133/26: run = partial(process, skip_list=skip_list, shape_known_list=shape_known_list)
133/27: # begins at 15:33
133/28:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
133/29:
def get_estimated_mountain2(idx, income, known_shapes, known_shapes_2, neighbours, n=50, resample=False):
    country, year = idx
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    average_shape = get_average_shape(country, year, known_shapes_2, neighbours)
    weights = all_weights[year]
    mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], weights)
    bracket = bracket_number_from_income(income)
    mixed_shape.index = mixed_shape.index + bracket
    # if mixed_shape.index[-1] != 200:
    #     new_idx = pd.Index.from_
    #     mixed_shape = mixed_shape.reindex(new_idx)
    # return mixed_shape.loc[0:200]
    res = mixed_shape.loc[0:199]
    if len(res) != 200:
        # print(f'not enough points in mixed shape: {idx}')
        new_idx = pd.Index(range(200))
        res = mixed_shape.reindex(new_idx, fill_value=0)
    if resample:
        return res[::4]
    else:
        return res
133/30:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
133/31: all_neighbours_json['ago'][1800]
133/32: all_neighbours_json['ago']
133/33:
def get_average_shape(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
133/34:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
133/35:
all_years = set(range(1800, 2041))
all_weights = dict([(x, shapeslib.get_weights(x)) for x in all_years])
133/36:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
133/37: !date
133/38:
res2 = []
for k, v in res:
    df = v.copy()
    country, year = k
    # print(country, year)
    df.index.name = 'bracket'
    df.name = 'income_mountain'
    df = df.reset_index()
    df['country'] = country
    df['year'] = year
    df = df.set_index(['country', 'year', 'bracket'])
    res2.append(df)
133/39: res
133/40: res = [x for x in res if x is not None]
133/41: len(res)
133/42:
res2 = []
for k, v in res:
    df = v.copy()
    country, year = k
    # print(country, year)
    df.index.name = 'bracket'
    df.name = 'income_mountain'
    df = df.reset_index()
    df['country'] = country
    df['year'] = year
    df = df.set_index(['country', 'year', 'bracket'])
    res2.append(df)
133/43: res3 = pd.concat(res2)
133/44:
res4 = []
for c in shapes.index.get_level_values('country').unique():
    df = shapes.loc[[c]]
    for ct in 'naur':
        if ct in df['coverage_type'].values:
            df_ = df[df['coverage_type'] == ct].copy()
            df_ = df_.drop(columns='coverage_type')
            res4.append((c, df_))
            break
133/45:
res5 = []
for c, df in res4:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/46:
def shape_to_mountain(shape_, idx, income_gini):
    c, y = idx
    try:
        i, _ = shapeslib.get_income_gini(idx, income_gini)
    except KeyError:
        print(idx)
        return None
    bracket = bracket_number_from_income(i)
    shape = shape_.copy()
    shape.index = shape.index + bracket
    res = shape.loc[0:199]
    if len(res) != 200:
        new_idx = pd.Index(range(200))
        res = res.reindex(new_idx, fill_value=0)
        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    res.index = new_idx2
    return res
133/47:
res5 = []
for c, df in res4:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/48: res6 = pd.concat(res5)
133/49: res6.columns = ['income_mountain']
133/50: res7 = pd.concat([res3, res6])
133/51: res7.to_csv('../wip/income_mountain_200brackets.csv')
133/52:
from ddf_utils.str import format_float_digits
formattor = partial(format_float_digits, digits=6)
133/53: res7
133/54: res7.loc[('chn', 2015)]
133/55: res7 = res7.sort_index()
133/56: res7.loc[('chn', 2015)]
133/57: plt.plot(res7.loc[('chn', 2015)])
133/58: res7.loc[('usa', 2015)]
133/59: res7.loc[('usa', 2015)].values
133/60: res7.loc[('usa', 1800)].values
133/61: res4[0]
133/62: res5[0]
133/63:
res5 = []
for c, df in res4[:2]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/64: res5[0]
133/65:
res5 = []
for c, df in res4[:2]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/66:
res5 = []
for c, df in res4[:2]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            print(idx)
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/67: df2
133/68: shape_to_mountain(df2, idx, income_gini)
133/69:
def shape_to_mountain(shape_, idx, income_gini):
    c, y = idx
    try:
        i, _ = shapeslib.get_income_gini(idx, income_gini)
    except KeyError:
        print(idx)
        return None
    bracket = bracket_number_from_income(i)
    shape = shape_.copy()
    shape.index = shape.index + bracket
    res = shape.loc[0:199]
    print(res)
    if len(res) != 200:
        new_idx = pd.Index(range(200))
        res = res.reindex(new_idx, fill_value=0)
        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    res.index = new_idx2
    return res
133/70:
res5 = []
for c, df in res4[:1]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            # print(idx)
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/71:
def shape_to_mountain(shape_, idx, income_gini):
    c, y = idx
    try:
        i, _ = shapeslib.get_income_gini(idx, income_gini)
    except KeyError:
        print(idx)
        return None
    bracket = bracket_number_from_income(i)
    shape = shape_.copy()
    shape.index = shape.index + bracket
    res = shape.loc[0:199]
    print(shape)
    if len(res) != 200:
        new_idx = pd.Index(range(200))
        res = res.reindex(new_idx, fill_value=0)
        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    res.index = new_idx2
    return res
133/72:
res5 = []
for c, df in res4[:1]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            # print(idx)
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/73: res4[0]
133/74: res4[0][1]
133/75:
res5 = []
for c, df in res4[:1]:
    for y in range(1981, 2020):
        idx = (c, y)
        try:
            df2 = df.set_index(['bracket'], append=True).loc[idx]
            # print(df2)
        except KeyError:
            print(idx)
            continue
        if not df2.empty:
            # print(idx)
            m = shape_to_mountain(df2, idx, income_gini)
            if m is not None:
                res5.append(m)
133/76:
res4 = []
for c in shapes.index.get_level_values('country').unique():
    df = shapes.loc[[c]]
    for ct in 'naur':
        if ct in df['coverage_type'].values:
            df_ = df[df['coverage_type'] == ct].copy()
            df_ = df_.drop(columns='coverage_type')
            res4.append(df_)
            break
133/77: res4[0]
133/78: res5 = pd.concat(res4)
133/79: res5
133/80:
res6 = []
gs = res5.groupby(['country', 'year'])
for g, df in res5:
    c, y = g
    try:
        i, _ = shapeslib.get_income_gini(g, income_gini)
    except KeyError:
        print(g)
        continue
    bracket = bracket_number_from_income(i)
    shape = df.copy()
    shape.index = shape.index + bracket
    result = shape.loc[0:199]
    if len(result) != 200:
        new_idx = pd.Index(range(200))
        result = result.reindex(new_idx, fill_value=0)        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    lesult.index = new_idx2
    res6.append(result)
133/81:
res6 = []
gs = res5.groupby(['country', 'year'])
for g, df in gs:
    c, y = g
    try:
        i, _ = shapeslib.get_income_gini(g, income_gini)
    except KeyError:
        print(g)
        continue
    bracket = bracket_number_from_income(i)
    shape = df.copy()
    shape.index = shape.index + bracket
    result = shape.loc[0:199]
    if len(result) != 200:
        new_idx = pd.Index(range(200))
        result = result.reindex(new_idx, fill_value=0)        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    lesult.index = new_idx2
    res6.append(result)
133/82:
res6 = []
gs = res5.groupby(['country', 'year'])
for g, df in gs:
    c, y = g
    try:
        i, _ = shapeslib.get_income_gini(g, income_gini)
    except KeyError:
        print(g)
        continue
    bracket = bracket_number_from_income(i)
    shape = df.copy()
    shape = shape.set_index('bracket')
    shape.index = shape.index + bracket
    result = shape.loc[0:199]
    if len(result) != 200:
        new_idx = pd.Index(range(200))
        result = result.reindex(new_idx, fill_value=0)        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    lesult.index = new_idx2
    res6.append(result)
133/83:
res6 = []
gs = res5.groupby(['country', 'year'])
for g, df in gs:
    c, y = g
    try:
        i, _ = shapeslib.get_income_gini(g, income_gini)
    except KeyError:
        print(g)
        continue
    bracket = bracket_number_from_income(i)
    shape = df.copy()
    shape = shape.set_index('bracket')
    shape.index = shape.index + bracket
    result = shape.loc[0:199]
    if len(result) != 200:
        new_idx = pd.Index(range(200))
        result = result.reindex(new_idx, fill_value=0)        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    result.index = new_idx2
    res6.append(result)
133/84: res7 = pd.concat(res6)
133/85: res7
133/86: df = gs.get_group(('ago', 1981))
133/87: df
133/88: df.set_index(['bracket'])
133/89: df = df.set_index(['bracket'])
133/90:
df = df.set_index(['bracket'])
df.index
133/91:
df = df.set_index('bracket')
df.index
133/92: df = gs.get_group(('ago', 1981))
133/93:
df = df.set_index('bracket')
df.index
133/94:
df = df.set_index('bracket')
df.index = df.index + 90
133/95:
df = gs.get_group(('ago', 1981))
df = df.set_index('bracket')
133/96: df.index = df.index + 90
133/97: df
133/98:
df = gs.get_group(('ago', 1980))
df = df.set_index('bracket')
i, _ = shapeslib.get_income_gini(('ago', 1980))
bracket = bracket_number_from_income(i)
bracket
133/99:
df = gs.get_group(('ago', 1981))
df = df.set_index('bracket')
i, _ = shapeslib.get_income_gini(('ago', 1981))
bracket = bracket_number_from_income(i)
bracket
133/100:
df = gs.get_group(('ago', 1981))
df = df.set_index('bracket')
i, _ = shapeslib.get_income_gini(('ago', 1981), income_gini)
bracket = bracket_number_from_income(i)
bracket
133/101: i
133/102:
df = gs.get_group(('ago', 1981))
df = df.set_index('bracket')
i, _ = shapeslib.get_income_gini(('ago', 1981), income_gini)
bracket = shapeslib.bracket_number_from_income(i)
bracket
133/103:
delta = 0.1
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    # delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
133/104:
res6 = []
gs = res5.groupby(['country', 'year'])
for g, df in gs:
    c, y = g
    try:
        i, _ = shapeslib.get_income_gini(g, income_gini)
    except KeyError:
        # print(g)
        continue
    bracket = bracket_number_from_income(i)
    shape = df.copy()
    shape = shape.set_index('bracket')
    shape.index = shape.index + bracket
    result = shape.loc[0:199]
    if len(result) != 200:
        new_idx = pd.Index(range(200))
        result = result.reindex(new_idx, fill_value=0)        
    new_idx2 = pd.MultiIndex.from_product([[c], [y], range(200)], names=['country', 'year', 'bracket'])
    result.index = new_idx2
    res6.append(result)
133/105: res7 = pd.concat(res6)
133/106: res7
133/107: res7.columns = ['income_mountain']
133/108: res8 = pd.concat([res3, res6])
133/109: res8 = pd.concat([res3, res7])
133/110: res8.to_csv('../wip/income_mountain_200brackets.csv')
133/111: res8 = res8.sort_index()
133/112: res7.loc[('usa', 1800)].values
133/113: res8.loc[('usa', 1800)].values
133/114: res8.loc[('chn', 1800)].values
133/115: len(res)
133/116:
res2 = []
for k, v in res:
    df = v.copy()
    country, year = k
    # print(country, year)
    df.index.name = 'bracket'
    df.name = 'income_mountain'
    df = df.reset_index()
    df['country'] = country
    df['year'] = year
    df = df.set_index(['country', 'year', 'bracket'])
    res2.append(df)
133/117: res3 = pd.concat(res2)
133/118: res3
135/1:
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, '../scripts/')
135/2:
import matplotlib.pyplot as plt

%matplotlib inline
135/3:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
135/4:
%load_ext autoreload
%autoreload 1
135/5:
%aimport shapeslib
%aimport smoothlib
135/6: esitmate = pd.read_csv('../wip/income_mountain_200brackets.csv')
135/7: est = esitmate.set_index(['country', 'year', 'bracket']).sort_index()
135/8: plt.plot(est.loc[('chn', 1983)]['income_mountain'])
135/9:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
135/10: pop
135/11: pop.loc[('deu', 2015)]
135/12: est
134/1: res8 = pd.read_csv('../wip/income_mountain_200brackets.csv').set_index(['counrty', 'year', 'bracket']).sort_index()
134/2:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
134/3:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
134/4:
%load_ext autoreload
%autoreload 1
134/5: %aimport shapeslib
134/6:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
134/7: res8 = pd.read_csv('../wip/income_mountain_200brackets.csv').set_index(['counrty', 'year', 'bracket']).sort_index()
134/8: res8 = pd.read_csv('../wip/income_mountain_200brackets.csv').set_index(['country', 'year', 'bracket']).sort_index()
134/9: res8.loc[('chn', 1800)].values
134/10: plt.plot(res8.loc[('chn', 1981)])
134/11: df = res8.loc[('chn', 1981)]
134/12: df
134/13: df = res8.loc[('chn', 1981), 'income_mountain']
134/14: df
134/15: np.intersect1d?
134/16: np.partition?
134/17: np.split(df, 50)
134/18: np.split(df, 50).sum()
134/19: [x.sum() for x in np.split(df, 50)]
134/20: plt.plot([x.sum() for x in np.split(df, 50)])
134/21: pd.Series([x.sum() for x in np.split(df, 50)])
134/22: pd.Series([x.sum() for x in np.split(df, 50)]).sum()
134/23: df = res8.loc[('deu', 1981), 'income_mountain']
134/24: pd.Series([x.sum() for x in np.split(df, 50)]).sum()
134/25:
def resample(ser):
    return pd.Series([x.sum() for x in np.split(ser, 50)])
134/26:
def resample(ser):
    res = pd.Series([x.sum() for x in np.split(ser, 50)])
    res.index.name = 'bracket'
    return res
134/27: res8.groupby(['country', 'year']).apply(resample)
134/28:
gs = res8.groupby(['country', 'year'])
df = gs.get_group(('deu', 1981))
134/29: resample(df)
134/30: df
134/31: resample(df.values)
134/32:
def resample(ser):
    res = pd.Series([x.sum() for x in np.split(ser.values, 50)])
    res.index.name = 'bracket'
    return res
134/33: res9 = res8.groupby(['country', 'year']).apply(resample)
134/34: res9
134/35:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
134/36: pop
134/37:
res10 = []
gs = res9.groupby(['country', 'year'])
# for g, row in res9.iterrows():
#     p = pop.loc[g]
#     res10.append(df['income_mountain'] * p)
134/38: gs.get_group(('ago', 1800))
134/39: gs.get_group(('ago', 1800)).values
134/40: gs.get_group(('ago', 1800)).values * 2
134/41: gs.get_group(('ago', 1800)).T * 2
134/42: gs.get_group(('ago', 1800)).T.values * 2
134/43: res9.sum(axis=1)
134/44:
res10 = []
# gs = res9.groupby(['country', 'year'])
for g, row in res9.iterrows():
    p = pop.loc[g]
    row_pop = row * p
    row_pop_str = ', '.join(row_pop.astype(str))
    res10.append(row_pop_str)
134/45: res10[0]
134/46:
res10 = []
# gs = res9.groupby(['country', 'year'])
for g, row in res9.iterrows():
    p = pop.loc[g]
    row_pop = row * p
    row_pop_str = ', '.join(row_pop.astype(int).astype(str))
    res10.append(row_pop_str)
134/47: res10[0]
134/48:
res10 = []
# gs = res9.groupby(['country', 'year'])
for g, row in res9.iterrows():
    p = pop.loc[g]
    row_pop = np.round(row * p)
    row_pop_str = ', '.join(row_pop.astype(str))
    res10.append(row_pop_str)
134/49: res10[0]
134/50:
res10 = []
# gs = res9.groupby(['country', 'year'])
for g, row in res9.iterrows():
    p = pop.loc[g]
    row_pop = np.round(row * p)
    row_pop_str = ', '.join(row_pop.astype(int).astype(str))
    res10.append(row_pop_str)
134/51: res10[0]
134/52: res11 = pd.Series(res10, res9.index)
134/53: res11
134/54: res11.name = 'income_mountain_50bracket_shape_for_log'
134/55: res11.to_csv('../../ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
134/56: plt.plot(res9.loc[('chn', 1981)].T.values)
134/57: res_with_pop = res9.stack()
134/58: res_with_pop
134/59: res9 * pop
134/60: res9.multiply(pop)
134/61: res9.multiply(pop, axis='rows')
134/62: res0
134/63: res9
134/64: pop
134/65: pop.index.names = ['country', 'year']
134/66: res9.multiply(pop, axis='rows')
134/67: res9.multiply(pop, axis='index')
134/68:
res_with_pop = []
for g, row in res9.iterrows():
    p = pop.loc[g]
    row_pop = np.round(row * p)
    res_with_pop.append(row_pop.T)
134/69: res_with_pop[0]
134/70:
res_with_pop = []
for g, df in res9.groupby(['country', 'year']):
    p = pop.loc[g]
    row_pop = np.round(row * p)
    res_with_pop.append(row_pop.T)
134/71: res_with_pop[0]
134/72:
res_with_pop = []
for g, df in res9.groupby(['country', 'year']):
    p = pop.loc[g]
    row_pop = np.round(df * p)
    res_with_pop.append(df.T)
134/73:
res_with_pop = []
for g, df in res9.groupby(['country', 'year']):
    p = pop.loc[g]
    row_pop = np.round(df * p)
    res_with_pop.append(row_pop)
134/74: res_with_pop[0]
134/75:
res_with_pop = []
for g, df in res9.stack().groupby(['country', 'year']):
    p = pop.loc[g]
    row_pop = np.round(df * p)
    res_with_pop.append(row_pop)
134/76: res_with_pop[0]
134/77: res_with_pop = pd.concat(res_with_pop)
134/78: res_with_pop
134/79: plt.plot(res_with_pop.loc[('chn', 2015)])
134/80: res_with_pop.loc[('chn', 2015)]
134/81:
ax = plt.plot(res_with_pop.loc[('chn', 2015)])
ax.set_yscale('log')
134/82:
ax,  = plt.plot(res_with_pop.loc[('chn', 2015)])
ax.set_yscale('log')
134/83:
fig, ax  = plt.plot(res_with_pop.loc[('chn', 2015)])
ax.set
134/84:
plt.plot(res_with_pop.loc[('chn', 2015)])
plt.yscale('log')
138/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
138/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
138/3:
%load_ext autoreload
%autoreload 1
138/4: %aimport shapeslib
138/5:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
138/6:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
138/7:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--gapminder_gini--by--geo--time.csv'
138/8:
delta = 0.1
def bracket_number_from_income(s):
    """input a daily income, output a bracket number"""
    # delta = 0.10050251256281406
    return ((np.log2(s) + 7) / delta).astype(int)
138/9:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
138/10: income_gini = pd.concat([income, gini], axis=1)
138/11: income_gini = income_gini.dropna(how='any').sort_index()
138/12:
shapes_noc = shapes.copy()
shapes_noc = shapes_noc.set_index(['bracket'], append=True)
138/13:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
138/14:
shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
138/15:
shapes_noc3 = shapes_noc2.reset_index()
shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
138/16:
shapes_noc4 = shapes_noc2.reset_index()
shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
138/17:
all_years = set(range(1800, 2041))
all_weights = dict([(x, shapeslib.get_weights(x)) for x in all_years])
138/18:
from multiprocessing import Pool
import json
from functools import partial
138/19:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
138/20:
all_years = set(range(1800, 2041))
all_weights = dict([(x, shapeslib.get_weights(x)) for x in all_years])
138/21:
from multiprocessing import Pool
import json
from functools import partial
138/22:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
138/23:
def get_average_shape(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
138/24:
def get_nearest_known_shape(country, year, known_shapes):
    try:
        df = known_shapes.loc[country]
    except KeyError:
        return None
    
    if year > 2018:
        nearest = df.index.get_level_values(0)[-1]
    else:
        nearest = df.index.get_level_values(0)[0]
    return df.loc[nearest]
138/25:
def get_estimated_mountain2(idx, income, known_shapes, known_shapes_2, neighbours, n=50, resample=False):
    country, year = idx
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    average_shape = get_average_shape(country, year, known_shapes_2, neighbours)
    weights = all_weights[year]
    mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], weights)
    bracket = bracket_number_from_income(income)
    mixed_shape.index = mixed_shape.index + bracket
    # if mixed_shape.index[-1] != 200:
    #     new_idx = pd.Index.from_
    #     mixed_shape = mixed_shape.reindex(new_idx)
    # return mixed_shape.loc[0:200]
    res = mixed_shape.loc[0:199]
    if len(res) != 200:
        # print(f'not enough points in mixed shape: {idx}')
        new_idx = pd.Index(range(200))
        res = mixed_shape.reindex(new_idx, fill_value=0)
    if resample:
        return res[::4]
    else:
        return res
138/26: get_nearest_known_shape('ago', 1921, shapes_noc2)
138/27:
skip_list = list()
for i in income_gini.index.values:
    country, year = i
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
138/28: shape_known_list = shapes.index.values.tolist()
138/29:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in skip_list:
        return None
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain2(i, income, shapes_noc2, shapes_noc4, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
138/30: run = partial(process, skip_list=skip_list, shape_known_list=shape_known_list)
138/31: !date
138/32:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
138/33:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
138/34:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['geo', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
138/35:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['country', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
138/36: income_gini = pd.concat([income, gini], axis=1)
138/37: income_gini = income_gini.dropna(how='any').sort_index()
143/1: import numpy as np
143/2: np.inf
145/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
145/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
145/3:
%load_ext autoreload
%autoreload 1
145/4: %aimport shapeslib
145/5: !head ../source/0000.csv
145/6: pd.read_csv('../source/0000.csv').head()
145/7:
s000 = pd.read_csv('../source/0000.csv')
s000.head()
145/8: s000['PovertyLine']
145/9: s000['PovertyLine'].unique()
145/10:
all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
brackets_delta = 0.1  # it's (13 - (-7)) / 200
145/11: all_brackets[0]
145/12:
def step1():
    res = dict()
    for f in os.listdir('../source/'):
        if f.endswith('.csv'):
            fn = f.split('.')[0]
            bracket = fn.lstrip('0')
            if bracket == '':
                bracket = 0
            else:
                bracket = int(bracket)
            res[bracket] = etllib.load_file_preprocess(os.path.join('../source/', f))
    return res
145/13: res1 = step1()
145/14:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
145/15: res1 = step1()
145/16:
%aimport shapeslib
%aimport etllib
%aimport smoothlib
145/17: res1 = step1()
145/18:
def step2(res1):
    res = dict()
    nans = set()
    for k, df in res1.items():
        if df['HeadCount'].hasnans:
            idxs = df[pd.isnull(df['HeadCount'])].index.unique()
            nans = nans.union(set(idxs))
        res[k] = df.dropna(how='any', subset=['HeadCount'])
    if len(nans) > 0:
        print("WARNING: NaNs detected in these datapoints, dropping them")
        for i in nans:
            print(i)
    return res
145/19: res2 = step1(res1)
145/20: res2 = step2(res1)
145/21: all_brackets[199]
145/22: all_brackets[200]
145/23:
# step3: subtract and get bracket head count, and concat them to DataFrame
def step3(res2):
    res3 = list()
    for i in range(1, 201):
        df1 = res2[i]
        df2 = res2[i-1]
        df3 = df1[['HeadCount']] - df2[['HeadCount']]
        df3['bracket'] = i - 1
        df3 = df3.set_index('bracket', append=True)
        res3.append(df3)
    return pd.concat(res3)
145/24: res3 = step3(res2)
145/25: res3
145/26:
# step4: fix negative values
def step4(res3):
    res4 = list()
    gs = res3.groupby(['country', 'year', 'coverage_type'])
    for g in gs.groups.keys():
        df = gs.get_group(g)
        s = df['HeadCount'].copy()
        todrop = set()
        if np.any(s < 0):  # if negative values exists
            where = np.where(s < 0)[0]
            for w in where:
                if w != 199:
                    todrop.add(w+1)
                if w != 0:
                    todrop.add(w-1)
                todrop.add(w)
            s.iloc[list(todrop)] = np.nan
            res4.append(s)
        else:
            res4.append(s)
    return pd.concat(res4)
145/27: res3.loc[('DEU', 2015, 'N')]
145/28: res3 = res3.sort_index()
145/29: res3.loc[('DEU', 2015, 'N')]
145/30: plt.plot(res3.loc[('DEU', 2015, 'N')])
145/31:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
145/32: plt.plot(res3.loc[('DEU', 2015, 'N')])
145/33: res4 = step4(res3)
145/34: plt.plot(res4.loc[('DEU', 2015, 'N')])
145/35: res4 = res4.sort_index()
145/36: plt.plot(res4.loc[('DEU', 2015, 'N')])
145/37: res4 = res4.sort_index()  # TODO: see if we can skip this step and not producing the unsorted warning
145/38:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        return pd.Series()
    return s_new
145/39:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
145/40: res5 = step5(res4)
145/41: from multiprocessing import Pool
145/42:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
145/43: res5 = step5(res4)
145/44:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        return pd.Series([], dtype=int)
    return s_new
145/45:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        return pd.Series([], dtype=float)
    return s_new
145/46: res5 = step5(res4)
145/47: res5
145/48:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        raise
        return pd.Series([], dtype=float)
    return s_new
145/49: res5 = step5(res4)
145/50:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        # interpolate curve if theree are nans
        x = x.interpolate()
        # if first value is nan, it won't be interpolated. we fill 0
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # run smoothing, based on standard deviation
    std = x.std()
    if std < 0.010:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    if res.min() < 0:
        res = res - res.min()
    res = res / res.sum()
    return res
145/51:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        return pd.Series([], dtype=float)
    return s_new
145/52: from multiprocessing import Pool
145/53:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
145/54: res5 = step5(res4)
145/55: res5
145/56: 14852 / 7.89
145/57: 1800 * 7.89
145/58: 1800 * 7.13
145/59: 14852 - 14202
145/60: 12834 + 650 - 1000
145/61: plt.plot(res4.loc[('DEU', 2015, 'N')])
145/62:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
#     except KeyboardInterrupt:
#         raise
    except:
        print(idx[0])
        return pd.Series([], dtype=float)
    return s_new
145/63:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
145/64: res5 = step5(res4)
145/65: res5
145/66: res4
146/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
146/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
146/3:
%load_ext autoreload
%autoreload 1
146/4:
%aimport shapeslib
%aimport etllib
%aimport smoothlib
146/5:
s000 = pd.read_csv('../source/0000.csv')
s000.head()
146/6:
all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
brackets_delta = 0.1  # it's (13 - (-7)) / 200
146/7: all_brackets[0]
146/8: s000['PovertyLine'].unique()
146/9: all_brackets[200]
146/10:
def step1():
    res = dict()
    for f in os.listdir('../source/'):
        if f.endswith('.csv'):
            fn = f.split('.')[0]
            bracket = fn.lstrip('0')
            if bracket == '':
                bracket = 0
            else:
                bracket = int(bracket)
            res[bracket] = etllib.load_file_preprocess(os.path.join('../source/', f))
    return res
146/11: res1 = step1()
146/12:
def step2(res1):
    res = dict()
    nans = set()
    for k, df in res1.items():
        if df['HeadCount'].hasnans:
            idxs = df[pd.isnull(df['HeadCount'])].index.unique()
            nans = nans.union(set(idxs))
        res[k] = df.dropna(how='any', subset=['HeadCount'])
    if len(nans) > 0:
        print("WARNING: NaNs detected in these datapoints, dropping them")
        for i in nans:
            print(i)
    return res
146/13: res2 = step2(res1)
146/14:
# step3: subtract and get bracket head count, and concat them to DataFrame
def step3(res2):
    res3 = list()
    for i in range(1, 201):
        df1 = res2[i]
        df2 = res2[i-1]
        df3 = df1[['HeadCount']] - df2[['HeadCount']]
        df3['bracket'] = i - 1
        df3 = df3.set_index('bracket', append=True)
        res3.append(df3)
    return pd.concat(res3)
146/15: res3 = step3(res2)
146/16: res3
146/17: res3 = res3.sort_index()
146/18: plt.plot(res3.loc[('DEU', 2015, 'N')])
146/19:
# step4: reduce noises
def step4(res3):
    res4 = list()
    gs = res3.groupby(['country', 'year', 'coverage_type'])
    for g in gs.groups.keys():
        df = gs.get_group(g)
        s = df['HeadCount'].copy()
        todrop = set()
        if np.any(s < 0):  # if negative values exists
            where = np.where(s < 0)[0]
            for w in where:
                if w != 199:
                    todrop.add(w+1)
                if w != 0:
                    todrop.add(w-1)
                todrop.add(w)
            s.iloc[list(todrop)] = np.nan
            res4.append(s)
        else:
            res4.append(s)
    return pd.concat(res4)
146/20: res4 = step4(res3)
146/21: res4 = res4.sort_index()  # TODO: see if we can skip this step and not producing the unsorted warning
146/22: plt.plot(res4.loc[('DEU', 2015, 'N')])
146/23:
def func(x):
    """function to smooth a series"""
    if x.hasnans:
        # interpolate curve if theree are nans
        x = x.interpolate()
        # if first value is nan, it won't be interpolated. we fill 0
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # run smoothing, based on standard deviation
    std = x.std()
    if std < 0.010:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    if res.min() < 0:
        res = res - res.min()
    res = res / res.sum()
    return res
146/24:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
#     except KeyboardInterrupt:
#         raise
    except:
        print(idx[0])
        return pd.Series([], dtype=float)
    return s_new
146/25: from multiprocessing import Pool
146/26:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
146/27: res5 = step5(res4)
146/28: res5
146/29: res4
146/30:
ggg = res4.groupby(['country', 'year' ,'coverage_type'])
ggg.get_group(('AGO', 1981, 'N'))
146/31:
ggg = res4.groupby(['country', 'year' ,'coverage_type'])
x = ggg.get_group(('AGO', 1981, 'N'))
146/32: func(x.values)
146/33: func(x)
146/34:
def func(x):
    """function to smooth a series"""
    run_smooth = smoothlib.run_smooth
    if x.hasnans:
        # interpolate curve if theree are nans
        x = x.interpolate()
        # if first value is nan, it won't be interpolated. we fill 0
        if pd.isnull(x.iloc[0]):
            x = x.fillna(0)
    # run smoothing, based on standard deviation
    std = x.std()
    if std < 0.010:
        res = run_smooth(x, 20, 6)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 1)
        res = run_smooth(res, 10, 0)
    elif std < 0.012:
        res = run_smooth(x, 20, 3)
        res = run_smooth(res, 16, 2)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    else:
        res = run_smooth(x, 20, 1)
        res = run_smooth(res, 16, 1)
        res = run_smooth(res, 16, 0)
        res = run_smooth(res, 10, 0)
        res = run_smooth(res, 8, 0)
    # also, make sure it will sum up to 100%
    if res.min() < 0:
        res = res - res.min()
    res = res / res.sum()
    return res
146/35:
def process(ser):
    idx = ser.index
    try:
        s_new = func(ser)
        s_new.index = idx
    except KeyboardInterrupt:
        raise
    except:
        print(idx[0])
        return pd.Series([], dtype=float)
    return s_new
146/36: from multiprocessing import Pool
146/37:
POOLSIZE = 6

def step5(res4):
    gs = res4.groupby(['country', 'year', 'coverage_type'])
    to_smooth = list()
    for g, df in gs:
        to_smooth.append(df)
    print(len(to_smooth))
    with Pool(POOLSIZE) as p:
        res5 = p.map(process, to_smooth[:10])
    return pd.concat(res5)
146/38: func(x)
146/39: res5 = step5(res4)
146/40: res5
146/41:
def step6(res5):
    res6 = res5.copy()
    res6.name = 'population_percentage'
    res6 = res6.reset_index()
    res6['country'] = res6['country'].map(str.lower)
    res6['country'] = res6['country'].replace({'xkx': 'kos'})
    res6['coverage_type'] = res6['coverage_type'].map(str.lower)
    return res6.set_index(['country', 'year', 'coverage_type'])
146/42: res6 = step6(res5)
146/43:
def step7(df):
    income = pd.read_csv(income_file).set_index(['country', 'time'])
    income.index.names = ['country', 'year']
    income.columns = ['income']

    res = list()
    gs = df.groupby(['country', 'year', 'coverage_type'])
    for g, gdf in gs:
        df_ = gdf.copy()
        g_ = (g[0], g[1])
        try:
            m = income.loc[g_, 'income']
        except KeyError:
            print(f"missing: {g_}")
            continue
        b = etllib.bracket_number_from_income(m)
        df_['bracket'] = df_['bracket'] - b
        res.append(df_)

    return pd.concat(res)
146/44: res7 = step7(res6)
146/45: income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
146/46:
def step7(df):
    income = pd.read_csv(income_file).set_index(['country', 'time'])
    income.index.names = ['country', 'year']
    income.columns = ['income']

    res = list()
    gs = df.groupby(['country', 'year', 'coverage_type'])
    for g, gdf in gs:
        df_ = gdf.copy()
        g_ = (g[0], g[1])
        try:
            m = income.loc[g_, 'income']
        except KeyError:
            print(f"missing: {g_}")
            continue
        b = etllib.bracket_number_from_income(m)
        df_['bracket'] = df_['bracket'] - b
        res.append(df_)

    return pd.concat(res)
146/47: res7 = step7(res6)
146/48: res7
146/49:
import json
from functools import partial
146/50:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
138/38:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
147/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
147/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
147/3:
%load_ext autoreload
%autoreload 1
147/4: %aimport shapeslib
147/5:
shapes_file = '../wip/smoothshape/ddf--datapoints--population_percentage--by--country--year--coverage_type--bracket.csv'
known_shapes = pd.read_csv(shapes_file)
147/6:
# create a sorted version
shapes = known_shapes.set_index(['country', 'year']).sort_index()
147/7:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'
147/8: bracket_number_from_income = shapeslib.bracket_number_from_income
147/9:
%aimport shapeslib
%aimport etllib
147/10: bracket_number_from_income = etllib.bracket_number_from_income
147/11:
income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['country', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
147/12: income_gini = pd.concat([income, gini], axis=1)
147/13: income_gini = income_gini.dropna(how='any').sort_index()
147/14:
shapes_noc = shapes.copy()
shapes_noc = shapes_noc.set_index(['bracket'], append=True)
147/15:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
147/16:
shapes_noc2 = shapes_noc.groupby(['country', 'year'], as_index=False).apply(applyfunc)
shapes_noc2 = shapes_noc2.reset_index(level=0, drop=True).sort_index()
147/17:
shapes_noc3 = shapes_noc2.reset_index()
shapes_noc3 = shapes_noc3.set_index(['country', 'year'])
147/18:
shapes_noc4 = shapes_noc2.reset_index()
shapes_noc4['country-year'] = list(zip(shapes_noc4['country'].values, shapes_noc4['year'].values))
shapes_noc4 = shapes_noc4.set_index(['country-year', 'bracket'])['population_percentage']
147/19:
all_years = set(range(1800, 2051))
all_weights = dict([(x, shapeslib.get_weights(x)) for x in all_years])
147/20:
all_years = set(range(1800, 2051))
all_weights = dict([(x, etllib.get_weights(x)) for x in all_years])
147/21:
%aimport shapeslib
%aimport etllib
%aimport constants
147/22:
all_years = set(range(1800, 2051))
all_weights = dict([(x, constants.get_weights(x)) for x in all_years])
147/23:
from multiprocessing import Pool
import json
from functools import partial
147/24:
fp = open('../wip/neighbours_list.json', 'r')
all_neighbours_json = json.load(fp)
147/25:
def get_average_shape(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
147/26:
def get_nearest_known_shape(country, year, known_shapes):
    try:
        df = known_shapes.loc[country]
    except KeyError:
        return None
    
    if year > 2017:
        nearest = df.index.get_level_values(0)[-1]
        # print(nearest)
    else:
        nearest = df.index.get_level_values(0)[0]
    return df.loc[nearest]
147/27:
def get_estimated_mountain2(idx, income, known_shapes, known_shapes_2, neighbours, n=50):
    country, year = idx
    wpov, was = all_weights[year]
    
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    
    if wpov == 1:
        mixed_shape = first_known_shape
    elif wpov == 0:        
        mixed_shape = get_average_shape(country, year, known_shapes_2, neighbours)
    else:
        average_shape = get_average_shape(country, year, known_shapes_2, neighbours)
        mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], (wpov, was))
        
    return shape_to_mountain(mixed_shape, income)
147/28:
def shape_to_mountain(shape, income):
    bracket = bracket_number_from_income(income)
    shape.index = shape.index + bracket
    res = shape.loc[0:199]
    if len(res) != 200:
        # print(f'not enough points in mixed shape: {idx}')
        new_idx = pd.Index(range(200))
        res = shape.reindex(new_idx, fill_value=0)
        
    return res
147/29: get_nearest_known_shape('ind', 2020, shapes_noc2)
147/30:
skip_list = list()
for country in income_gini.index.get_level_values(0):
    if country in skip_list:
        continue
    try:
        shapes.loc[country]
    except KeyError:
        skip_list.append(country)
        print(f'skip {country}')
147/31:
skip_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
know_countries = shapes.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        skip_list.append(country)
        print(f'skip {country}')
147/32:
skip_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        skip_list.append(country)
        print(f'skip {country}')
147/33:
unknown_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        unknown_list.append(country)
        print(f'skip {country}')
147/34:
unknown_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        unknown_list.append(country)
        print(f'{country} not available in povcal')
147/35: known_list = shapes.index.values.tolist()
147/36: shapes_noc4
147/37: shapes_noc2
147/38:
%%timeit

get_average_shape('ago', 1800, shapes_noc4, all_neighbours_json)
147/39: shapes_noc2
147/40:
def mask_shape(idx, c, y):
    pass

for x in shapes_noc2.index:
    x[0] == 'ago'
147/41:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 1800:
        mask.append(True)
    else:
        mask.append(False)
147/42: shapes_noc2.loc[mask]
147/43: shapes_noc2[mask]
147/44: shapes_noc2
147/45:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 1800:
        mask.append(True)
    else:
        mask.append(False)
        
mask = pd.Series(mask, index=shapes_noc2.index)
147/46: shapes_noc2.loc[mask]
147/47: mask
147/48: shapes_noc2.where(mask)
147/49: shapes_noc2.where(mask).dropna()
147/50: shapes_noc2.index.get_level_values(0) == 'ago'
147/51: shapes_noc2.loc[shapes_noc2.index.get_level_values(0) == 'ago']
147/52:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 1800:
        mask.append(True)
    else:
        mask.append(False)
        
mask = np.array(mask)
147/53:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 1960:
        mask.append(True)
    else:
        mask.append(False)
        
mask = np.array(mask)
147/54: shapes_noc2.loc[mask]
147/55: shapes_noc2.loc[shapes_noc2.index.get_level_values(0) == 'ago']
147/56:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 2000:
        mask.append(True)
    else:
        mask.append(False)
        
mask = np.array(mask)
147/57: shapes_noc2.loc[mask]
147/58:
def mask_shape(idx, c, y):
    pass

mask = []

for x in shapes_noc2.index:
    if x[0] == 'ago' and x[1] == 2000:
        mask.append(True)
    else:
        mask.append(False)
        
# mask = np.array(mask)
147/59: shapes_noc2.loc[mask]
147/60:
def mask_shape(idx, c, y):
    mask = []
    for x in idx:
        if x[0] == c and x[1] == y:
            mask.append(True)
        else:
            mask.append(False)
    return mask
147/61: shapes_noc2.loc[mask(shapes_noc2.index, 'chn', 2000)]
147/62: shapes_noc2.loc[mask_shape(shapes_noc2.index, 'chn', 2000)]
147/63:
def mask_shape(idx, check_list):
    mask = []
    for x in idx:
        val = [x[0], x[1]]  # [country, year]
        if val in check_list:
            mask.append(True)
        else:
            mask.append(False)
    return mask
147/64:
c, y = ('ago', 1900)
nei = all_neighbours_json[c][y]['neighbours']
147/65:
c, y = ('ago', '1900')
nei = all_neighbours_json[c][y]['neighbours']
147/66: nei
147/67: shapes_noc2.loc[mask_shape(shapes_noc2.index, nei)]
147/68: shapes_noc2.loc[mask_shape(shapes_noc2.index, nei)]
147/69: shapes_noc2.index.droplevel(-1)
147/70: nei2 = [tuple(x) for x in nei]
147/71: shapes_noc2.index.droplevel(-1).isin(nei2)
147/72:
def mask_shape2(idx, check_list):
    cl = [tuple(x) for x in check_list]
    return index.droplevel(-1).isin(cl)
147/73: shapes_noc2.loc[mask_shape2(shapes_noc2.index, nei)]
147/74:
def mask_shape2(idx, check_list):
    cl = [tuple(x) for x in check_list]
    return idx.droplevel(-1).isin(cl)
147/75: shapes_noc2.loc[mask_shape2(shapes_noc2.index, nei)]
147/76:
def get_average_shape_2(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    # nei = [tuple(x) for x in nei]
    return shapes.loc[mask_shape2(shapes.index, nei)].groupby('bracket').sum() / 50
147/77:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/78:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/79:
def mask_shape2(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    return idx.droplevel(-1).isin(cl)
147/80:
def get_average_shape_2(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[mask_shape2(shapes.index, nei)].groupby('bracket').sum() / 50
147/81:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/82:
def mask_shape2(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    return idx.droplevel(-1).isin(check_list)
147/83:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/84:
def mask_shape2(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    return idx.isin(check_list)
147/85:
def get_average_shape_2(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[mask_shape2(shapes.index.droplevel(-1), nei)].groupby('bracket').sum() / 50
147/86:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/87: set(shapes_noc2.index.droplevel(-1).values)
147/88:
def mask_shape3(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    
    return [x in check_list for x in idx]
147/89:
def mask_shape3(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    return [x in check_list for x in idx]
147/90:
def get_average_shape_3(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[mask_shape3(shapes.index.droplevel(-1).values, nei)].groupby('bracket').sum() / 50
147/91:
%%timeit

get_average_shape_3('ago', 1800, shapes_noc2, all_neighbours_json)
147/92:
def get_average_shape_3(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[mask_shape3(shapes.index.droplevel(-1).values, nei)].groupby('bracket').sum() / 50
147/93:
%%timeit

get_average_shape_3('ago', 1800, shapes_noc2, all_neighbours_json)
147/94:
def mask_shape3(idx, check_list):
    # cl = [tuple(x) for x in check_list]
    return idx.map(lambda x: x in check_list)
147/95:
def get_average_shape_3(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[mask_shape3(shapes.index.droplevel(-1), nei)].groupby('bracket').sum() / 50
147/96:
%%timeit

get_average_shape_3('ago', 1800, shapes_noc2, all_neighbours_json)
147/97:
def get_average_shape_3(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[mask_shape3(shapes.index.droplevel(-1), nei)].groupby('bracket').sum() / 50
147/98:
%%timeit

get_average_shape_3('ago', 1800, shapes_noc2, all_neighbours_json)
147/99:
def get_average_shape_3(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[mask_shape2(shapes.index.droplevel(-1), nei)].groupby('bracket').sum() / 50
147/100:
%%timeit

get_average_shape_3('ago', 1800, shapes_noc2, all_neighbours_json)
147/101:
def get_average_shape(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
147/102:
%%timeit

get_average_shape('ago', 1800, shapes_noc4, all_neighbours_json)
147/103:
def get_average_shape(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.get_level_values(0).isin(nei)].groupby('bracket').sum() / 50
147/104:
%%timeit

get_average_shape('ago', 1800, shapes_noc4, all_neighbours_json)
147/105:
def get_average_shape_2(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    return shapes.loc[shapes.index.droplevel(-1).isin(nei)].groupby('bracket').sum() / 50
147/106:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/107:
def get_average_shape_2(c, y, shapes, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = set([tuple(x) for x in nei])
    return shapes.loc[shapes.index.droplevel(-1).isin(nei)].groupby('bracket').sum() / 50
147/108:
%%timeit

get_average_shape_2('ago', 1800, shapes_noc2, all_neighbours_json)
147/109: country_year = list(zip(shape_noc2.index.get_level_values(0), shape_noc2.index.get_level_values(1)))
147/110: country_year = list(zip(shapes_noc2.index.get_level_values(0), shapes_noc2.index.get_level_values(1)))
147/111: country_year
147/112: country_year = pd.Series(country_year, index=shapes_noc2.index)
147/113: country_year.isin(nei)
147/114:
%%timeit 
country_year.isin(nei)
147/115:
%%timeit 
shapes_noc2.loc[country_year.isin(nei)]
147/116:
%%timeit
country_year = list(zip(shapes_noc2.index.get_level_values(0), shapes_noc2.index.get_level_values(1)))
country_year = pd.Series(country_year, index=shapes_noc2.index)
147/117:
def get_average_shape_3(c, y, shapes, all_country_year, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    mask = all_country_year.isin(nei)
    return shapes.loc[mask].groupby('bracket').sum() / 50
147/118:
def get_average_shape_4(c, y, shapes, all_country_year, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    mask = all_country_year.isin(nei)
    return shapes.loc[mask].groupby('bracket').sum() / 50
147/119:
%%timeit

get_average_shape_4('ago', 1800, shapes_noc2, country_year, all_neighbours_json)
147/120:
def get_estimated_mountain(idx, income, known_shapes, all_country_year, neighbours, n=50):
    country, year = idx
    wpov, was = all_weights[year]
    
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    
    if wpov == 1:
        mixed_shape = first_known_shape
    elif wpov == 0:        
        mixed_shape = get_average_shape2(country, year, known_shapes, all_country_year, neighbours)
    else:
        average_shape = get_average_shape(country, year, known_shapes, all_country_year, neighbours)
        mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], (wpov, was))
        
    return shape_to_mountain(mixed_shape, income)
147/121:
def get_estimated_mountain(idx, income, known_shapes, all_country_year, neighbours, n=50):
    country, year = idx
    wpov, was = all_weights[year]
    
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    
    if wpov == 1:
        mixed_shape = first_known_shape
    elif wpov == 0:        
        mixed_shape = get_average_shape2(country, year, known_shapes, all_country_year, neighbours)
    else:
        average_shape = get_average_shape(country, year, known_shapes, all_country_year, neighbours)
        mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], (wpov, was))
        
    return shape_to_mountain(mixed_shape, income)
147/122:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in unknown_list: 
        # only use Average Shapes
        income, _ = shapeslib.get_income_gini(i, income_gini)
        average_shape = get_average_shape2(country, year, shapes_noc2, all_country_year, all_neighbours_json)
        res = shape_to_mountain(average_shape, income)
        return i, res
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain(i, income, shapes_noc2, all_country_year, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
147/123: known_list = shapes.index.values.tolist()
147/124:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in unknown_list: 
        # only use Average Shapes
        income, _ = shapeslib.get_income_gini(i, income_gini)
        average_shape = get_average_shape2(country, year, shapes_noc2, all_country_year, all_neighbours_json)
        res = shape_to_mountain(average_shape, income)
        return i, res
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain(i, income, shapes_noc2, all_country_year, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
147/125: run = partial(process, skip_list=skip_list, shape_known_list=shape_known_list)
147/126: run = partial(process, skip_list=unknow_list, shape_known_list=known_list)
147/127: run = partial(process, skip_list=unknown_list, shape_known_list=known_list)
147/128: !date
147/129: !date
147/130:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
147/131:
def get_average_shape2(c, y, shapes, all_country_year, neighbours):
    y = str(y)
    nei = neighbours[c][y]['neighbours']
    nei = [tuple(x) for x in nei]
    mask = all_country_year.isin(nei)
    return shapes.loc[mask].groupby('bracket').sum() / 50
147/132:
%%timeit

get_average_shape2('ago', 1800, shapes_noc2, country_year, all_neighbours_json)
147/133:
def get_estimated_mountain(idx, income, known_shapes, all_country_year, neighbours, n=50):
    country, year = idx
    wpov, was = all_weights[year]
    
    first_known_shape = get_nearest_known_shape(country, year, known_shapes)
    if first_known_shape is None:
        return None
    
    if wpov == 1:
        mixed_shape = first_known_shape
    elif wpov == 0:        
        mixed_shape = get_average_shape2(country, year, known_shapes, all_country_year, neighbours)
    else:
        average_shape = get_average_shape2(country, year, known_shapes, all_country_year, neighbours)
        mixed_shape = shapeslib.merge_nshapes_with_weights([first_known_shape, average_shape], (wpov, was))
        
    return shape_to_mountain(mixed_shape, income)
147/134:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in unknown_list: 
        # only use Average Shapes
        income, _ = shapeslib.get_income_gini(i, income_gini)
        average_shape = get_average_shape2(country, year, shapes_noc2, all_country_year, all_neighbours_json)
        res = shape_to_mountain(average_shape, income)
        return i, res
    if i not in shape_known_list:
        income, _ = shapeslib.get_income_gini(i, income_gini)
        res = get_estimated_mountain(i, income, shapes_noc2, all_country_year, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
147/135: run = partial(process, skip_list=unknown_list, shape_known_list=known_list)
147/136: !date
147/137:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
147/138: all_country_year = country_year
147/139: !date
147/140:
with Pool(6) as p:
    res = p.map(run, income_gini.index.values)
147/141: !date
147/142: res = [x for x in res if x is not None]
147/143: len(res)
147/144:
res2 = []
for k, v in res:
    df = v.copy()
    country, year = k
    # print(country, year)
    df.index.name = 'bracket'
    df.name = 'income_mountain'
    df = df.reset_index()
    df['country'] = country
    df['year'] = year
    df = df.set_index(['country', 'year', 'bracket'])
    res2.append(df)
146/51: known_shapes = res7.copy()
146/52:
get_estimated_mountain = shapeslib.get_estimated_mountain
shape_to_mountain = shapeslib.shape_to_mountain
147/145: res3 = pd.concat(res2)
147/146: res3
146/53: all_brackets
146/54:
income_file = '../../../ddf--gapminder--fasttrack/ddf--datapoints--mincpcap_cppp--by--country--time.csv'
gini_file =  '../../../ddf--gapminder--fasttrack/ddf--datapoints--gini--by--country--time.csv'

income = pd.read_csv(income_file).set_index(['country', 'time'])
gini = pd.read_csv(gini_file).set_index(['country', 'time'])

income.index.names = ['country', 'year']
gini.index.names = ['country', 'year']

income.columns = ['income']
gini.columns = ['gini']
146/55:
income_gini = pd.concat([income, gini], axis=1)
income_gini = income_gini.dropna(how='any').sort_index()
146/56:
def applyfunc(ser):
    for c in 'naur':
        if c in ser['coverage_type'].values:
            return ser[ser['coverage_type'] == c]['population_percentage']
146/57:
unknown_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        unknown_list.append(country)
        print(f'{country} not available in povcal')
146/58:
unknown_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes_noc.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        unknown_list.append(country)
        print(f'{country} not available in povcal')
146/59:
known_shapes = res7.copy()
shapes_noc = known_shapes.set_index(['bracket'], append=True)
146/60:
unknown_list = list()

all_countries = income_gini.index.get_level_values(0).unique()
known_countries = shapes_noc.index.get_level_values(0).unique()

for country in all_countries:
    if country not in known_countries:
        unknown_list.append(country)
        print(f'{country} not available in povcal')
146/61: all_countries
146/62: known_countries
146/63:
get_estimated_mountain = shapeslib.get_estimated_mountain
shape_to_mountain = shapeslib.shape_to_mountain
get_average_shape2 = shapeslib.get_average_shape2

bracket_number_from_income = etllib.bracket_number_from_income
146/64:
get_estimated_mountain = shapeslib.get_estimated_mountain
shape_to_mountain = shapeslib.shape_to_mountain
get_average_shape2 = shapeslib.get_average_shape2
get_income_gini = shapeslib.get_income_gini

bracket_number_from_income = etllib.bracket_number_from_income
146/65:
def process(i, skip_list, shape_known_list):
    country, year = i
    if country in unknown_list: 
        # only use Average Shapes
        income, _ = get_income_gini(i, income_gini)
        average_shape = get_average_shape2(country, year, shapes_noc2, all_country_year, all_neighbours_json)
        res = shape_to_mountain(average_shape, income)
        return i, res
    if i not in shape_known_list:
        income, _ = get_income_gini(i, income_gini)
        res = get_estimated_mountain(i, income, shapes_noc2, all_country_year, all_neighbours_json)
        # res = shapeslib.get_estimated_mountain(i, shapes, income_gini, resample=False) 
        return i, res
146/66:
def fix_column_names(k, df):
    country, year = k
    # print(country, year)
    df.index.name = 'bracket'
    df.name = 'income_mountain'
    df = df.reset_index()
    df['country'] = country
    df['year'] = year
    df = df.set_index(['country', 'year', 'bracket'])
    return df
146/67: res6
146/68: res6['bracket'].unique()
146/69: res6
149/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
149/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
149/3:
%load_ext autoreload
%autoreload 1
149/4: from ddf_utils.factory.common import download
149/5:
lines = {
    'owd': [1.9, 3.2, 5.5, 10],
    'on': [2, 8, 32]
}
149/6: all_lines = [1.9, 3.2, 5.5, 10, 2, 8, 32]
149/7:
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    print(file_csv)
149/8:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(bracket)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
149/9:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(l)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
152/1:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(l)
    print(file_csv)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
152/2:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
152/3:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
152/4:
%load_ext autoreload
%autoreload 1
152/5: from ddf_utils.factory.common import download
152/6:
lines = {
    'owd': [1.9, 3.2, 5.5, 10],
    'on': [2, 8, 32]
}
152/7: all_lines = [1.9, 3.2, 5.5, 10, 2, 8, 32]
152/8:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(l)
    print(file_csv)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
154/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
154/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
154/3:
%load_ext autoreload
%autoreload 1
154/4: from ddf_utils.factory.common import download
154/5:
lines = {
    'owd': [1.9, 3.2, 5.5, 10],
    'on': [2, 8, 32]
}
154/6: all_lines = [1.9, 3.2, 5.5, 10, 2, 8, 32]
154/7:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(l)
    print(file_csv)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
155/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
155/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
155/3:
%load_ext autoreload
%autoreload 1
155/4: %aimport etllib
155/5: from ddf_utils.factory.common import download
155/6:
lines = {
    'owd': [1.9, 3.2, 5.5, 10],
    'on': [2, 8, 32]
}
155/7: all_lines = [1.9, 3.2, 5.5, 10, 2, 8, 32]
155/8: etllib.load_file_preprocess('../source/poverty_rates/1.9.csv')
155/9: owd_lines = lines['owd']
155/10: all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
155/11: mountain_ptc = pd.read_csv('../../ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
155/12: mountain_ptc
155/13:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
155/14: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
155/15: mountain_ptc
155/16: pop
155/17:
mountain_num = []

for g, df in mountain_pct.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    mountain_num.append(df * p)
    break
155/18:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    mountain_num.append(df * p)
    break
155/19: mountain_num[0]
155/20:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.floor(df_new)
    mountain_num.append(df * p)
    break
155/21: mountain_num[0]
155/22:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.floor(df_new['income_mountain'])
    mountain_num.append(df * p)
    break
155/23: mountain_num[0]
155/24:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain'])
    mountain_num.append(df * p)
    break
155/25:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.floor(df_new['income_mountain'])
    mountain_num.append(df_new)
    break
155/26: mountain_num[0]
155/27:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain'])
    mountain_num.append(df_new)
    break
155/28: mountain_num[0]
155/29:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
    break
155/30: mountain_num[0]
155/31:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
155/32: mountain_num = pd.concat(mountain_num)
155/33: mountain_num
155/34:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
155/35: mountain_num.to_csv('../../ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
155/36: etllib.bracket_number_from_income(2)
155/37: etllib.bracket_number_from_income(8)
155/38: etllib.bracket_number_from_income(32)
155/39: gs = mountain.groupby(['country', 'year'])
155/40: gs = mountain_ptc.groupby(['country', 'year'])
155/41: gs.get_group(('ago', 2000))
155/42: df = gs.get_group(('ago', 2000))
155/43:
for i in [2, 8, 32]:
    print(etllib.bracket_number_from_income(i))
155/44: df.loc[df.index.get_level_values('bracket')[:80]]
155/45: df.loc[:, :, df.index.get_level_values('bracket')[:80]]
155/46: df.loc[:, :, df.index.get_level_values('bracket')[:80]].sum()
155/47: df.loc[:, :, df.index.get_level_values('bracket')[:80]]
155/48: df.loc[:, :, df.index.get_level_values('bracket')[:80]].sum()
155/49: df.loc[:, :, 80:100].sum()
155/50: df.loc[:, :, 80:100]
155/51: df.loc[:, :, :80]
155/52: df.loc[:, :, :79].sum()
161/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
161/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
161/3:
%load_ext autoreload
%autoreload 1
161/4:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
161/5: from ddf_utils.factory.common import download
161/6:
lines = {
    'owd': [1.9, 3.2, 5.5, 10],
    'on': [2, 8, 32]
}
161/7: all_lines = [1.9, 3.2, 5.5, 10, 2, 8, 32]
161/8:
url_tmpl = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?YearSelected=all&PovertyLine={}&Countries=all&display=C&format=csv"
for l in all_lines:
    file_csv = os.path.join('../source', 'poverty_rates', f'{l}.csv')
    url = url_tmpl.format(l)
    print(file_csv)
    download(url, file_csv, resume=True, progress_bar=False, backoff=5, timeout=60)
161/9: all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
161/10: etllib.load_file_preprocess('../source/poverty_rates/1.9.csv')
161/11: owd_lines = lines['owd']
161/12: mountain_ptc = pd.read_csv('../../ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
161/13: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
161/14:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
161/15: mountain_ptc
161/16: pop
161/17:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
161/18: mountain_num = pd.concat(mountain_num)
161/19: mountain_num
161/20: # mountain_num.to_csv('../../ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
161/21:
for i in [2, 8, 32]:
    print(etllib.bracket_number_from_income(i))
161/22: gs = mountain_ptc.groupby(['country', 'year'])
161/23: df = gs.get_group(('ago', 2000))
161/24: df.loc[:, :, df.index.get_level_values('bracket')[:80]].sum()
161/25: df.loc[:, :, :79].sum()
161/26:
def get_on_rates(ser):
    res = []
    thresholes = [2, 8, 32]
    brackets = list(map(etllib.brackets_nu))
    for i in [2, 8, 32]:
        res.append()
163/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
163/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
163/3:
%load_ext autoreload
%autoreload 1
163/4:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
163/5: all_brackets = np.logspace(-7, 13, 201, endpoint=True, base=2)
163/6: etllib.load_file_preprocess('../source/poverty_rates/1.9.csv')
163/7: mountain_ptc = pd.read_csv('../../ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
163/8: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
163/9:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
163/10: mountain_ptc
163/11: pop
163/12:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
163/13: mountain_num = pd.concat(mountain_num)
163/14: mountain_num
163/15:
for i in [2, 8, 32]:
    print(etllib.bracket_number_from_income(i))
163/16: gs = mountain_ptc.groupby(['country', 'year'])
163/17: df = gs.get_group(('ago', 2000))
163/18: df.loc[:, :, df.index.get_level_values('bracket')[:80]].sum()
163/19: df.loc[:, :, :79].sum()
163/20:
def get_on_rates(ser):
    res = []
    thresholes = [2, 8, 32]
    brackets = list(map(etllib.bracket_number_from_income, thresholes))
    
    res.append(ser.loc[:, :, :(brackets[0] - 1)].sum())
    res.append(ser.loc[:, :, brackets[0]:(brackets[1] - 1)].sum())
    res.append(ser.loc[:, :, brackets[1]:(brackets[2] - 1)].sum())
    res.append(ser.loc[:, :, brackets[2]:].sum())
    
    return np.array(res)
163/21: get_on_rates(df['income_mountain'])
163/22:
def get_on_rates(ser):
    res = []
    thresholes = [2, 8, 32]
    brackets = list(map(etllib.bracket_number_from_income, thresholes))
    print(brackets)
    res.append(ser.loc[:, :, :(brackets[0] - 1)].sum())
    res.append(ser.loc[:, :, brackets[0]:(brackets[1] - 1)].sum())
    res.append(ser.loc[:, :, brackets[1]:(brackets[2] - 1)].sum())
    res.append(ser.loc[:, :, brackets[2]:].sum())
    
    return np.array(res)
163/23: get_on_rates(df['income_mountain'])
163/24: df.loc[:, :, 80:99].sum()
163/25: df.loc[:, :, 100:119].sum()
163/26: df.loc[:, :, 120:].sum()
163/27: res1 = gs.apply(get_on_rates)
163/28:
def get_on_rates(ser):
    res = []
    thresholes = [2, 8, 32]
    brackets = list(map(etllib.bracket_number_from_income, thresholes))
    # print(brackets)
    res.append(ser.loc[:, :, :(brackets[0] - 1)].sum())
    res.append(ser.loc[:, :, brackets[0]:(brackets[1] - 1)].sum())
    res.append(ser.loc[:, :, brackets[1]:(brackets[2] - 1)].sum())
    res.append(ser.loc[:, :, brackets[2]:].sum())
    
    return np.array(res)
163/29: res1 = gs.apply(get_on_rates)
163/30:
thresholes = [2, 8, 32]
brackets = list(map(etllib.bracket_number_from_income, thresholes))

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.loc[:, :, :(brackets[0] - 1)].sum())
    res.append(ser.loc[:, :, brackets[0]:(brackets[1] - 1)].sum())
    res.append(ser.loc[:, :, brackets[1]:(brackets[2] - 1)].sum())
    res.append(ser.loc[:, :, brackets[2]:].sum())
    
    return np.array(res)
163/31: get_on_rates(df['income_mountain'])
163/32: res1 = gs.apply(get_on_rates)
163/33: res1 = gs['income_mountain'].apply(get_on_rates)
163/34: res1
163/35:
thresholes = [2, 8, 32]
brackets = list(map(etllib.bracket_number_from_income, thresholes))

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.loc[:, :, :(brackets[0] - 1)].sum())
    res.append(ser.loc[:, :, brackets[0]:(brackets[1] - 1)].sum())
    res.append(ser.loc[:, :, brackets[1]:(brackets[2] - 1)].sum())
    res.append(ser.loc[:, :, brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'on_level'
    return res
163/36: get_on_rates(df['income_mountain'])
163/37: res1 = gs['income_mountain'].apply(get_on_rates)
163/38: res1
163/39: get_on_rates(df['income_mountain']).sum()
163/40: get_on_rates(df['income_mountain'])
163/41: df.loc[:, :, :80].sum()
163/42: df.iloc[:80].sum()
163/43: df.iloc[:81].sum()
163/44: df.iloc[81:101]
163/45: df.iloc[:81]
163/46: df.iloc[:81].sum()
163/47: df.iloc[81:101].sum()
163/48:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) + 1 for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'on_level'
    return res
163/49: get_on_rates(df['income_mountain'])
163/50: res1 = gs['income_mountain'].apply(get_on_rates)
163/51: res1
163/52: !jupyter --path
163/53: !jupyter-lab --generate-config
163/54: !jupyter --path
163/55: !jupyter lab path
163/56:
res1_num = []

for g, df in res1.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    res1_num.append(df_new)
163/57:
res1_num = []

for g, df in res1.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    res1_num.append(df_new)
163/58: res1_num = pd.concat(res1_num)
163/59: res1_num
163/60: res1
163/61:
on_map = {
    0: 'gm_lvl1',
    1: 'gm_lvl2',
    2: 'gm_lvl3',
    3: 'gm_lvl4'
}

on_ents = {
    'gm_lvl1': '<2$/day',
    'gm_lvl2': '2-8$/day',
    'gm_lvl3': '8-32$/day',
    'gm_lvl4': '>32$/day'
}
163/62: res1.index[2]
163/63: res1.index.get_level('on_level')
163/64: res1.index.get_level_values('on_level')
163/65: res1.index
163/66: res1.index.set_levels(leval=2, list(on_map.values()))
163/67: res1.index.set_levels(list(on_map.values()), level=2)
163/68:
new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index.set_levels(new_vals, level='on_level')
163/69:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index.set_levels([1, 2], level='on_level')
163/70:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index.set_levels(list(on_map.values()), level='on_level')
163/71:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index.set_levels(list(on_map.values()), level='on_level', inplace=True)
163/72:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index = res1.index.set_levels(list(on_map.values()), level='on_level')
163/73: res1
163/74: res1_num
163/75: res1_num.index = res1_num.index.set_levels(list(on_map.values()), level='on_level')
163/76: res1_num
163/77:
res1.index.names = ['country', 'year', 'gm_level']
res1.name = 'poverty_rate'
163/78: res1.min()
163/79:
from ddf_utils.str import format_float_digits
from functools import partial
163/80: formattor = partial(format_float_digits, digits=6)
163/81: res1 = res1.map(formattor)
163/82: res1
163/83: !mkdir  ../../poverty_rates
163/84: !ls ../../
163/85: res1.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--gm_level.csv')
163/86:
res1_num.index.names = ['country', 'year', 'gm_level']
res1_num.name = 'poverty_population'
163/87: res1_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
163/88: res1 = res1.astype(float)
163/89: plt
163/90: plt.plot(res1.loc[('ago', 1981)])
163/91: df
163/92: gs = mountain_ptc.groupby(['country', 'year'])
163/93: df = gs.get_group(('ago', 2000))
163/94: df.loc[:, :, df.index.get_level_values('bracket')[:80]].sum()
163/95: df
163/96: df.cumsum()
163/97: cs = df.cumsum()
163/98: cs.loc[:, :, 80]
163/99:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) + 1 for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(200)
    res = cs.loc[bs]
    res.index.name = 'on_level'
    return res
163/100: res1.loc[('ago', 2000)]
163/101: get_on_rates(df['income_mountain'])
163/102:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(200)
    res = cs.loc[bs]
    res.index.name = 'on_level'
    return res
163/103: get_on_rates(df['income_mountain'])
163/104:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(200)
    res = cs.loc[:, :, bs]
    # res.index.name = 'on_level'
    return res
163/105: get_on_rates(df['income_mountain'])
163/106:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(199)
    res = cs.loc[:, :, bs].reset_index()
    res.index.name = 'on_level'
    return res
163/107: get_on_rates(df['income_mountain'])
163/108:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(199)
    res = cs.loc[:, :, bs].reset_index(, drop=True)
    res.index.name = 'on_level'
    return res
163/109:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    cs = ser.cumsum()
    bs = brackets.copy()
    bs.append(199)
    res = cs.loc[:, :, bs].reset_index(drop=True)
    res.index.name = 'on_level'
    return res
163/110: get_on_rates(df['income_mountain'])
163/111:
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) + 1 for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'on_level'
    return res
163/112: get_on_rates(df['income_mountain'])
163/113: res1
163/114: # now estimate levels in owd
163/115: # we should use the cumsum of income mountain to estimate!
163/116: mountain_pct
163/117: mountain_ptc
163/118: cdf = mountain_ptc.groupby(['country', 'year']).cumsum()
163/119: cdf
163/120:
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]
163/121: brackets
163/122:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
%aimport constants
163/123:
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]

brackets_ = [(np.log2(x) + 7) / constants.brackets_delta for x in to_estimate]
163/124: brackets
163/125: brackets_
163/126: gs = cdf.groupby(['country', 'year'])
163/127: df.get_group(('ago', 2000))
163/128: df = gs.get_group(('ago', 2000))
163/129: df
163/130: gs = cdf.groupby(['country', 'year'])['income_mountain']
163/131: df = gs.get_group(('ago', 2000))
163/132: df.iloc[0]
163/133: df.iloc[brackets_]
163/134: df.iloc[[80, 81]]
163/135: df.iloc[[79, 80]]
163/136: df.iloc[79]
163/137:
a = df.iloc[79]
b = df.iloc[80]

e = (b - a) * (brackets_[0] - 79)
163/138: e
163/139: e + a
163/140:
def get_estimate(df, x):
    a = int(x)
    b = a + 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - a) + ya
163/141: get_estimate(df, brackets_[0])
163/142: np.r[0, [get_estimate(df, x) for x in brackets_], 1]
163/143: np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
163/144: dec = np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
163/145: dec = pd.Series(dec)
163/146: dec
163/147: dec.shift(1) - dec
163/148: dec - dec.shift(1)
163/149: dec.shift(1)
163/150: dec - dec.shift(-1)
163/151: dec.shift(-1)
163/152: dec
163/153: dec.shift(-1) - dec
163/154:
def get_owd_level(ser):
    dec = np.r_[0, [get_estimate(ser, x) for x in brackets_], 1]
    dec = pd.Series(dec)
    res = dec.shift(-1) - dec
    res = res.dropna()
    res.index.name = 'owd_level'
    return res
163/155: get_owd_level(df)
163/156: get_owd_level(df).sum()
163/157: owd_rates = mountain_ptc.groupby(['country', 'year'])['income_mountain'].apply(get_owd_level)
163/158: owd_rates
163/159: owd_rates = cdf.groupby(['country', 'year'])['income_mountain'].apply(get_owd_level)
163/160: owd_rates
163/161:
owd_num = []

for g, df in owd_rates.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    owd_num.append(df_new)
163/162: owd_num = pd.concat(owd_num)
163/163: owd_num
163/164:
owid_map = {
    0: 'owid_lvl1',
    1: 'owid_lvl2',
    2: 'owid_lvl3',
    3: 'owid_lvl4',
    4: 'owid_lvl5'
}

owid_ent = {
    'owid_lvl1': '<1.90$/day',
    'owid_lvl2': '1.9-3.20$/day',
    'owid_lvl3': '3.20-5.50$/day',
    'owid_lvl4': '5.50-10$/day',
    'owid_lvl5': '>10$/day'
}
163/165: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='owid_level')
163/166: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='owd_level')
163/167: owd_num
163/168:
owd_num.index.names = ['country', 'year', 'owid_level']
owd_num.name = ['poverty_population']
163/169:
owd_num.index.names = ['country', 'year', 'owid_level']
owd_num.name = 'poverty_population'
163/170: owd_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--owid_level.csv')
163/171: owd_rates = owd_rates.map(formatter)
163/172: owd_rates = owd_rates.map(formattor)
163/173: owd_rates
163/174: owd_rates.index = owd_rates.index.set_levels(list(owid_map.values()), level='owd_level')
163/175:
owd_rates.index.names = ['country', 'year', 'owid_level']
owd_rates.name = 'poverty_rate'
163/176: owd_rates.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--owid_level.csv')
163/177: on_ent
163/178: on_ents
163/179: pd.DataFrame.from_dict(on_ents)
163/180: pd.DataFrame.from_dict(on_ents, orient='index')
163/181:
on_ent = pd.DataFrame.from_dict(on_ents, orient='index')
on_ent.index.name = 'gm_level'
on_ent.columns = ['name']
163/182: on_ent
163/183: on_ent.to_csv('../../ddf--entities--gm_level.csv')
163/184:
owid_map = {
    0: 'owid_lvl1',
    1: 'owid_lvl2',
    2: 'owid_lvl3',
    3: 'owid_lvl4',
    4: 'owid_lvl5'
}

owid_ents = {
    'owid_lvl1': '<1.90$/day',
    'owid_lvl2': '1.9-3.20$/day',
    'owid_lvl3': '3.20-5.50$/day',
    'owid_lvl4': '5.50-10$/day',
    'owid_lvl5': '>10$/day'
}
163/185:
owid_ent = pd.DataFrame.from_dict(owid_ents, orient='index')
owid_ent.index.name = 'owid_level'
owid_ent.columns = ['name']
163/186: owid_ent
163/187: owid_ent.to_csv('../../ddf--entities--owid_level.csv')
164/1: mountain_ptc = pd.read_csv('../precomputed/ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
164/2:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
164/3:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
164/4:
%load_ext autoreload
%autoreload 1
164/5:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
%aimport constants
164/6: mountain_ptc = pd.read_csv('../precomputed/ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
164/7: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
164/8:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
164/9: mountain_ptc
164/10: pop
164/11:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
164/12: mountain_num = pd.concat(mountain_num)
164/13: mountain_num
164/14: # mountain_num.to_csv('../precomputed/ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
164/15:
for i in [2, 8, 32]:
    print(etllib.bracket_number_from_income(i))
164/16: gs = mountain_ptc.groupby(['country', 'year'])
164/17: df = gs.get_group(('ago', 2000))
164/18:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) + 1 for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/19: get_on_rates(df['income_mountain'])
164/20: gm_rate = gs['income_mountain'].apply(get_on_rates)
164/21: gm_rate
164/22:
gm_num = []

for g, df in gm_rate.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    gm_num.append(df_new)
164/23: gm_num = pd.concat(gm_num)
164/24: gm_num
164/25:
on_map = {
    0: 'gm_lvl1',
    1: 'gm_lvl2',
    2: 'gm_lvl3',
    3: 'gm_lvl4'
}

on_ents = {
    'gm_lvl1': '<2$/day',
    'gm_lvl2': '2-8$/day',
    'gm_lvl3': '8-32$/day',
    'gm_lvl4': '>32$/day'
}
164/26:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

res1.index = res1.index.set_levels(list(on_map.values()), level='on_level')
164/27:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

gm_rate.index = gm_rate.index.set_levels(list(on_map.values()), level='on_level')
164/28:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

gm_rate.index = gm_rate.index.set_levels(list(on_map.values()), level='gm_level')
164/29: gm_rate
164/30: gm_num.index = gm_num.index.set_levels(list(on_map.values()), level='gm_level')
164/31: res1_num
164/32: gm_num
164/33:
gm_rate.index.names = ['country', 'year', 'gm_level']
gm_rate.name = 'poverty_rate'
164/34:
from ddf_utils.str import format_float_digits
from functools import partial
164/35: formattor = partial(format_float_digits, digits=6)
164/36: gm_rate_str = gm_rate.map(formattor)
164/37: gm_rate_str.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--gm_level.csv')
164/38:
gm_num.index.names = ['country', 'year', 'gm_level']
gm_num.name = 'poverty_population'
164/39: gm_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
164/40: # now estimate levels in owd
164/41: # we should use the cumsum of income mountain to estimate!
164/42: mountain_ptc
164/43: cdf = mountain_ptc.groupby(['country', 'year']).cumsum()
164/44: cdf
164/45:
# owid levels
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]

brackets_ = [(np.log2(x) + 7) / constants.brackets_delta for x in to_estimate]
164/46: brackets
164/47: brackets_
164/48: gs = cdf.groupby(['country', 'year'])['income_mountain']
164/49: df = gs.get_group(('ago', 2000))
164/50: df.iloc[[79, 80]]
164/51:
a = df.iloc[79]
b = df.iloc[80]

e = (b - a) * (brackets_[0] - 79)
164/52: e + a
164/53:
def get_estimate(df, x):
    a = int(x)
    b = a + 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - a) + ya
164/54: get_estimate(df, brackets_[0])
164/55: dec = np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
164/56: dec = pd.Series(dec)
164/57: dec.shift(-1) - dec
164/58:
def get_owd_level(ser):
    dec = np.r_[0, [get_estimate(ser, x) for x in brackets_], 1]
    dec = pd.Series(dec)
    res = dec.shift(-1) - dec
    res = res.dropna()
    res.index.name = 'owid_level'
    return res
164/59: get_owd_level(df)
164/60: get_owd_level(df).sum()
164/61: owd_rates = cdf.groupby(['country', 'year'])['income_mountain'].apply(get_owd_level)
164/62: owd_rates
164/63:
owid_map = {
    0: 'owid_lvl1',
    1: 'owid_lvl2',
    2: 'owid_lvl3',
    3: 'owid_lvl4',
    4: 'owid_lvl5'
}

owid_ents = {
    'owid_lvl1': '<1.90$/day',
    'owid_lvl2': '1.9-3.20$/day',
    'owid_lvl3': '3.20-5.50$/day',
    'owid_lvl4': '5.50-10$/day',
    'owid_lvl5': '>10$/day'
}
164/64:
owd_num = []

for g, df in owd_rates.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    owd_num.append(df_new)
164/65: owd_num = pd.concat(owd_num)
164/66: owd_num
164/67: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='owd_level')
164/68: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='oiwd_level')
164/69: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='owid_level')
164/70: owd_num
164/71:
owd_num.index.names = ['country', 'year', 'owid_level']
owd_num.name = 'poverty_population'
164/72: owd_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--owid_level.csv')
164/73: owd_rates = owd_rates.map(formattor)
164/74: owd_rates
164/75: owd_rates.index = owd_rates.index.set_levels(list(owid_map.values()), level='owd_level')
164/76:
owd_rates.index.names = ['country', 'year', 'owid_level']
owd_rates.name = 'poverty_rate'
164/77: owd_rates.index = owd_rates.index.set_levels(list(owid_map.values()), level='owid_level')
164/78:
owd_rates.index.names = ['country', 'year', 'owid_level']
owd_rates.name = 'poverty_rate'
164/79: owd_rates.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--owid_level.csv')
164/80: on_ents
164/81:
on_ent = pd.DataFrame.from_dict(on_ents, orient='index')
on_ent.index.name = 'gm_level'
on_ent.columns = ['name']
164/82: on_ent
164/83: on_ent.to_csv('../../ddf--entities--gm_level.csv')
164/84:
owid_ent = pd.DataFrame.from_dict(owid_ents, orient='index')
owid_ent.index.name = 'owid_level'
owid_ent.columns = ['name']
164/85: owid_ent
164/86: owid_ent.to_csv('../../ddf--entities--owid_level.csv')
165/1:
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
165/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
165/3:
%load_ext autoreload
%autoreload 1
165/4:
%aimport shapeslib
%aimport constants
165/5: allb = constants.all_brackets
165/6: allb
165/7: len(allb)
165/8: brackets = pd.Series(allb)
165/9: brackets
165/10: bracket_df = pd.DataFrame([brackets, brackets.shift(-1)]).T
165/11: bracket_df.columns = ['bracket_start', 'bracket_end']
165/12: bracket_df.index.name = 'bracket'
165/13: bracket_200 = bracket_df.iloc[:-1].copy()
165/14: bracket_200
165/15: bracket_200.index.name = 'income_bracket_200'
165/16: bracket_200.to_csv('../../ddf--entities--income_bracket_200.csv')
165/17: btoi = shapeslib.bracket_number_from_income
165/18:
%aimport etllib
%aimport constants
165/19: btoi = etllib.bracket_number_from_income
165/20: btoi(8000)
165/21: btoi(8000, integer=False)
165/22: btoi(8001, integer=False)
165/23: btoi(8001, integer=False)
165/24: bracket_df.loc[btoi(8001)]  # make sure calculation is correct
165/25: btoi(8001)
165/26: bracket_df
165/27: bracket_200
165/28: bracket_200.loc[btoi(8001)]  # make sure calculation is correct
165/29: bracket_200.loc[btoi(2)]
164/87:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) + 1 for x in thresholes]

def get_on_rates(ser):
    res = []
    print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/88: get_on_rates(df['income_mountain'])
164/89:
gs = mountain_ptc.groupby(['country', 'year'])
df = gs.get_group(('ago', 2000))
164/90: get_on_rates(df['income_mountain'])
164/91: df.iloc[:81]
164/92:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) - 1 for x in thresholes]

def get_on_rates(ser):
    res = []
    print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/93: get_on_rates(df['income_mountain'])
164/94: df.iloc[:79]
164/95:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    res = []
    print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/96: get_on_rates(df['income_mountain'])
164/97: df.iloc[:80]
164/98: df.iloc[80:100]
164/99:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    res = []
    print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/100:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
164/101: get_on_rates(df['income_mountain'])
164/102: gm_rate = gs['income_mountain'].apply(get_on_rates)
164/103: gm_rate
164/104:
gm_num = []

for g, df in gm_rate.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    gm_num.append(df_new)
164/105: gm_num = pd.concat(gm_num)
164/106: gm_num
164/107:
on_map = {
    0: 'gm_lvl1',
    1: 'gm_lvl2',
    2: 'gm_lvl3',
    3: 'gm_lvl4'
}

on_ents = {
    'gm_lvl1': '<2$/day',
    'gm_lvl2': '2-8$/day',
    'gm_lvl3': '8-32$/day',
    'gm_lvl4': '>32$/day'
}
164/108:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

gm_rate.index = gm_rate.index.set_levels(list(on_map.values()), level='gm_level')
164/109: gm_rate
164/110: gm_num.index = gm_num.index.set_levels(list(on_map.values()), level='gm_level')
164/111: gm_num
164/112:
gm_rate.index.names = ['country', 'year', 'gm_level']
gm_rate.name = 'poverty_rate'
164/113:
from ddf_utils.str import format_float_digits
from functools import partial
164/114: formattor = partial(format_float_digits, digits=6)
164/115: gm_rate_str = gm_rate.map(formattor)
164/116: gm_rate_str.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--gm_level.csv')
164/117:
gm_num.index.names = ['country', 'year', 'gm_level']
gm_num.name = 'poverty_population'
164/118: gm_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
165/30: bracket_200.loc[btoi(1.9)]
165/31: bracket_200.iloc[btoi(1.9)]
165/32: bracket_200.iloc[btoi(1.9)] == bracket_200.loc[btoi(1.9)]
164/119:
# 79: below 2
# 78: below 1.866066
df.iloc[[78, 79]]
164/120: gs = cdf.groupby(['country', 'year'])['income_mountain']
164/121: df = gs.get_group(('ago', 2000))
164/122:
# 79: below 2
# 78: below 1.866066
df.iloc[[78, 79]]
164/123:
# assuming linear function between 2 known brackets.
def get_estimate(df, x):
    b = int(x)
    a = b - 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - a) + ya
164/124:
a = df.iloc[78]
b = df.iloc[79]

e = (b - a) * (brackets_[0] - 78)
164/125: e + a
164/126:
a = df.iloc[78]
b = df.iloc[79]

e = (b - a) * (brackets_[0] - 79)
164/127: e + a
164/128:
# assuming linear function between 2 known brackets.
def get_estimate(df, x):
    b = int(x)
    a = b - 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - b) + ya
164/129: get_estimate(df, brackets_[0])
164/130: dec = np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
164/131: dec = pd.Series(dec)
164/132: dec.shift(-1) - dec
164/133:
def get_owd_level(ser):
    dec = np.r_[0, [get_estimate(ser, x) for x in brackets_], 1]
    dec = pd.Series(dec)
    res = dec.shift(-1) - dec
    res = res.dropna()
    res.index.name = 'owid_level'
    return res
164/134: get_owd_level(df)
164/135: get_owd_level(df).sum()
164/136: owd_rates = cdf.groupby(['country', 'year'])['income_mountain'].apply(get_owd_level)
166/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
166/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
166/3:
%load_ext autoreload
%autoreload 1
166/4:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
%aimport constants
166/5: mountain_ptc = pd.read_csv('../precomputed/ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
166/6: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
166/7:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
166/8: mountain_ptc
166/9: pop
166/10:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
166/11: mountain_num = pd.concat(mountain_num)
166/12: mountain_num
166/13: # mountain_num.to_csv('../precomputed/ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
166/14:
gs = mountain_ptc.groupby(['country', 'year'])
df = gs.get_group(('ago', 2000))
166/15:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
166/16: get_on_rates(df['income_mountain'])
166/17: gm_rate = gs['income_mountain'].apply(get_on_rates)
166/18: gm_rate
166/19:
gm_num = []

for g, df in gm_rate.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    gm_num.append(df_new)
166/20: gm_num = pd.concat(gm_num)
166/21: gm_num
166/22:
on_map = {
    0: 'gm_lvl1',
    1: 'gm_lvl2',
    2: 'gm_lvl3',
    3: 'gm_lvl4'
}

on_ents = {
    'gm_lvl1': '<2$/day',
    'gm_lvl2': '2-8$/day',
    'gm_lvl3': '8-32$/day',
    'gm_lvl4': '>32$/day'
}
166/23:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

gm_rate.index = gm_rate.index.set_levels(list(on_map.values()), level='gm_level')
166/24: gm_rate
166/25: gm_num.index = gm_num.index.set_levels(list(on_map.values()), level='gm_level')
166/26: gm_num
166/27:
gm_rate.index.names = ['country', 'year', 'gm_level']
gm_rate.name = 'poverty_rate'
166/28:
from ddf_utils.str import format_float_digits
from functools import partial
166/29: formattor = partial(format_float_digits, digits=6)
166/30: gm_rate_str = gm_rate.map(formattor)
166/31: # !mkdir  ../../poverty_rates
166/32: gm_rate_str.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--gm_level.csv')
166/33:
gm_num.index.names = ['country', 'year', 'gm_level']
gm_num.name = 'poverty_population'
166/34: gm_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
166/35: # now estimate levels in owd
166/36: # we should use the cumsum of income mountain to estimate!
166/37: mountain_ptc
166/38: cdf = mountain_ptc.groupby(['country', 'year']).cumsum()
166/39: cdf
166/40:
# owid levels
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]

brackets_ = [(np.log2(x) + 7) / constants.brackets_delta for x in to_estimate]
166/41: brackets
166/42: brackets_
166/43: gs = cdf.groupby(['country', 'year'])['income_mountain']
166/44: df = gs.get_group(('ago', 2000))
166/45:
# 79: below 2
# 78: below 1.866066
df.iloc[[78, 79]]
166/46:
a = df.iloc[78]
b = df.iloc[79]

e = (b - a) * (brackets_[0] - 79)
166/47: e + a
166/48:
# assuming linear function between 2 known brackets.
def get_estimate(df, x):
    b = int(x)
    a = b - 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - b) + ya
166/49: get_estimate(df, brackets_[0])
166/50: dec = np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
166/51: dec = pd.Series(dec)
166/52: dec.shift(-1) - dec
166/53:
def get_owd_level(ser):
    dec = np.r_[0, [get_estimate(ser, x) for x in brackets_], 1]
    dec = pd.Series(dec)
    res = dec.shift(-1) - dec
    res = res.dropna()
    res.index.name = 'owid_level'
    return res
166/54: get_owd_level(df)
166/55: get_owd_level(df).sum()
166/56: owd_rates = cdf.groupby(['country', 'year'])['income_mountain'].apply(get_owd_level)
166/57: owd_rates
166/58:
owid_map = {
    0: 'owid_lvl1',
    1: 'owid_lvl2',
    2: 'owid_lvl3',
    3: 'owid_lvl4',
    4: 'owid_lvl5'
}

owid_ents = {
    'owid_lvl1': '<1.90$/day',
    'owid_lvl2': '1.9-3.20$/day',
    'owid_lvl3': '3.20-5.50$/day',
    'owid_lvl4': '5.50-10$/day',
    'owid_lvl5': '>10$/day'
}
166/59:
owd_num = []

for g, df in owd_rates.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    owd_num.append(df_new)
166/60: owd_num = pd.concat(owd_num)
166/61: owd_num
166/62: owd_num.index = owd_num.index.set_levels(list(owid_map.values()), level='owid_level')
166/63: owd_num
166/64:
owd_num.index.names = ['country', 'year', 'owid_level']
owd_num.name = 'poverty_population'
166/65: owd_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--owid_level.csv')
166/66: owd_rates = owd_rates.map(formattor)
166/67: owd_rates
166/68: owd_rates.index = owd_rates.index.set_levels(list(owid_map.values()), level='owid_level')
166/69:
owd_rates.index.names = ['country', 'year', 'owid_level']
owd_rates.name = 'poverty_rate'
166/70: owd_rates.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--owid_level.csv')
166/71: on_ents
166/72:
on_ent = pd.DataFrame.from_dict(on_ents, orient='index')
on_ent.index.name = 'gm_level'
on_ent.columns = ['name']
166/73: on_ent
166/74: on_ent.to_csv('../../ddf--entities--gm_level.csv')
166/75:
owid_ent = pd.DataFrame.from_dict(owid_ents, orient='index')
owid_ent.index.name = 'owid_level'
owid_ent.columns = ['name']
166/76: owid_ent
166/77: owid_ent.to_csv('../../ddf--entities--owid_level.csv')
166/78:
# owid levels
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]

brackets_ = [etllib.bracket_number_from_income(x, integer=False) for x in to_estimate]
166/79: brackets
166/80: brackets_
167/1:
import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, '../scripts/')

import matplotlib.pyplot as plt

%matplotlib inline
167/2:
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['figure.dpi'] = 196
167/3:
%load_ext autoreload
%autoreload 1
167/4:
%aimport etllib
%aimport shapeslib
%aimport smoothlib
%aimport constants
167/5: mountain_ptc = pd.read_csv('../precomputed/ddf--datapoints--income_mountain--by--country--year--bracket_200.csv')
167/6: mountain_ptc = mountain_ptc.set_index(['country', 'year', 'bracket'])
167/7:
pop_file = '../../../ddf--gapminder--systema_globalis/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv'
pop = pd.read_csv(pop_file).set_index(['geo', 'time'])['population_total']
167/8: mountain_ptc
167/9: pop
167/10:
mountain_num = []

for g, df in mountain_ptc.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new['income_mountain'] = np.round(df_new['income_mountain']).astype(int)
    mountain_num.append(df_new)
167/11: mountain_num = pd.concat(mountain_num)
167/12: mountain_num
167/13: # mountain_num.to_csv('../precomputed/ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
167/14:
gs = mountain_ptc.groupby(['country', 'year'])
df = gs.get_group(('ago', 2000))
167/15:
# gapminder levels
thresholes = [2, 8, 32]
brackets = [etllib.bracket_number_from_income(x) for x in thresholes]

def get_on_rates(ser):
    res = []
    # print(brackets)
    res.append(ser.iloc[:brackets[0]].sum())
    res.append(ser.iloc[brackets[0]:brackets[1]].sum())
    res.append(ser.iloc[brackets[1]:brackets[2]].sum())
    res.append(ser.iloc[brackets[2]:].sum())
    
    res = pd.Series(res)
    res.index.name = 'gm_level'
    return res
167/16: get_on_rates(df['income_mountain'])
167/17: gm_rate = gs['income_mountain'].apply(get_on_rates)
167/18: gm_rate
167/19:
gm_num = []

for g, df in gm_rate.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    gm_num.append(df_new)
167/20: gm_num = pd.concat(gm_num)
167/21: gm_num
167/22:
on_map = {
    0: 'gm_lvl1',
    1: 'gm_lvl2',
    2: 'gm_lvl3',
    3: 'gm_lvl4'
}

on_ents = {
    'gm_lvl1': '<2$/day',
    'gm_lvl2': '2-8$/day',
    'gm_lvl3': '8-32$/day',
    'gm_lvl4': '>32$/day'
}
167/23:
# new_vals = res1.index.get_level_values('on_level').map(on_map)

gm_rate.index = gm_rate.index.set_levels(list(on_map.values()), level='gm_level')
167/24: gm_rate
167/25: gm_num.index = gm_num.index.set_levels(list(on_map.values()), level='gm_level')
167/26: gm_num
167/27:
gm_rate.index.names = ['country', 'year', 'gm_level']
gm_rate.name = 'poverty_rate'
167/28:
from ddf_utils.str import format_float_digits
from functools import partial
167/29: formattor = partial(format_float_digits, digits=6)
167/30: gm_rate_str = gm_rate.map(formattor)
167/31: # !mkdir  ../../poverty_rates
167/32: gm_rate_str.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--gm_level.csv')
167/33:
gm_num.index.names = ['country', 'year', 'gm_level']
gm_num.name = 'poverty_population'
167/34: gm_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
167/35: # now estimate levels in owd
167/36: # we should use the cumsum of income mountain to estimate!
167/37: mountain_ptc
167/38: cdf = mountain_ptc.groupby(['country', 'year']).cumsum()
167/39: cdf
167/40:
# owid levels
to_estimate = [1.9, 3.2, 5.5, 10]

brackets = [etllib.bracket_number_from_income(x) for x in to_estimate]

brackets_ = [etllib.bracket_number_from_income(x, integer=False) for x in to_estimate]
167/41: brackets
167/42: brackets_
167/43: gs = cdf.groupby(['country', 'year'])['income_mountain']
167/44: df = gs.get_group(('ago', 2000))
167/45:
# 79: below 2
# 78: below 1.866066
df.iloc[[78, 79]]
167/46:
a = df.iloc[78]
b = df.iloc[79]

e = (b - a) * (brackets_[0] - 79)
167/47: e + a
167/48:
# assuming linear function between 2 known brackets.
def get_estimate(df, x):
    b = int(x)
    a = b - 1
    
    ya = df.iloc[a]
    yb = df.iloc[b]
    
    return (yb - ya) * (x - b) + ya
167/49: get_estimate(df, brackets_[0])
167/50: dec = np.r_[0, [get_estimate(df, x) for x in brackets_], 1]
167/51: dec = pd.Series(dec)
167/52: dec.shift(-1) - dec
167/53:
def get_wb_level(ser):
    dec = np.r_[0, [get_estimate(ser, x) for x in brackets_], 1]
    dec = pd.Series(dec)
    res = dec.shift(-1) - dec
    res = res.dropna()
    res.index.name = 'wb_level'
    return res
167/54: get_wb_level(df)
167/55: get_wb_level(df).sum()
167/56: wb_rates = cdf.groupby(['country', 'year'])['income_mountain'].apply(get_wb_level)
167/57: wb_rates
167/58:
wb_map = {
    0: 'wb_lvl1',
    1: 'wb_lvl2',
    2: 'wb_lvl3',
    3: 'wb_lvl4',
    4: 'wb_lvl5'
}

wb_ents = {
    'wb_lvl1': '<1.90$/day',
    'wb_lvl2': '1.9-3.20$/day',
    'wb_lvl3': '3.20-5.50$/day',
    'wb_lvl4': '5.50-10$/day',
    'wb_lvl5': '>10$/day'
}
167/59:
wb_num = []

for g, df in wb_rates.groupby(['country', 'year']):
    try:
        p = pop.loc[g]
    except KeyError:
        print(f'no population for {g}')
        continue
    df_new = df * p
    df_new = np.round(df_new).astype(int)
    wb_num.append(df_new)
167/60: wb_num = pd.concat(wb_num)
167/61: wb_num
167/62: wb_num.index = wb_num.index.set_levels(list(wb_map.values()), level='wb_level')
167/63: owd_num
167/64: owd_num
167/65: wb_num
167/66:
wb_num.index.names = ['country', 'year', 'wb_level']
wb_num.name = 'poverty_population'
167/67: wb_num.to_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--year--wb_level.csv')
167/68: wb_rates = wb_rates.map(formattor)
167/69: wb_rates
167/70: wb_rates.index = wb_rates.index.set_levels(list(wb_map.values()), level='wb_level')
167/71:
wb_rates.index.names = ['country', 'year', 'wb_level']
wb_rates.name = 'poverty_rate'
167/72: wb_rates.to_csv('../../poverty_rates/ddf--datapoints--poverty_rate--by--country--year--wb_level.csv')
167/73: on_ents
167/74:
on_ent = pd.DataFrame.from_dict(on_ents, orient='index')
on_ent.index.name = 'gm_level'
on_ent.columns = ['name']
167/75: on_ent
167/76: on_ent.to_csv('../../ddf--entities--gm_level.csv')
167/77:
wb_ent = pd.DataFrame.from_dict(wb_ents, orient='index')
wb_ent.index.name = 'wb_level'
wb_ent.columns = ['name']
167/78: wb_ent
167/79: wb_ent.to_csv('../../ddf--entities--wb_level.csv')
168/1: import certifi
168/2: certifi.where()
168/3: !ls -lah /home/semio/.pyvenv/gapminder/lib/python3.9/site-packages/certifi/cacert.pem
168/4: !head /home/semio/.pyvenv/gapminder/lib/python3.9/site-packages/certifi/cacert.pem
168/5: certifi.version
168/6: certifi.__version__
169/1: import certifi
169/2: certifi.__version__
169/3: certifi.where()
169/4: a = certifi.where()
169/5: !head $a
169/6: !ls -lah $a
171/1:
import pandas as pd
import requests
from ddf_utils.factory import download
171/2:
import pandas as pd
import requests
from ddf_utils.factory.common import download
171/3: download?
171/4: endpoint = "https://www.fao.org/aquastat/statistics/query/results.html"
171/5:
post_data = {
    'regionQuery': False,
    'yearGrouping': "SURVEY",
    'showCodes': False,
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': [4100],
    'cntIds': [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894],
    'regIds': [],
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': True,
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': False,
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/6: download(endpoint, './test.html', method='POST', post_data=post_data)
171/7: !head test.html
171/8: import json
171/9:
with open('form.json', 'w') as f:
    json.dump(post_data, f)
171/10: !head test.html
171/11: pd.read_html('./test.html')
171/12:
with open('form.json', 'w') as f:
    json.dump(post_data, f, indent=2)
171/13:
post_data = {
    'regionQuery': False,
    'yearGrouping': "SURVEY",
    'showCodes': False,
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': ','.join([4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]),
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': True,
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': False,
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/14:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

post_data = {
    'regionQuery': False,
    'yearGrouping': "SURVEY",
    'showCodes': False,
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': ','.join(cntids),
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': True,
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': False,
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/15: ','.join(cntids)
171/16:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

post_data = {
    'regionQuery': False,
    'yearGrouping': "SURVEY",
    'showCodes': False,
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': ','.join(map(str, cntids)),
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': True,
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': False,
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/17:
with open('form.json', 'w') as f:
    json.dump(post_data, f, indent=2)
171/18:
with open('form.json', 'w') as f:
    for k, v in post_data.items():
        line = '='.join([k, str(v).lower()])
        f.write(line)
        f.write('\n')
    f.close()
171/19: pd.read_html('./test.html')
171/20: pd.read_html('./test.html')[0]
171/21:
frontpage = "https://www.fao.org/aquastat/statistics/query/index.html"
endpoint = "https://www.fao.org/aquastat/statistics/query/results.html"
171/22: res = request.get(frontpage)
171/23: res = requests.get(frontpage)
171/24: res.cookies
171/25:
session = requests.Session()
session.get(frontpage)
171/26:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

post_data = {
    'regionQuery': False,
    'yearGrouping': "SURVEY",
    'showCodes': False,
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': '4',
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': True,
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': False,
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/27: res = session.post(endpoint, post_data=post_data)
171/28: res = session.post(endpoint, data=post_data)
171/29: res
171/30: res.request.headers
171/31: res.request.body
171/32:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

post_data = {
    'regionQuery': 'false',
    'yearGrouping': "SURVEY",
    'showCodes': 'false',
    'yearRang.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': '4',
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': 'true',
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': 'false',
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/33: res = session.post(endpoint, data=post_data)
171/34: res
171/35: res.content
171/36: pd.read_html(res.content)
171/37: pd.read_html(res.content)[0]
171/38: res.content
171/39:
with open('test2.html', 'wb') as f:
    f.write(res.content)
171/40:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

post_data = {
    'regionQuery': 'false',
    'yearGrouping': "SURVEY",
    'showCodes': 'false',
    'yearRange.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': '4',
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': 'true',
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': 'false',
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/41: res = session.post(endpoint, data=post_data)
171/42: res
171/43: res.request.headers
171/44: res.request.body
171/45: pd.read_html(res.content)[0]
171/46:
with open('test2.html', 'wb') as f:
    f.write(res.content)
171/47:
session = requests.Session()
session.get(frontpage)
171/48:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    res = session.post(endpoint, data=post_data)
    return pd.read_html(res.content)[0]
171/49: df = get_indicator(4112)
171/50: df
171/51: import re
171/52: rg = re.complie(r'[\d\w]+\(\d+\)')
171/53: rg = re.compile(r'[\d\w]+\(\d+\)')
171/54: rg.match('546 666 678(1962)')
171/55: rg = re.compile(r'[\d\s]+\(\d+\)')
171/56: rg.match('546 666 678(1962)')
171/57: rg = re.compile(r'([\d\s]+)\((\d+)\)')
171/58: rg.match('546 666 678(1962)')
171/59: m = rg.match('546 666 678(1962)')
171/60: m
171/61: m.groups
171/62: m.groups()
171/63: rg = re.compile(r'([\d\s.]+)\((\d+)\)')
171/64: m = rg.match('546 666 678(1962)')
171/65: m.groups()
171/66: df.columns[0] = 'country'
171/67:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    res = session.post(endpoint, data=post_data)
    return pd.read_html(res.content, index_col=0)[0]
171/68: df = get_indicator(4112)
171/69: df
171/70:
df.index.name = 'country'
df = df.iloc[:, ::2]
171/71: df
171/72:
frontpage = "https://www.fao.org/aquastat/statistics/query/index.html"
endpoint = "https://www.fao.org/aquastat/statistics/query/results.html"
csv_endpoint = "https://www.fao.org/aquastat/statistics/query/results.html?csv=2"
171/73:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return pd.resd_csv(res.content)
171/74: df = get_indicator(4112)
171/75:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return res
171/76: df = get_indicator(4112)
171/77: pd.read_csv(df.content)
171/78: from IO import BytesIO
171/79: from io import BytesIO
171/80: pd.read_(BytesIO(df.content))
171/81: pd.read_csv(BytesIO(df.content))
171/82:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return pd.read_csv(BytesIO(res.content))
171/83: df = get_indicator(4112)
171/84:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return pd.read_csv(BytesIO(res.content), skipfooter=8)
171/85: df = get_indicator(4112)
171/86: df
171/87: df.index
171/88:
df = df.reset_index()
df
171/89:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return pd.read_csv(BytesIO(res.content), skipfooter=8, index_col=False)
171/90: df = get_indicator(4112)
171/91: df
171/92:
def get_indicator(code):
    post_data['varGrpIds'] = str(code)
    session.post(endpoint, data=post_data)
    res = session.get(csv_endpoint)
    return pd.read_csv(BytesIO(res.content), skipfooter=8, engine='python')
171/93:
df = df.reset_index()
df
171/94:
post_data = {
    'regionQuery': 'false',
    'yearGrouping': "SURVEY",
    'showCodes': 'false',
    'yearRange.fromYear': 1958,
    'yearRange.toYear': 2022,
    'varGrpIds': '4100',
    'cntIds': ','.join(map(str, cntids)),
    'regIds': '',
    'edit': 0,
    'save': 0,
    'query_type': 'querypage',
    'lowBandwidth': 1,
    '_newestOnly': 'on',
    'showValueYears': 'true',
    '_showValueYears': 'on',
    'XAxis': 'YEAR',
    'YAxis': 'AREA',
    'showSymbols': 'false',
    '_showSymbols': 'off',
    '_hideEmptyRowsColumns': 'on'
}
171/95:
cntids = [4,8,12,20,24,28,31,32,36,40,44,48,50,51,52,56,
               64,68,70,72,76,84,90,96,100,104,108,112,116,120,
               124,132,140,144,148,152,156,170,174,178,180,184,
               188,191,192,196,203,204,208,212,214,218,222,226,
               231,232,233,234,242,246,250,262,266,268,270,275,
               276,288,296,300,308,320,324,328,332,336,340,348,
               352,356,360,364,368,372,376,380,384,388,392,398,
               400,404,408,410,414,417,418,422,426,428,430,434,
               438,440,442,450,454,458,462,466,470,478,480,484,
               492,496,498,499,504,508,512,516,520,524,528,548,
               554,558,562,566,570,578,583,584,585,586,591,598,
               600,604,608,616,620,624,626,630,634,642,643,646,
               659,662,670,674,678,682,686,688,690,694,702,703,
               704,705,706,710,716,724,728,729,740,748,752,756,
               760,762,764,768,772,776,780,784,788,792,795,798,
               800,804,807,818,826,834,840,854,858,860,862,882,887,894]

varGrps = [
    4100,4101,4102,4103,4104,4105,4106,4107,4111,4112,4113,4114,4115,4116,
    4150,4151,4154,4155,4156,4157,4158,4159,4160,4161,4162,4164,4165,4168,
    4171,4172,4173,4174,4176,4177,4178,4182,4185,4187,4188,4190,4192,4193,
    4194,4195,4196,4197,4250,4251,4252,4253,4254,4255,4256,4257,4260,4261,
    4262,4263,4264,4265,4269,4270,4273,4275,4300,4303,4304,4305,4307,4308,
    4309,4310,4311,4312,4313,4314,4315,4316,4317,4318,4319,4320,4321,4322,
    4323,4324,4325,4326,4327,4328,4329,4330,4331,4345,4346,4347,4348,4349,
    4350,4351,4352,4353,4354,4355,4356,4357,4358,4359,4360,4361,4362,4363,
    4364,4365,4366,4367,4368,4369,4370,4371,4372,4373,4374,4375,4376,4377,
    4378,4379,4400,4401,4403,4445,4446,4451,4452,4453,4458,4461,4463,4464,
    4465,4466,4469,4470,4471,4472,4473,4474,4475,4490,4491,4493,4509,4510,
    4512,4513,4514,4515,4516,4517,4520,4521,4522,4523,4524,4525,4526,4527,
    4533,4534,4535,4536,4537,4539,4540,4541,4542,4543,4544,4545,4546,4547,
    4548,4549,4550,4551,4552,4553,4554,4555,4556,4557,4558
]
171/96: df = get_indicator(4112)
171/97:
df = df.reset_index()
df
171/98: !mkdir ../source/new
171/99: len(varGrps)
175/1: import pandas as pd
175/2: df = pd.read_csv('../precomputed/ddf--datapoints--income_mountain_numbers--by--country--year--bracket_200.csv')
175/3: df.head()
175/4:
def resample(ser):
    res = pd.Series([x.sum() for x in np.split(ser.values, 50)])
    res.index.name = 'bracket'
    return res
175/5: res9 = res8.groupby(['country', 'year']).apply(resample)
175/6: res9 = df.groupby(['country', 'year']).apply(resample)
175/7:
import pandas as pd
import numpy as np
175/8: res9 = df.groupby(['country', 'year']).apply(resample)
175/9: res9
175/10: res9.stack()
175/11: res9.stack().groupby(['country']).max()
175/12: res = res9.stack().groupby(['country']).max()
175/13: res
175/14: res.loc['chn']
175/15: ent = pd.read_csv('../../ddf--entities--geo--country.csv', dtype=str, keep_default_na=False)
175/16: ent
175/17:
ent = ent.set_index('country')
ent['income_mountain_50bracket_max_height_for_log'] = res
175/18: ent
175/19: ent[pd.isnull(ent.income_mountain_50bracket_max_height_for_log)]
175/20:
ent['income_mountain_50bracket_max_height_for_log'] = ent['income_mountain_50bracket_max_height_for_log'].map(
    lambda x: '' if pd.isnull(x) else str(x) 
)
175/21: ent
175/22: ent['income_mountain_50bracket_max_height_for_log'] = res
175/23:
ent['income_mountain_50bracket_max_height_for_log'] = ent['income_mountain_50bracket_max_height_for_log'].map(
    lambda x: '' if pd.isnull(x) else str(int(x)) 
)
175/24: ent
175/25: ent.to_csv('../../ddf--entities--geo--country.csv')
175/26: res9
175/27: res9.loc[('chn', 1989)]
175/28: res9.loc[('chn', 1990)]
175/29: df = pd.read_csv('../../income_mountain/ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
175/30: df
175/31: df = df.set_index(['country', 'year'])
175/32: df.loc[('chn', 1990)]
175/33:
def findmax(val):
    val_int = map(int, val.split(','))
    return max(val_int)
175/34: findmax(df.loc[('chn', 1990)])
175/35: findmax(df.loc[('chn', 1990), 'income_mountain_50bracket_shape_for_log'])
175/36: df = pd.read_csv('../../income_mountain/ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
175/37: df = df.set_index(['country', 'year'])['income_mountain_50bracket_shape_for_log']
175/38: df.loc[('chn', 1990)]
175/39: findmax(df.loc[('chn', 1990)])
175/40:
def findmax_in_series(ser):
    return ser.map(findmax).max()
175/41: findmax_in_series(df.loc['chn'])
175/42: res = df.groupby('country').apply(findmax_in_series)
175/43: res
175/44: ent['income_mountain_50bracket_max_height_for_log'] = res
175/45: ent[pd.isnull(ent.income_mountain_50bracket_max_height_for_log)]
175/46:
ent['income_mountain_50bracket_max_height_for_log'] = ent['income_mountain_50bracket_max_height_for_log'].map(
    lambda x: '' if pd.isnull(x) else str(int(x)) 
)
175/47: ent
175/48: ent.to_csv('../../ddf--entities--geo--country.csv')
177/1: import pandas as pd
177/2:
import pandas as pd
import os.path as osp
177/3: !ls ../source/fixtures/
177/4: !ls -lah../source/fixtures/
177/5: !ls -lah ../source/fixtures/
177/6: pd.read_csv(osp.join('../source/fixture', 'povcal_country_year.csv'))
177/7: pd.read_csv(osp.join('../source/fixtures', 'povcal_country_year.csv'))
177/8: pd.read_csv(osp.join('../source/fixtures', 'west_and_rest.csv'))
177/9: c = pd.read_csv(osp.join('../source/fixtures', 'povcal_country_year.csv'))
177/10: c['country'].unique()
177/11: len(c['country'].unique())
177/12: w = pd.read_csv(osp.join('../source/fixtures', 'west_and_rest.csv'))
177/13: w.shape
178/1: import frictionless
178/2: p = frictionless.Package('datapackage.json')
178/3:
for resource in p.resources:
    df = resource.to_pandas()
179/1: import pathlib
179/2: pathlib.Path('income_mountain\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/3: p = pathlib.Path('income_mountain\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/4: p.exists
179/5: p.exists()
179/6: p = pathlib.Path('./income_mountain\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/7: p.exists()
179/8: p.exists?
179/9: p
179/10: pathlib.PurePath('income_mountain\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/11: a = pathlib.PurePath('income_mountain\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/12: a.exists()
179/13: p2 = pathlib.Path(a)
179/14: p2
179/15: p2.exists()
179/16: a = pathlib.PurePath('income_mountain\\\\ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/17: p2 = pathlib.Path(a)
179/18: p2.exists()
179/19: p2
179/20: a = pathlib.PurePath('income_mountain/ddf--datapoints--income_mountain_50bracket_shape_for_log--by--country--year.csv')
179/21: p2 = pathlib.Path(a)
179/22: p2.exists()
180/1: import pandas as pd
180/2: df = pd.read_csv('./owid-covid-data.csv')
180/3: df.head()
180/4: import locale
180/5: locale.getpreferredencoding(False)
187/1: import sys
187/2: ps = [getattr(sys, 'ps%s' % i, '') for i in range(1,4)]
187/3: ps_json = '\n["%s", "%s", "%s"]\n' % tuple(ps)
187/4: print (ps_json)
187/5: sys.exit(0)
186/1: exec("def __PYTHON_EL_eval(source, filename):\n    import ast, sys\n    if sys.version_info[0] == 2:\n        from __builtin__ import compile, eval, globals\n    else:\n        from builtins import compile, eval, globals\n    try:\n        p, e = ast.parse(source, filename), None\n    except SyntaxError:\n        t, v, tb = sys.exc_info()\n        sys.excepthook(t, v, tb.tb_next)\n        return\n    if p.body and isinstance(p.body[-1], ast.Expr):\n        e = p.body.pop()\n    try:\n        g = globals()\n        exec(compile(p, filename, 'exec'), g, g)\n        if e:\n            return eval(compile(ast.Expression(e.value), filename, 'eval'), g, g)\n    except Exception:\n        t, v, tb = sys.exc_info()\n        sys.excepthook(t, v, tb.tb_next)")
186/2: exec("def __PYTHON_EL_eval_file(filename, tempname, delete):\n    import codecs, os, re\n    pattern = r'^[ \t\f]*#.*?coding[:=][ \t]*([-_.a-zA-Z0-9]+)'\n    with codecs.open(tempname or filename, encoding='latin-1') as file:\n        match = re.match(pattern, file.readline())\n        match = match or re.match(pattern, file.readline())\n        encoding = match.group(1) if match else 'utf-8'\n    with codecs.open(tempname or filename, encoding=encoding) as file:\n        source = file.read().encode(encoding)\n    if delete and tempname:\n        os.remove(tempname)\n    return __PYTHON_EL_eval(source, filename)")
186/3: __PYTHON_EL_eval_file("/tmp/pyLTLYO0", "/tmp/pyLTLYO0", True)
186/4: import base64
186/5: a = base64.decode("UmVhbEJlbg==")
186/6: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"base64.de\")))", "<string>")
186/7: a = base64.decodebytes("UmVhbEJlbg==")
186/8: a = base64.decodebytes(r"UmVhbEJlbg==")
186/9: from io import BytesIO
186/10: b = BytesIO("UmVhbEJlbg==")
186/11: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"BytesIO.\")))", "<string>")
186/12: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"BytesIO\")))", "<string>")
186/13: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"BytesIO.\")))", "<string>")
186/14: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"base64.b64\")))", "<string>")
186/15: a = base64.b64decode("UmVhbEJlbg==")
186/16: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"a\")))", "<string>")
186/17: a
186/18: __PYTHON_EL_eval("\ndef __PYTHON_EL_get_completions(text):\n    completions = []\n    completer = None\n\n    try:\n        import readline\n\n        try:\n            import __builtin__\n        except ImportError:\n            # Python 3\n            import builtins as __builtin__\n        builtins = dir(__builtin__)\n\n        is_ipython = ('__IPYTHON__' in builtins or\n                      '__IPYTHON__active' in builtins)\n        splits = text.split()\n        is_module = splits and splits[0] in ('from', 'import')\n\n        if is_ipython and is_module:\n            from IPython.core.completerlib import module_completion\n            completions = module_completion(text.strip())\n        elif is_ipython and '__IP' in builtins:\n            completions = __IP.complete(text)\n        elif is_ipython and 'get_ipython' in builtins:\n            completions = get_ipython().Completer.all_completions(text)\n        else:\n            # Try to reuse current completer.\n            completer = readline.get_completer()\n            if not completer:\n                # importing rlcompleter sets the completer, use it as a\n                # last resort to avoid breaking customizations.\n                import rlcompleter\n                completer = readline.get_completer()\n            if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n                completer.print_mode = False\n            i = 0\n            while True:\n                completion = completer(text, i)\n                if not completion:\n                    break\n                i += 1\n                completions.append(completion)\n    except:\n        pass\n    finally:\n        if getattr(completer, 'PYTHON_EL_WRAPPED', False):\n            completer.print_mode = True\n    return completions\nprint(';'.join(__PYTHON_EL_get_completions(\"\\\"gb\")))", "<string>")
186/19: a.decode("gb18030")
186/20: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"args\"))", "/home/semio/bin/parse-mail-header")
186/21: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"args\"))", "/home/semio/bin/parse-mail-header")
186/22: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"args\"))", "/home/semio/bin/parse-mail-header")
186/23: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"automatic_ws_datasets\"))", "/home/semio/src/work/gapminder/libs/gapminder-airflow-dags/dags/update_all_datasets.py")
186/24: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"notify_ws_task\"))", "/home/semio/src/work/gapminder/libs/gapminder-airflow-dags/templates/etl_recipe_auto_ws.py")
186/25: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"cleanup_task\"))", "/home/semio/src/work/gapminder/libs/gapminder-airflow-dags/templates/etl_recipe_auto_ws.py")
186/26: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"text\"))", "/home/semio/src/work/gapminder/libs/gapminder-airflow-dags/plugins/ddf_operators.py")
186/27: __PYTHON_EL_eval("\ndef __FFAP_get_module_path(objstr):\n    try:\n        import inspect\n        import os.path\n        # NameError exceptions are delayed until this point.\n        obj = eval(objstr)\n        module = inspect.getmodule(obj)\n        filename = module.__file__\n        ext = os.path.splitext(filename)[1]\n        if ext in ('.pyc', '.pyo'):\n            # Point to the source file.\n            filename = filename[:-1]\n        if os.path.exists(filename):\n            return filename\n        return ''\n    except:\n        return ''\nprint(__FFAP_get_module_path(\"env.get_template\"))", "/home/semio/src/work/gapminder/libs/gapminder-airflow-dags/dags/refresh_dags.py")
188/1: import pandas as pd
188/2: df = pd.read_csv('ddf--datapoints--poverty_population--by--country--year--wb_level.csv')
188/3: df.head()
188/4: df2 = df.groupby(['wb_level', 'year'])['poverty_population'].sum()
188/5: df2
188/6: df2 = df2.reset_index()
188/7: df2
188/8: df2.to_csv('ddf--datapoints--poverty_population--by--wb_level--year.csv', index=False)
188/9: df = pd.read_csv('ddf--datapoints--poverty_population--by--country--year--gm_level.csv')
188/10: df2 = df.groupby(['gm_level', 'year'])['poverty_population'].sum()
188/11: df2 = df2.reset_index()
188/12: df2.to_csv('ddf--datapoints--poverty_population--by--gm_level--year.csv', index=False)
190/1: import pandas as pd
190/2:
ilevel4 = pd.read_csv('../source/fixtures/ddf--datapoints--ilevels4--by--country--time.csv')
ilevel4_wb = pd.read_csv('../source/fixtures/ddf--datapoints--ilevels4_wb--by--country--time.csv')
ilevel4_latest = pd.read_csv('../source/fixtures/wb_groups.csv')
190/3:
m1 = { "Level 1": 'lic', 'Level 2': 'lmic', 'Level 3': 'umic', 'Level 4': 'hic'}
m2 = {'Low income': 'lic', 'Lower middle income': 'lmic', 'Upper middle income': 'umic', 'High income': 'hic'}
190/4: ilevel4_latest
190/5:
ilevel4_latest.columns = ['country', 'time', 'gm', 'wb']
ilevel4_latest = ilevel4_latest.set_index(['country', 'time'])['wb']
190/6: ilevel4_latest
190/7:
ilevel4_latest.columns = ['country', 'name', 'time', 'gm', 'wb']
ilevel4_latest = ilevel4_latest.set_index(['country', 'time'])['wb']
190/8: ilevel4_latest
190/9: ilevel4_latest = ilevel4_latest.map(m2)
190/10: ilevel4_latest
190/11: ilevel4_latest.hasnans
190/12: ilevel4_latest = ilevel4_latest.reset_index()
190/13: df = pd.read_csv('../../poverty_rates/ddf--datapoints--poverty_population--by--country--time--wb_level.csv')
190/14: df.head()
190/15: df1 = df[df.wb_level == 'wb_lvl1']
190/16: df1 = df1[['country', 'time', 'poverty_population']].set_index(['country', 'time'])
190/17: df1
190/18: grps = ilevel4_latest.copy()
190/19: grps
190/20: grps = ilevel4_latest.set_index(['country', 'time']).copy()
190/21: grps
190/22: df1['grp'] = grps['wb']
190/23: df1['grp'].hasnans
190/24: df1 = df1.reset_index()
190/25: df1
190/26: res = df1.groupby(['grp', 'time'])['poverty_population'].sum()
190/27: res
190/28: res = res.reset_index()
190/29: gs = res['grp'].unique()
190/30: gs
190/31: gt = pd.CategoricalDtype(['lic', 'lmic', 'umic', 'hic'], ordered=True)
190/32: res['grp'] = res['grp'].astype(gt)
190/33: res
190/34: res = res.sort_values(by=['grp', 'time'])
190/35: res.columns = ['income_groups', 'time', 'poverty_population_under_1_9_per_day']
190/36: res
190/37: m = {'lic': 'low_income', 'hic': 'high_income', 'lmic': 'lower_middle_income', 'umic': 'upper_middle_income'}
190/38: res['income_groups'] = res['income_groups'].map(m)
190/39: res
190/40: res.dtypes['income_groups']
190/41: res.to_csv('../../poverty_rates/ddf--datapoints--poverty_population_under_1_9_per_day--by--income_groups--time.csv', index=False)
190/42:
ilevel4['ilevels4'] = ilevel4['ilevels4'].map(m1)
ilevel4_wb['ilevels4_wb'] = ilevel4_wb['ilevels4_wb'].map(m2)
190/43: ilevel4_latest
190/44: ilevel4
190/45: ilevel4_wb
190/46:
gm = ilevel4.set_index(['country', 'time'])
wb = ilevel4_wb.set_index(['country', 'time'])
190/47: wb.columns = ['ilevels4']
190/48: gm.update(wb)
190/49: gm
190/50: gm.to_csv('tmp_gm_2.csv')
190/51: grps.to_csv('hisham.csv')
191/1: import pandas as pd
191/2: import requests as req
191/3: url = "https://www.forbes.com/ajax/list/data?year=1997&uri=billionaires&type=person"
191/4: res = req.get(url)
191/5: data = res.json()
191/6: data
191/7: data[0]
   1: import pandas as pd
   2: df = pd.read_stata('../source/Billionaires1996-2014.dta')
   3: df.head()
   4: df.columns
   5: df['name'].unique()
   6: df[df.name == "Bill Gates"]
   7: df[df.name == "Bill Gates"][['year', 'age']]
   8: df['countrycode'].unique()
   9: df1 = pd.read_csv('../source/1997.csv')
  10: df2 = pd.read_csv('../source/2021.csv')
  11: df1.head()
  12: df1[df1.name == "Warren Buffett"]
  13: df1[df1.name == "Warren Buffett"]['worth']
  14: df2[df2.name == "Warren Buffett"]['worth']
  15: df2[df2.name == "Warren Buffett"]
  16:
for year in range(1997, 2022):
    fn = f"../source/{year}.csv"
    df = pd.read_csv(fn)
    res.append(df)
  17: res = []
  18:
for year in range(1997, 2022):
    fn = f"../source/{year}.csv"
    df = pd.read_csv(fn)
    res.append(df)
  19: res = []
  20:
for year in range(1997, 2022):
    fn = f"../source/{year}.csv"
    df = pd.read_csv(fn)
    df['year'] = year
    res.append(df)
  21: res = pd.concat(res)
  22: res.head()
  23: res[res.name == "Warren Buffett"]
  24: res[res.name == "Warren Buffett"]['worth']
  25: res[res.name == "Warren Buffett"][['year', 'worth']]
  26: res[pd.isnull(res['worth'])]
  27: res[pd.isnull(res['worth'])][['name', 'year']]
  28: res.shape
  29: res[~pd.isnull(res['worth'])][['name', 'year']]
  30: res[~pd.isnull(res['worth'])][['name', 'year']].sort_values(by='name')
  31: res[pd.isnull(res['worth'])][['name', 'year']].sort_values(by='name')
  32: res['uri'].unique()
  33: res['uri'].unique().shape
  34: res_worth = res[~pd.isnull(res['worth'])][['uri', 'year', 'worth']].copy()
  35: res_worth
  36: from ddf_utils.str import to_concept_id
  37: res_worth.columns = ['person', 'year', 'worth']
  38: res_worth.person = res_worth.person.map(to_concept_id)
  39: res_worth.to_csv('../../ddf--worth--by--person--year.csv', index=False)
  40: res
  41: res.columns
  42:
ent = res[['name', 'lastName', 'age', 'country', 'gender', 'uri', 'source',
       'industry', 'headquarters', 'state',
                     'year', 'title', 'government', 'salary',
                            'managementAssets', 'pay']].copy()
  43: ent
  44: ent = ent.sort_values(by=['uri', 'year'])
  45: ent
  46: ent2 = ent.groupby('uri').last()
  47: ent2
  48: ent2['person'] = ent2.index.map(to_concept_id)
  49: ent2 = ent2.reset_index().set_index('person')
  50: ent2
  51: ent2.to_csv('../../ddf--entities--person.csv')
  52: ?%history
  53: %history -g -f etllog.py
