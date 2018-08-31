import pandas
import itertools
import datetime


# Data creation.
columns = ['bool_filter', 'start_date', 'end_date', 'outer_grouper',
        'inner_grouper', 'unaggregated']

base_date = datetime.datetime.now().date()
start_range, end_range = range(1, 11), range (11, 20)
dates = [(base_date.replace(day=x), base_date.replace(day=y)) for x, y in
        zip(start_range, end_range)]
[(base_date.replace(day=x), base_date.replace(day=y)) for x, y in zip(start_range, end_range)]
starts = [base_date.replace(day=x) for x in start_range]
ends = [base_date.replace(day=y) for y in end_range]
base_date.replace(day=19)
outer_grouper_vals, inner_grouper_vals = ('OUTER', 'outer'), ('INNER', 'inner')
bool_values = unaggregated = (True, False)
rows = itertools.product(bool_values, starts, ends, outer_grouper_vals, inner_grouper_vals, unaggregated)
df = pandas.DataFrame(list(rows), columns=columns)
len(df)
# get_ipython().magic(u'pinfo df.filter')
filtered_df = df[df['bool_filter'] == True]
len(filtered_df)
grouped_df = filtered_df.groupby(['start_date', 'end_date', 'outer_grouper', 'inner_grouper'])
# print(filtered_df.groupby(['start_date', 'end_date', 'outer_grouper', 'inner_grouper']).get_group('OUTER'))
print(grouped_df.groups)
# print(grouped_df.get_group('OUTER'))
