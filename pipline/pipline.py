import sys 
import pandas as pd



print('argument', sys.argv)
mouth = int(sys.argv[1])


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['month'] = mouth
print(df.head())

df.to_parquet(f'output_month_{mouth}.parquet')
# df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

print(f'hello pipeline, month={mouth}')