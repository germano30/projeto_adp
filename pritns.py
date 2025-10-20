import pandas as pd
import os

for file in os.listdir('output'):
    print(file)
    df = pd.read_csv(f'output/{file}')
    print(df.columns)