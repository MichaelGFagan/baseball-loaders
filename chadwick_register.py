import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'bigquery_credentials.json'
)
project_id = 'baseball-source'
table_id = 'chadwick.register'
url_base = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data/'
files = [
    'people-0.csv',
    'people-1.csv',
    'people-2.csv',
    'people-3.csv',
    'people-4.csv',
    'people-5.csv',
    'people-6.csv',
    'people-7.csv',
    'people-8.csv',
    'people-9.csv',
    'people-a.csv',
    'people-b.csv',
    'people-c.csv',
    'people-d.csv',
    'people-e.csv',
    'people-f.csv',

]


def convert_to_string(column):
    if not column:
        return ''
    try:
        return str(column)
    except:
        return ''

print('Downloading Chadwick files')
dataframes = []
for file in files:
    print('Downloading Chadick file ' + file)
    url = url_base + file
    df = pd.read_csv(url,
                     dtype={'key_sr_nfl': 'str',
                            'key_sr_nba': 'str',
                            'key_sr_nhl': 'str',})
    dataframes.append(df)

print('Collating data')
df = pd.concat(dataframes)

print('Loading Chadwick register')
pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')