import yfinance as yf
import json
from sqlalchemy import create_engine
import psycopg2

engine = create_engine("postgresql://myuser:mypassword@localhost:5432/stock_db")

def fetch_data():
    df = yf.download('JIOFIN.NS',period='1mo',interval='1d',multi_level_index=False)
    df = df[['Open','High','Low','Close','Volume']]
    df.reset_index(inplace=True)
    print("Fetch Data Successfully")
    return df

def publish_to_postgres():
    df = fetch_data()
    df.to_sql(name='demo',
                    con=engine,
                    if_exists='append',
                    index=False)
    print("Pushed to postgres")
    
publish_to_postgres()