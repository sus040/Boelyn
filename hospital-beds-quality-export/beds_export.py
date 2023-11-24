import pandas as pd
import psycopg
from credentials import DBNAME, USER, PASSWORD

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com",
    dbname=DBNAME, user=USER, password=PASSWORD
)

query = "SELECT * FROM beds;"

df = pd.read_sql(query, conn)

conn.close()

df.to_csv('~/Desktop/beds.csv', index=False)
