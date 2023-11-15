import psycopg
import pandas as pd
import sys

filename = sys.argv[1]
print("!!!!!!!!!!!!!!!!!!!!!!")
print(filename)
batch = pd.read_csv(filename)

conn = psycopg.connect(
    host="pinniped.postgres.database.azure.com", dbname="gkrotkov",
    user="gkrotkov", password="5uqFbI&rRi"
)
