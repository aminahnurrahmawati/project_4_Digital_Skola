import psycopg2
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine
data_property = pd.read_csv('TR_PropertyInfo.csv')
data_user = pd.read_csv('TR_UserInfo.csv')

conn = psycopg2.connect(database="postgres",
                        user='postgres',
                        password='mekanika', 
                        host='localhost',
                        port='5432')

conn.autocommit = True
cursor = conn.cursor()



sql = '''CREATE TABLE Property_Info(Prop_ID int,\
Property_City varchar(20),\
Property_State varchar(30))
;'''

cursor.execute(sql)

sql2 = '''COPY Property_Info(Prop_ID, Property_City,\
Property_State)
FROM 'TR_PropertyInfo.csv'
DELIMITER ','
CSV HEADER;'''


cursor.execute(sql2)

sql3 = '''select * from Property_Info;'''
cursor.execute(sql3)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()