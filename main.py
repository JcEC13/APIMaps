from fastapi import FastAPI
import random
import pandas as pd

app=FastAPI()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower

spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

df_metadata = spark.read.parquet("./datasets/metada-streamlit.parquet")
dfuser = spark.read.parquet("./datasets/users.parquet")
dfbar = spark.read.parquet("./datasets/bar_part.snappy.parquet")
dfrestaurant = spark.read.parquet("./datasets/restaurant_part.snappy.parquet")
dfcafe = spark.read.parquet("./datasets/cafe_part.snappy.parquet")

@app.get('/')
def index():
    return {'message':'Bienvenido a mi Api'}

@app.get('/process/{name}/{category}/{service}/{lat}/{lon}')
def process(name,category,service,lat,lon):
    idclient=idclient_generate(name,category)
    data=prediction(category,idclient,service)
    df=pd.DataFrame(data,columns=['Name store','Rating','Address','Delivery','lat','lon'])
    return {'store':df['Name store'],'rating':df['Rating'],'address':df['Address'],'lat':df['lat'],'lon':df['lon']}

def read_dataset(option):        
    if option=='Bar':
        df = dfbar
    if option=='Restaurant':
        df= dfrestaurant
    if option=='Cafe':
        df = dfcafe
    return df      

def prediction(option,idclient,service):
    df = read_dataset(option)
    df = df.filter((col("id_name") ==idclient))
    joined_df = df.join(df_metadata,df.id_name_empresa==df_metadata.id_name_empresa)
    joined_df=joined_df.select('name','avg_rating','address','Delivery','latitude', 'longitude')
    if service=='Delivery':
        joined_df=joined_df.filter((col("Delivery") =='si'))
    if service=='In store':
        joined_df=joined_df.filter((col("Delivery") =='no'))
    df2= joined_df.head(5)
    return df2

def random_client(dfread):
    idrnd=dfread.select('id_name').drop_duplicates()
    idrnd=idrnd.head(10)
    return idrnd[random.randint(0, len(idrnd))][0]

def idclient_generate(name,option):
    name=name.lower()
    df= dfuser.filter(lower(dfuser['name']).like(f"%{name}%"))
    id=df.head(1)
    dataset=read_dataset(option)
    if id==[]:
        idclient=random_client(dataset)
    else:
        idclient=str(id[0][0])
    
    df_fil=dataset.filter(dataset['id_name']==idclient)

    if df_fil.isEmpty():
        idclient=random_client(dataset)

    return idclient