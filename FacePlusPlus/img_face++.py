from fpp.facepp import API
import fpp.apikey
import os
import mysql.connector
import csv
import json
import app

mysqlConnection = mysql.connector.connect(user=app.dbUser, 
        password=app.dbPassword, database=app.dbName,
        host=app.dbHost)
cursor = mysqlConnection.cursor()
cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

data_path = 'data/'
img_urls = 'geo_selfies.csv'

if not os.path.exists(data_path): os.makedirs(data_path)

api = API(key=fpp.apikey.API_KEY,
        secret=fpp.apikey.API_SECRET,
        srv=fpp.apikey.SERVER)

# atrs = ['gender', 'age', 'race', 'smiling', 'glass', 'pose']
error = open('erros.log', 'w')
with open(img_urls, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    for i, selfie in enumerate(csv_reader):
        try:
            mid,tag,lat,lon = selfie
            query = 'select std_res_url from images where medias_pk_ref = %s'%mid
            cursor.execute(query)
            img_url = cursor.fetchone()[0]
            print i, img_url
            ans = api.detection.detect(url=img_url)
            #ans = api.detection.detect(url=img_url,attribute=atrs)

            json.dump(ans,open('data/%s.json'%mid, 'w'))
        except Exception, e:
            print e
            error.write('%d,%s,%s'%(i,img_url,str(e)))
error.close()
