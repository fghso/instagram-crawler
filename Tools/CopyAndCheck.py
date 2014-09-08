#!/usr/bin/python
# -*- coding: iso-8859-1 -*-


import os
import shutil
import mysql.connector
import app


def CopyAndCheck(source, dest, cursor):
    feedsDest = os.path.join(os.path.abspath(dest), "Feeds")
    usersDest = os.path.join(os.path.abspath(dest), "Users")
    
    for rootpath, dirs, files in os.walk(source):
        if len(files) > 0:
            userID = os.path.split(rootpath)[1]
            query = ("SELECT COUNT(*) FROM usersToCollect WHERE pinterestID = %s AND statusColeta = 2")
            data = (userID,)
            cursor.execute(query, data)
            response = cursor.fetchone()
            
            if (response[0] == 0):
                print("Usuario %s nao coletado")
            else:
                fullFeedDestPath = os.path.join(feedsDest, userID)
                fullUsersPath = os.path.join(usersDest, userID + ".json")
                os.mkdir(fullFeedDestPath)
                
                for filename in files:
                    fullSourcePath = os.path.join(rootpath, filename)
                    if filename.startswith("feed"):
                        shutil.copy(fullSourcePath, fullFeedDestPath)
                    else:
                        shutil.copyfile(fullSourcePath, fullUsersPath)
    
    return
    

mysqlConnection = mysql.connector.connect(user=app.dbUser, password=app.dbPassword, host=app.dbHost, database=app.dbName)
cursor = mysqlConnection.cursor()

CopyAndCheck("FeedCrawler/data", "CrawledData", cursor)

cursor.close()
mysqlConnection.close()
