#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import json
import glob
import xmltodict
import mysql.connector


# Load configurations file
configFile = open("config.xml", "r")
configDict = xmltodict.parse(configFile.read())
config = configDict["config"]["client"]["crawler"]

# Open database connection
config["connargs"]["charset"] = "utf8"
config["connargs"]["collation"] = "utf8_unicode_ci"
connection = mysql.connector.connect(**config["connargs"]) 
cursor = connection.cursor()
cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

# # Check charset and collation variables
# print connection.charset, connection.collation
# cursor.execute("SHOW VARIABLES LIKE 'character_set%'")
# print cursor.fetchall()
# cursor.execute("SHOW VARIABLES LIKE 'collation%'")
# print cursor.fetchall()
# exit(0)

# Define setup variables
insert = "INSERT INTO users (`id`, `username`, `full_name`, `profile_picture`, `bio`, `website`, `counts_media`, `counts_follows`, `counts_followed_by`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)" 
usersDir = "../../data/users/"
filesList = glob.glob(usersDir + "valid_users_*")
remainingFiles = len(filesList)

# Do import
for fileName in filesList:
    fileObj = open(fileName, "r")
    input = json.load(fileObj)
    fileObj.close()

    data = []
    remainingUsers = len(input)
    for userInfo in input:
        print "%d/%d" % (remainingFiles, remainingUsers)
        data.append((userInfo["id"], userInfo["username"], userInfo["full_name"], userInfo["profile_picture"], userInfo["bio"], userInfo["website"], userInfo["counts"]["media"], userInfo["counts"]["follows"], userInfo["counts"]["followed_by"]))
        remainingUsers -= 1

    cursor.executemany(insert, data)
    connection.commit()
    remainingFiles -= 1

print "Finished."

