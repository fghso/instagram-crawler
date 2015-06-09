#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import csv
import logging
import xmltodict
import mysql.connector
from mysql.connector.errors import Error
from datetime import datetime


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

# Define setup variables
update = "UPDATE locations INNER JOIN media ON locations.media_pk_ref = media.media_pk SET `country` = %s, `state` = %s, `city` = %s WHERE media.id = %s" 
logging.basicConfig(format=u"%(asctime)s %(name)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", filename="gadm_errors.log", filemode="a")
feedsResolvedFile = "../../data/csvs/feeds_geotagged_resolved.csv"
counter = 0
before = datetime.now()

print before
with open(feedsResolvedFile) as input:
    reader = csv.DictReader(input)
    for row in reader:
        data = (row["country"], row["state"], row["city"], row["mid"])
        try: 
            cursor.execute(update, data)
            counter += 1
            if (counter % 100000 == 0):
                print counter / 100000,  datetime.now() - before,
                commitStart = datetime.now()
                connection.commit()
                print datetime.now() - commitStart
                before = datetime.now()
        except mysql.connector.errors: 
            logging.exception("mid: %s" % row["mid"]) 
            #connection.rollback()
    
connection.commit()
print "Finished."
