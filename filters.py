# -*- coding: iso-8859-1 -*-

import sys
import os
import threading
import httplib2
import json
import csv
import xmltodict
import random
import Queue
import common
import persistence
import mysql.connector


class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
        if ("name" in self.config): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def setup(self): pass
    def apply(self, resourceID, resourceInfo, extraInfo): return {}
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): pass
    def finish(self): pass
    def shutdown(self): pass
      
    
class SaveResourcesFilter(BaseFilter): 
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        PersistenceHandlerClass = getattr(persistence, self.config["persistence"]["class"])
        self.persist = PersistenceHandlerClass(self.config["persistence"])
        
    def setup(self): self.persist.setup()
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["parallel"]): extraResources = extraInfo[self.name]
        else: extraResources = extraInfo["original"][self.name]
        self.persist.insert(extraResources)
        
    def finish(self): self.persist.finish()
    def shutdown(self): self.persist.shutdown()
    
    
class ResourceInfoFilter(BaseFilter):
    def apply(self, resourceID, resourceInfo, extraInfo): return resourceInfo
    

class MySQLBatchInsertFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.echo = common.EchoHandler()
        self.insertThreadExceptionEvent = threading.Event()
        
        # Get column names
        query = "SELECT * FROM " + self.config["table"] + " LIMIT 0"
        connection = mysql.connector.connect(**self.config["connargs"])
        cursor = connection.cursor()
        cursor.execute(query)
        cursor.fetchall()
        self.colNames = cursor.column_names
        cursor.close()
        connection.close()
        
        # Start insert thread
        self.batchQueue = Queue.Queue()
        self.batchList = []
        t = threading.Thread(target = self._insertThread)
        t.daemon = True
        t.start()
        
    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        if ("trigger" not in self.config): raise KeyError("Parameter 'trigger' must be specified.")
        else: self.config["trigger"] = int(self.config["trigger"])
        if (self.config["trigger"] < 1): raise ValueError("Parameter 'trigger' must be greater than zero.")
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
    
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): 
        if self.insertThreadExceptionEvent.is_set(): raise RuntimeError("Exception in batch insert thread. Execution of MySQLBatchInsertFilter aborted.") 
        batchData = extraInfo["original"][self.name]
        self.batchQueue.put((resourceID, batchData))
        
    def _insertQuery(self):
        self.echo.out("[Table: %s] Inserting batch..." % self.config["table"])
        query = "INSERT INTO " + self.config["table"] + " (" + ", ".join(self.colNames) + ") VALUES "
        
        data = []
        values = []
        batchSize = 0
        for i in range(self.config["trigger"]): 
            if not self.batchList: break
            (resourceID, batchData) = self.batchList.pop()
            batchSize += len(batchData)
            for row in batchData:
                rowValues = []
                for column in self.colNames:
                    if (column in row): 
                        rowValues.append("%s")
                        data.append(row[column])
                    else: rowValues.append("DEFAULT")
                values.append("(" + ", ".join(rowValues) + ")") 
        
        if batchSize:        
            query += ", ".join(values)
            if (self.config["onduplicateupdate"]):
                query += " ON DUPLICATE KEY UPDATE " + ", ".join(["{0} = VALUES({0})".format(column) for column in self.colNames])
        
            connection = mysql.connector.connect(**self.config["connargs"])
            cursor = connection.cursor()
            cursor.execute(query, data)
            connection.commit()
            cursor.close()
            connection.close()
            
        self.echo.out("[Table: %s] %d rows inserted." % (self.config["table"], batchSize))
        
    def _insertThread(self):
        try: 
            while True:
                batchInfo = self.batchQueue.get()
                if not batchInfo: break
                self.batchList.append(batchInfo)
                if (len(self.batchList) >= self.config["trigger"]): self._insertQuery()
        except: 
            self.insertThreadExceptionEvent.set()
            self.echo.out("[Table: %s] Exception while inserting a batch." % self.config["table"], "EXCEPTION")

    def shutdown(self):
        self.batchQueue.put(None)
        while not self.batchQueue.empty(): pass
        if self.batchList: self._insertQuery()
        
    
class FppAppFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.appQueue = Queue.Queue()
        self.local = threading.local()
        self._loadAppFile()

    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        self.config["maxapprequests"] = int(self.config["maxapprequests"])
        
    def _loadAppFile(self):
        # Open file
        if os.path.exists(os.path.join(sys.path[0], self.config["appsfile"])): 
            appsFile = open(os.path.join(sys.path[0], self.config["appsfile"]), "r")
        else: 
            appsFile = open(os.path.join(sys.path[1], self.config["appsfile"]), "r")
            
        # Load content
        fileType = os.path.splitext(self.config["appsfile"])[1][1:].lower()
        if (fileType == "csv"):
            reader = csv.DictReader(appsFile, quoting = csv.QUOTE_NONE)
            if (self.config["maxapprequests"] < 2): self.appList = list(reader)
            else: self.appList = list(reader) * self.config["maxapprequests"]
        else: raise TypeError("Unknown file type '%s'." % self.selectConfig["filename"])
        
        # Get API servers addresses
        for app in self.appList:
            server = app["apiserver"].lower()
            if (server == "us"): app["apiserver"] = "http://api.us.faceplusplus.com/"
            elif (server == "cn"): app["apiserver"] = "http://api.cn.faceplusplus.com/"
            if (self.config["maxapprequests"] > 0): self.appQueue.put(app)
        
        # Close file
        appsFile.close()
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        if (self.config["maxapprequests"] > 0): self.local.application = self.appQueue.get()
        else: self.local.application = self.appList[random.randint(0, len(self.appList) - 1)]
        return {"application": self.local.application}
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["maxapprequests"] > 0): self.appQueue.put(self.local.application)
        
        
class InstagramAppFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.httpObj = httplib2.Http(disable_ssl_certificate_validation = True)
        self.echo = common.EchoHandler()
        self._loadAppFile()
        
        # Start manage app thread
        if (self.config["method"] == "roundrobin"):
            self.manageAppThreadExceptionEvent = threading.Event()
            self.appQueue = Queue.Queue()
            t = threading.Thread(target = self._manageAppThread)
            t.daemon = True
            t.start()
            self.appQueue.get()
            self.appQueue.task_done()
        
    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        if (self.config["method"] != "random") and (self.config["method"] != "roundrobin"):
            raise ValueError("Unknown value '%' for parameter 'method'." % self.config["method"])
        if ("rateuse" in self.config): self.config["rateuse"] = int(self.config["rateuse"])
        else: self.config["rateuse"] = 1
        if (self.config["rateuse"] < 1): raise ValueError("Parameter 'rateuse' must be greater than or equal to 1.")
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        if (self.config["method"] == "random"): 
            appIndex = random.randint(0, len(self.appList) - 1)
            appInfo = self.appList[appIndex]
        elif (self.config["method"] == "roundrobin"): 
            if self.manageAppThreadExceptionEvent.is_set(): raise RuntimeError("Exception in manage app thread. Execution of InstagramAppFilter aborted.")
            appInfo = self.appQueue.get()
            self.appQueue.task_done()
        return {"application": appInfo}
        
    def _manageAppThread(self):
        # try: 
            # currentAppIndex = random.randint(0, len(self.appList) - 1)
            # currentAppRate = -1
            # while True:
                # if (currentAppRate >= self.config["rateuse"]):
                    # currentAppRate -= self.config["rateuse"]
                    # self.appQueue.put(self.appList[currentAppIndex])
                    # self.appQueue.join()
                # else:
                    # currentAppIndex = currentAppIndex + 1 if (currentAppIndex < len(self.appList) - 1) else 0
                    # currentAppRate = self._getAppRate(self.appList[currentAppIndex])
                    # if (currentAppRate > 0): self.echo.out("Using %s (rate remaining: %d)." % (self.appList[currentAppIndex]["name"], currentAppRate))
        try: 
            currentAppIndex = random.randint(0, len(self.appList) - 1)
            while True:
                currentAppIndex = currentAppIndex + 1 if (currentAppIndex < len(self.appList) - 1) else 0
                currentAppRate = self._getAppRate(self.appList[currentAppIndex])
                if (currentAppRate > 0): self.echo.out("Using %s (rate remaining: %d)." % (self.appList[currentAppIndex]["name"], currentAppRate))
                for i in range(0, currentAppRate / self.config["rateuse"]): self.appQueue.put(self.appList[currentAppIndex])
                self.appQueue.join()
        except: 
            self.manageAppThreadExceptionEvent.set()
            self.echo.out("Exception while managing application distribution.", "EXCEPTION")
        
    def _loadAppFile(self):
        # Open file
        if os.path.exists(os.path.join(sys.path[0], self.config["appsfile"])): 
            appsFile = open(os.path.join(sys.path[0], self.config["appsfile"]), "r")
        else: 
            appsFile = open(os.path.join(sys.path[1], self.config["appsfile"]), "r")
            
        # Load content
        fileType = os.path.splitext(self.config["appsfile"])[1][1:].lower()
        if (fileType == "xml"):
            appDict = xmltodict.parse(appsFile.read())
            self.appList = appDict["instagram"]["application"] if isinstance(appDict["instagram"]["application"], list) else [appDict["instagram"]["application"]]
        elif (fileType == "json"):
            self.appList = json.load(appsFile)["application"]
        elif (fileType == "csv"):
            reader = csv.DictReader(appsFile, quoting = csv.QUOTE_NONE)
            self.appList = list(reader)
        else: raise TypeError("Unknown file type '%s'." % self.selectConfig["filename"])
        
        appsFile.close()
    
    def _getAppRate(self, application):
        clientID = application["clientid"]
        rateRemaining = 0
        try: 
            (header, content) = self.httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            rateRemaining = int(header["x-ratelimit-remaining"])
        except:
            if (header['status'] == '503') or (header['status'] == '429'):
                self.echo.out(u"Ratelimit of %s exceeded." % application["name"], "WARNING")
            else:
                message = u"Request to check ratelimit of %s failed.\n" % application["name"]
                message += u"Header: %s\n" % header
                message += u"Content: %s\n" % content
                self.echo.out(message, "ERROR")
        return rateRemaining  
                
    # Methods for tests
    def _spendRandomRate(self, repeat):
        for i in range(1, repeat + 1):
            appIndex = random.randint(0, len(self.appList) - 1)
            clientID = self.appList[appIndex]["clientid"]
            (header, content) = self.httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.appList[appIndex]["name"], header["x-ratelimit-remaining"]
            
    def _spendSpecificRate(self, appIndex, repeat):
        for i in range(1, repeat + 1):
            clientID = self.appList[appIndex]["clientid"]
            (header, content) = self.httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.appList[appIndex]["name"], header["x-ratelimit-remaining"]#, content 

            
# ====== Tests ===== 
if __name__ == "__main__":
    #print InstagramAppFilter()._spendRandomRate(50)
    #print InstagramAppFilter()._spendSpecificRate(0, 1)
    print InstagramAppFilter({"appsfile": "inout\instagramapps.csv", 
                              "method": "roundrobin", # random or roundrobin
                              "rateuse": 1}).apply(None, None, None)
                              