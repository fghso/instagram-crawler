# -*- coding: iso-8859-1 -*-

import sys
import os
import threading
import httplib2
import time
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
        self.shutdownEvent = threading.Event()
        self.insertWaitCondition = threading.Condition()
        
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
        self.resourcesQueue = Queue.Queue()
        t = threading.Thread(target = self._insertThread)
        t.daemon = True
        t.start()
        
    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        if ("batchsize" not in self.config): raise KeyError("Parameter 'batchsize' must be specified.")
        else: self.config["batchsize"] = int(self.config["batchsize"])
        if (self.config["batchsize"] < 1): raise ValueError("Parameter 'batchsize' must be greater than zero.")
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
    
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): 
        if self.insertThreadExceptionEvent.is_set(): raise RuntimeError("Exception in batch insert thread. Execution of MySQLBatchInsertFilter aborted.") 
        extraResources = extraInfo["original"][self.name]
        if not extraResources: return
        for resource in extraResources: self.resourcesQueue.put(resource)
        with self.insertWaitCondition: self.insertWaitCondition.notify()
        
    def _insertQuery(self):
        if self.resourcesQueue.empty(): return
        query = "INSERT INTO " + self.config["table"] + " (" + ", ".join(self.colNames) + ") VALUES "
        
        data = []
        values = []
        for i in range(self.config["batchsize"]): 
            try: resource = self.resourcesQueue.get_nowait()
            except Queue.Empty: break
            resourceValues = []
            for column in self.colNames:
                if (column in resource): 
                    resourceValues.append("%s")
                    data.append(resource[column])
                else: resourceValues.append("DEFAULT")
            values.append("(" + ", ".join(resourceValues) + ")") 
            
        query += ", ".join(values)
        if (self.config["onduplicateupdate"]):
            query += " ON DUPLICATE KEY UPDATE " + ", ".join(["{0} = VALUES({0})".format(column) for column in self.colNames])
        
        connection = mysql.connector.connect(**self.config["connargs"])
        cursor = connection.cursor()
        cursor.execute(query, data)
        connection.commit()
        cursor.close()
        connection.close()
        
    def _insertThread(self):
        try: 
            with self.insertWaitCondition:
                while not self.shutdownEvent.is_set():
                    if (self.resourcesQueue.qsize() >= self.config["batchsize"]): self._insertQuery()
                    else: self.insertWaitCondition.wait()
                self.insertWaitCondition.notify()
        except: 
            self.insertThreadExceptionEvent.set()
            self.echo.out("Exception while inserting a batch of resources.", "EXCEPTION")

    def shutdown(self):
        with self.insertWaitCondition:
            self.shutdownEvent.set()
            self.insertWaitCondition.notify()
            self.insertWaitCondition.wait()
        self._insertQuery()
        
    
class FppAppFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.applicationsQueue = Queue.Queue()
        self.local = threading.local()
        self._loadAppFile()

    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        self.config["maxapprequests"] = int(self.config["maxapprequests"])
        if (self.config["maxapprequests"] < 0): raise ValueError("Parameter 'maxapprequests' must be greater than or equal to zero. Zero means no boundary on the number of requests.")
        
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
            if (self.config["maxapprequests"] < 2): applicationsList = list(reader)
            else: applicationsList = list(reader) * self.config["maxapprequests"]
        else: raise TypeError("Unknown file type '%s'." % self.selectConfig["filename"])
        
        # Put applications on the queue
        for app in applicationsList:
            server = app["apiserver"].lower()
            if (server == "us"): app["apiserver"] = "http://api.us.faceplusplus.com/"
            elif (server == "cn"): app["apiserver"] = "http://api.cn.faceplusplus.com/"
            self.applicationsQueue.put(app)
        
        # Close file
        appsFile.close()
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        self.local.application = self.applicationsQueue.get()
        if (self.config["maxapprequests"] == 0): self.applicationsQueue.put(self.local.application)
        return {"application": self.local.application}
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["maxapprequests"] > 0): self.applicationsQueue.put(self.local.application)
        
        
class InstagramAppFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.httpObj = httplib2.Http(disable_ssl_certificate_validation = True)
        self.echo = common.EchoHandler()
        self.appRates = {}
        self.zeroAppRates = []
        self.local = threading.local()
        self._loadAppFile()
        
    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        #self.config["dynamicallyload"] = common.str2bool(self.config["dynamicallyload"])
        self.config["resetpercent"] = float(self.config["resetpercent"]) / 100
        self.config["sleeptime"] = int(self.config["sleeptime"])
        if (self.config["sleeptime"] < 1): raise ValueError("Parameter 'sleeptime' must be greater than 1 second.")
        
    def setup(self): self.local.lastAppName = None
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        #if (self.config["dynamicallyload"]): self._loadAppFile()
        
        if (self.config["method"] == "random"): 
            appIndex = random.randint(0, len(self.applicationList) - 1)
        else: 
            maxRate = 0
            appIndex = None
            if (self.config["method"] == "maxpost"):
                percentZeroAppRates = float(len(self.zeroAppRates)) / len(self.appRates)
                if (percentZeroAppRates > self.config["resetpercent"]):
                    self.echo.out(u"Maximum number of applications with ratelimit exceeded hit, making requests now to check actual ratelimits.", "WARNING")
                    for appName in self.zeroAppRates[:]:
                        appIndex = self.appIndexes[appName]
                        self.appRates[appName] = self._getAppRate(self.applicationList[appIndex])
                        self.zeroAppRates.remove(appName)
                for appName, rateRemaining in self.appRates.iteritems():
                    if (rateRemaining > maxRate) and (appName != self.local.lastAppName):
                        maxRate = rateRemaining
                        appIndex = self.appIndexes[appName]
                self.local.lastAppName = self.applicationList[appIndex]["name"]
            else: 
                while (True):
                    for i, application in enumerate(self.applicationList):
                        rateRemaining = self._getAppRate(application)
                        if (rateRemaining > maxRate): 
                            maxRate = rateRemaining
                            appIndex = i
                    if (maxRate == 0): 
                        self.echo.out(u"Ratelimit of all applications exceeded, sleeping now for %d seconds..." % self.config["sleeptime"], "WARNING")
                        time.sleep(self.config["sleeptime"])
                    else: break
                    
        return {"application": self.applicationList[appIndex]}
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        appName = extraInfo["original"][self.name]["appname"]
        appRate = extraInfo["original"][self.name]["apprate"]
        if (appRate == 0): self.zeroAppRates.append(appName)
        elif (appRate is not None): self.appRates[appName] = appRate
        
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
            self.applicationList = appDict["instagram"]["application"] if isinstance(appDict["instagram"]["application"], list) else [appDict["instagram"]["application"]]
        elif (fileType == "json"):
            self.applicationList = json.load(appsFile)["application"]
        elif (fileType == "csv"):
            reader = csv.DictReader(appsFile, quoting = csv.QUOTE_NONE)
            self.applicationList = list(reader)
        else: raise TypeError("Unknown file type '%s'." % self.selectConfig["filename"])
        
        # Build indexes and rates lists
        self.appIndexes = {}
        for i, application in enumerate(self.applicationList):
            self.appIndexes[application["name"]] = i
            if (self.config["method"] == "maxpost") and (application["name"] not in self.appRates): 
                self.appRates[application["name"]] = self._getAppRate(application)
        
        # Close file
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
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            appIndex = random.randint(0, len(self.applicationList) - 1)
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["name"], header["x-ratelimit-remaining"]
            
    def _spendSpecificRate(self, appName, repeat):
        appIndex = self.appIndexes[appName]
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["name"], header["x-ratelimit-remaining"]#, content 

            
# ====== Tests ===== 
if __name__ == "__main__":
    #print InstagramAppFilter()._spendRandomRate(50)
    #print InstagramAppFilter()._spendSpecificRate("CampsApp1", 1)
    print InstagramAppFilter({"name": None, 
                              "appsfile": "apps.csv", 
                              #"dynamicallyload": "False",
                              "method": "maxpost", # random, maxpost or maxpre
                              "resetpercent": 50,
                              "sleeptime": 60}).apply(None, None, None)
                              