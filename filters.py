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
import common
import persistence


# The filters are sequentially applied in the same order in wich they were specified in the configuration file, 
# unless they were explicitly set as parallel. Each filter receives everything in its corresponding filter 
# section of the XML configuration file as the parameter configurationsDictionary
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("name" in self.config): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def setup(self): pass # Called when a connection to a client is opened
 
    # Apply must return a dictionary containing the desired filter information to be sent to clients. 
    # The parameter extraInfo is a reference to a dictionary and can be used to pass information among 
    # sequential filters. It is not send to clients and its value will always be None if the filter is 
    # executed in parallel
    def apply(self, resourceID, resourceInfo, extraInfo): return {}
        
    # Callback is called when a client is done in crawling its designated resource. Sequential filters
    # receive the parameters resourceInfo, newResources and extraInfo as references, so they can alter 
    # the values of these parameters. The server will store the final values of resourceInfo and newResources
    # as they are after all filters were called back. Parallel filters receive just a copy of the values 
    # of these three parameters as they came from crawler. As in apply method, extraInfo can be used to pass 
    # information among sequential filters (in the case of sequential filters, the original information received 
    # from crawler is stored in extraInfo["original"], so it is available at any time). 
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): pass
        
    def finish(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
    
    
class SaveResourcesFilter(BaseFilter): 
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        PersistenceHandlerClass = getattr(persistence, self.config["handler"]["class"])
        self.persist = PersistenceHandlerClass(self.config["handler"])
        
    def setup(self): self.persist.setup()
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["parallel"]): newResources = extraInfo[self.name]
        else: newResources = extraInfo["original"][self.name]
        self.persist.insert(newResources)
        
    def finish(self): self.persist.finish()
    def shutdown(self): self.persist.shutdown()
    
    
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
        self.config["dynamicallyload"] = common.str2bool(self.config["dynamicallyload"])
        self.config["resetpercent"] = float(self.config["resetpercent"]) / 100
        self.config["sleeptimedelta"] = int(self.config["sleeptimedelta"])
        if (self.config["sleeptimedelta"] < 1): raise ValueError("Parameter sleeptimedelta must be greater than 1 second.")
        
    def setup(self): self.local.lastAppName = None
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        if (self.config["dynamicallyload"]): self._loadAppFile()
        
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
                        self.echo.out(u"Ratelimit of all applications exceeded, sleeping now for %d seconds..." % self.config["sleeptimedelta"], "WARNING")
                        time.sleep(self.config["sleeptimedelta"])
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
                
    # Funções apenas para testes
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

            
# ====== Testes ===== 
if __name__ == "__main__":
    #print InstagramAppFilter()._spendRandomRate(50)
    #print InstagramAppFilter()._spendSpecificRate("CampsApp1", 1)
    print InstagramAppFilter({"name": None, 
                              "appsfile": "apps.csv", 
                              "dynamicallyload": "False",
                              "method": "maxpost",
                              "resetpercent": 50,
                              "sleeptimedelta": 60}).apply(None, None, None)
                              