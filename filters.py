# -*- coding: iso-8859-1 -*-

import sys
import os
import httplib2
import time
#import xmltodict
import json
import random
import common
from collections import OrderedDict


# The filters are sequentially applied in the same order in wich they were specified in the configuration file, 
# unless they were explicitly set as parallel. Each filter receives everything in its corresponding filter 
# section of the XML configuration file as the parameter configurationsDictionary
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
    
        if ("name" in self.config): self._name = self.config["name"]
        else: self._name = self.__class__.__name__
    
    def name(self): return self._name
 
    # Apply must return a dictionary containing the desired filter information to be sent to clients. 
    # The parameter extraInfo is a reference to a dictionary and can be used to pass information among 
    # sequential filters. It is not send to clients and its value will always be None if the filter is 
    # executed in parallel
    def apply(self, resourceID, resourceInfo, extraInfo):
        return resourceInfo
        
    # Callback is called when a client is done in crawling its designated resource. Sequential filters
    # receive the parameters resourceInfo, newResources and extraInfo as references, so they can alter 
    # the values of these parameters. The server will store the final values of resourceInfo and newResources
    # as they are after all filters were called back. Parallel filters receive just a copy of the values 
    # of these three parameters. As in apply method, extraInfo can be used to pass information among sequential 
    # filters. The information received from crawler can be accessed in extraInfo["original"]. 
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        pass
        
    def close(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources


class InstagramAppFilter(BaseFilter):
    appRates = {}
    zeroAppRates = []

    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.httpObj = httplib2.Http(disable_ssl_certificate_validation = True)
        self.echo = common.EchoHandler()
        self.lastAppName = None
        self._loadAppFile()
        
    def _extractConfig(self, configurationsDictionary):
        BaseFilter._extractConfig(self, configurationsDictionary)
        
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        self.config["resetpercent"] = float(self.config["resetpercent"]) / 100
        self.config["sleeptimedelta"] = int(self.config["sleeptimedelta"])
        if (self.config["sleeptimedelta"] < 1): raise ValueError("Parameter sleeptimedelta must be greater than 1 second.")
        
    def apply(self, resourceID, resourceInfo, extraInfo):
        if (self.config["dynamicallyload"]): self._loadAppFile()
        
        if (self.config["method"] == "random"): 
            appIndex = random.randint(0, len(self.applicationList) - 1)
        else: 
            maxRate = 0
            appIndex = 0
            while (True):
                if (self.config["method"] == "maxpost"):
                    percentZeroAppRates = float(len(self.zeroAppRates)) / len(self.appRates)
                    if (percentZeroAppRates > self.config["resetpercent"]):
                        self.echo.default(u"Número máximo de aplicações com ratelimit esgotado atingido, fazendo requisições agora para verificar ratelimits atuais.", "WARNING")
                        for appName in self.zeroAppRates[:]:
                            appIndex = self.appIndexes[appName]
                            self.appRates[appName] = self._getAppRate(self.applicationList[appIndex])
                            self.zeroAppRates.remove(appName)
                    for appName, rateRemaining in self.appRates.iteritems():
                        if (rateRemaining > maxRate) and (appName != self.lastAppName):
                            maxRate = rateRemaining
                            appIndex = self.appIndexes[appName]
                    self.lastAppName = self.applicationList[appIndex]["name"]
                else: 
                    for i, application in enumerate(self.applicationList):
                        rateRemaining = self._getAppRate(application)
                        if (rateRemaining > maxRate): 
                            maxRate = rateRemaining
                            appIndex = i
                    if (maxRate == 0): 
                        self.echo.default(u"Ratelimit de todas as aplicações esgotado, dormindo agora por %d segundos..." % self.config["sleeptimedelta"], "WARNING")
                        time.sleep(self.config["sleeptimedelta"])
                    else: break
        return OrderedDict([("application", self.applicationList[appIndex])])
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        appName = extraInfo["original"]["application"]["name"]
        appRate = extraInfo["original"]["application"]["rate"]
        if (appRate == 0): self.zeroAppRates.append(appName)
        self.appRates[appName] = appRate
        
    def _loadAppFile(self):
        if os.path.exists(os.path.join(sys.path[0], self.config["appsfile"])): 
            appsFile = open(os.path.join(sys.path[0], self.config["appsfile"]), "r")
        else: 
            appsFile = open(os.path.join(sys.path[1], self.config["appsfile"]), "r")
        #appDict = xmltodict.parse(appFile.read())
        #self.applicationList = appDict["instagram"]["application"] if isinstance(appDict["instagram"]["application"], list) else [appDict["instagram"]["application"]]
        self.applicationList = json.load(appsFile)["application"]
        self.appIndexes = {}
        for i, application in enumerate(self.applicationList):
            self.appIndexes[application["name"]] = i
            if (self.config["method"] == "maxpost") and (application["name"] not in self.appRates): 
                self.appRates[application["name"]] = self._getAppRate(application)
        appsFile.close()
    
    def _getAppRate(self, application):
        clientID = application["clientid"]
        rateRemaining = 0
        try: 
            (header, content) = self.httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            rateRemaining = int(header["x-ratelimit-remaining"])
        except:
            if (header['status'] == '503') or (header['status'] == '429'):
                self.echo.default(u"Ratelimit de %s esgotado." % application["name"], "WARNING")
            else:
                message = u"Falha na requisição para verificar ratelimit de %s.\n" % application["name"]
                message += u"Header: %s\n" % header
                message += u"Content: %s\n" % content
                self.echo.default(message, "ERROR")
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
    #print InstagramAppFilter()._spendSpecificRate("CampsApp18", 1)
    print InstagramAppFilter({"name": None, 
                              "appsfile": "apps.json", 
                              "dynamicallyload": "False",
                              "method": "maxpost",}).apply(None, None, None)
