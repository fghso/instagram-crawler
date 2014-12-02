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
# section of the XML configuration file
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
    
        if ("name" in self.config): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def getName(self): return self.name
 
    # Apply must return a dictionary containing the desired filter information to be sent to the client. 
    # The value of previousFilterData will always be None if the filter is executed in parallel
    def apply(self, resourceID, resourceInfo, previousFilterData):
        return resourceInfo
        
    def close(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources


class InstagramAppFilter(BaseFilter):
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        self.config["appsfile"] = self.config["appsfile"].encode("utf-8")
        self._loadAppFile()
        self.echo = common.EchoHandler()
        
    def apply(self, resourceID, resourceInfo, previousFilterData):
        #self._loadAppFile()
        # Escolhe aplicação aleatória
        if (common.str2bool(self.config["getrandom"])): 
            appIndex = random.randint(0, len(self.applicationList) - 1)
        # Escolhe aplicação com a maior quantidade de requisições disponíveis
        else:
            while (True):
                (bestRate, appIndex) = self._getBestRate()
                if (bestRate == 0): time.sleep(300)
                else: break
        return OrderedDict([("application", self.applicationList[appIndex])])
        
    def _loadAppFile(self):
        if os.path.exists(os.path.join(sys.path[0], self.config["appsfile"])): 
            appsFile = open(os.path.join(sys.path[0], self.config["appsfile"]), "r")
        else: 
            appsFile = open(os.path.join(sys.path[1], self.config["appsfile"]), "r")
        #appDict = xmltodict.parse(appFile.read())
        #self.applicationList = appDict["instagram"]["application"] if isinstance(appDict["instagram"]["application"], list) else [appDict["instagram"]["application"]]
        self.applicationList = json.load(appsFile)["application"]
        appsFile.close()
    
    def _getBestRate(self):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        rateList = []
        for application in self.applicationList:
            clientID = application["clientid"]
            rateRemaining = 0
            try: 
                (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
                rateRemaining = int(header["x-ratelimit-remaining"])
                print application["name"], rateRemaining
            except:
                if (header['status'] == '503') or (header['status'] == '429'):
                    self.echo.default("Ratelimit de %s esgotado." % application["name"], "WARNING")
                else:
                    message = "Falha na requisicao para verificar ratelimit de %s.\n" % application["name"]
                    message += "Header: %s\n" % header
                    message += "Content: %s\n" % content
                    self.echo.default(message, "ERROR")
            rateList.append(rateRemaining)
        maxRate = max(rateList)
        return (maxRate, rateList.index(maxRate))
                
    # Funções apenas para testes
    def _spendRandomRate(self, repeat):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            appIndex = random.randint(0, len(self.applicationList) - 1)
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["name"], header["x-ratelimit-remaining"]
            
    def _spendSpecificRate(self, appIndex, repeat):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["name"], header["x-ratelimit-remaining"]#, content 

            
# ====== Testes ===== 
if __name__ == "__main__":
    #print InstagramAppFilter()._spendRandomRate(50)
    #print InstagramAppFilter()._spendSpecificRate(18, 1)
    print InstagramAppFilter({"name": None, "appsfile": "apps.json", "getrandom": "False"}).apply(None, None, None)
