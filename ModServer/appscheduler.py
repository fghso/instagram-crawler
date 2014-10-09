#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import xmltodict
import httplib2
import time
import logging
import random
from collections import OrderedDict


class CrawlerParams():
    def __init__(self):
        paramsFile = open("params.xml", "r")
        paramsDict = xmltodict.parse(paramsFile.read())
        self.databaseParams = OrderedDict([("database", paramsDict["params"]["database"])])
        self.applicationList = paramsDict["params"]["instagram"]["application"]
        
    def getParams(self):
        while (True):
            (bestRate, appIndex) = self._getBestRate()
            if bestRate == 0: time.sleep(300)
            else: break
        applicationParams = OrderedDict([("application", self.applicationList[appIndex])])
        params = self.databaseParams
        params.update(applicationParams)
        return params

    def _getBestRate(self):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        rateList = []
        for application in self.applicationList:
            clientID = application["clientid"]
            rateRemaining = 0
            try: 
                (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            except:
                logging.exception("Falha na requisição para verificar ratelimit de %s" % application["@name"])
            else:
                if header['status'] != '503' and header['status'] != '429':
                    rateRemaining = int(header["x-ratelimit-remaining"])
            rateList.append(rateRemaining)
        maxRate = max(rateList)
        return (maxRate, rateList.index(maxRate))
        
    # Função apenas para testes
    def _spendRandomRate(self, repeat):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            appIndex = random.randint(0,19)
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["@name"], header["x-ratelimit-remaining"]
 

# ====== Testes ===== 
#print CrawlerParams()._spendRandomRate(50)
#print CrawlerParams().getParams()
        