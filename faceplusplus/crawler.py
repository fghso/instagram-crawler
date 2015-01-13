# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import common
import random
import fpp.apikey
from fpp.facepp import API
from fpp.facepp import APIError


class Crawler:
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary
    
    def crawl(self, resourceID, filters):
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Extrai filtros
        application = filters[0]["data"]["application"]
    
        # Constrói objeto da API com as credenciais de acesso
        apiServer = application["apiserver"]
        apiKey = application["apikey"]
        apiSecret = application["apisecret"]
        api = API(srv = apiServer, key = apiKey, secret = apiSecret, timeout = 5, max_retries = 0, retry_delay = 0)
        echo.out(u"App: %s." % str(application["name"]))
    
        # Configura diretórios base para armazenamento
        fppBaseDir = "../../data/fpp"
        fppDataDir = os.path.join(fppBaseDir, str(resourceID % 1000), str(resourceID))
        if not os.path.exists(fppDataDir): os.makedirs(fppDataDir)
        
        # Carrega arquivo de feed do usuário
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Determina mídias a serem coletadas
        feedSampleSize = 10
        feedList = random.sample(feed, min(len(feed), feedSampleSize))
        
        # Inicializa variáveis de retorno
        extraInfo = {"mediaerrors": [], "usersok": [], "usersproblem": []}
        
        # Executa coleta
        attributes = ["gender", "age", "race", "smiling", "glass", "pose"]
        for i, media in enumerate(feedList):
            echo.out(u"Collecting media %d." % (i + 1))
            while (True):
                try:
                    response = api.detection.detect(url = media["images"]["low_resolution"]["url"], attribute = attributes)
                except Exception as error: 
                    # Códigos de erro HTTP: http://www.faceplusplus.com/detection_detect/
                    if isinstance(error, APIError): message = "%d: %s" % (error.code, json.loads(error.body)["error"])
                    # socket.error e urllib2.URLError 
                    else: message = str(error)
                    extraInfo["mediaerrors"].append((media["id"], {"error": message}))
                    extraInfo["usersproblem"].append((resourceID, None))
                    return (None, extraInfo, None)
                else:
                    fppFilePath = os.path.join(fppDataDir, "%s.fpp" % media["id"])
                    with open(fppFilePath, "w") as fppFile: json.dump(response, fppFile)
                    break
                    
        extraInfo["usersok"].append((resourceID, None))
        return (None, extraInfo, None)
