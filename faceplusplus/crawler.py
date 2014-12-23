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
    # Upon initialization the crawler object receives a copy of everything in the client 
    # section of the XML configuration file as the parameter configurationsDictionary
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary

    # Valores de retorno globais:
    #     3 => Coleta bem sucedida
    #    -4 => Erro em alguma das mídias
    # Valores de retorno individuais:
    #    200 => Coleta bem sucedida
    #    4** => Ver lista de códigos HTTP ao final da página http://www.faceplusplus.com/detection_detect/
    def crawl(self, resourceID, filters):
        globalResponseCode = 3
        individualResponseCodes = []
        
        echo = common.EchoHandler(self.config)
        echo.default(u"User ID received: %s." % resourceID)
    
        # Configura tratamento de exceções
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diretórios base para armazenamento
        fppBaseDir = "data/fpp"
        fppDataDir = os.path.join(fppBaseDir, str(resourceID % 1000), str(resourceID))
        if not os.path.exists(fppDataDir): os.makedirs(fppDataDir)
        
        # Carrega arquivo de feed do usuário
        feedsBaseDir = "data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Determina mídias a serem coletadas
        feedSampleSize = 10
        feedList = []
        if (len(feed) <= feedSampleSize): 
            feedList = feed
        else: 
            for i in range(feedSampleSize):
                media = random.randint(0, len(feed) - 1)
                feedList.append(feed[media])
        
        # Executa coleta
        attributes = ["gender", "age", "race", "smiling", "glass", "pose"]
        api = API(key = fpp.apikey.API_KEY, secret = fpp.apikey.API_SECRET, srv = fpp.apikey.SERVER)
        for i, media in enumerate(feedList):
            echo.default(u"Collecting media %d." % (i + 1))
            while (True):
                try: 
                    response = api.detection.detect(url = media["images"]["standard_resolution"]["url"], attribute = attributes)
                except APIError as error:
                    # Se o erro não for INTERNAL_ERROR ou SERVER_TOO_BUSY, apenas reporta e prossegue
                    if ((error.code != 500) and (error.code != 502)):
                        globalResponseCode = -4
                        individualResponseCodes.append((media["id"], {"response_code": error.code}))
                        break
                    else:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            echo.exception(u"API call error. Trying again in %02d second(s)." % sleepSeconds)
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("Maximum number of retrys exceeded.")
                else:
                    retrys = 0
                    sleepSecondsMultiply = 3
                    fppFilePath = os.path.join(fppDataDir, "%s.fpp" % resourceID)
                    with open(fppFilePath, "w") as fppFile: json.dump(response, fppFile)
                    individualResponseCodes.append((media["id"], {"response_code": 200}))
                    break
        
        print individualResponseCodes
        
        return ({#"crawler_name": socket.gethostname(), 
                "response_code": globalResponseCode}, 
                None,
                individualResponseCodes)
        