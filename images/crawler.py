# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import urllib2
import common


class Crawler:
    # Upon initialization the crawler object receives a copy of everything in the client 
    # section of the XML configuration file as the parameter configurationsDictionary
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary

    # Valores de retorno:
    #    3 => Coleta bem sucedida
    #   -4 => Erro em alguma das m�dias
    def crawl(self, resourceID, filters):      
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Configura tratamento de exce��es
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diret�rios base para armazenamento
        imagesBaseDir = "../../data/images"
        imagesDataDir = os.path.join(imagesBaseDir, str(resourceID % 1000))
        if not os.path.exists(imagesDataDir): os.makedirs(imagesDataDir)
        
        # Carrega arquivo de feed do usu�rio
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Inicializa vari�veis de retorno
        responseCode = 3
        
        # Executa coleta
        for media in feed:
            echo.out(u"Media: %s." % media["id"])
            while (True):
                try:
                    # Executa requisi��o para obter a imagem
                    header = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
                    request = urllib2.Request(media["images"]["standard_resolution"]["url"], headers = header)
                    #request = urllib2.Request(media["images"]["standard_resolution"]["url"])
                    imageData = urllib2.urlopen(request).read()
                except Exception as error:
                    if type(error) == urllib2.HTTPError:
                        echo.out(error, "ERROR")
                        responseCode = -4
                        break
                    else:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            echo.out(u"Request error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("Maximum number of retrys exceeded.")
                else:
                    retrys = 0
                    sleepSecondsMultiply = 3
                        
                    # Salva a imagem no disco
                    imageFile = "%s.jpg" % media["id"]
                    output = open(os.path.join(imagesDataDir, imageFile), "wb")
                    output.write(imageData)
                    output.close()
                    
                    break
        
        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                None,
                None)
        