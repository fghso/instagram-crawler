# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import common
from datetime import datetime
from datetime import timedelta
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from instagram.bind import InstagramClientError


class Crawler:
    # Upon initialization the crawler object receives a copy of everything in the client 
    # section of the XML configuration file as the parameter configurationsDictionary
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary

    # Valores de retorno:
    #    3 => Coleta bem sucedida
    #   -4 => APINotAllowedError - you cannot view this resource
    #   -5 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):      
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Extrai filtros
        for f in filters: 
            if (f["name"] == "InstagramAppFilter"): application = f["data"]["application"]
    
        # Constr�i objeto da API com as credenciais de acesso
        clientID = application["clientid"]
        clientSecret = application["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.out(u"App: %s." % str(application["name"]))

        # Configura tratamento de exce��es
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diret�rios base para armazenamento
        feedsBaseDir = "../../data/feeds"
        feedsDataDir = os.path.join(feedsBaseDir, str(resourceID % 1000))
        if not os.path.exists(feedsDataDir): os.makedirs(feedsDataDir)
        
        # Configura data m�nima de m�dias a coletar
        #timeInterval = datetime.utcnow() - timedelta(90)
        #minTimestamp = calendar.timegm(timeInterval.utctimetuple())
        minTimestamp = 1322697600 # = 12/01/2011 @ 12:00am (UTC)
        
        # Inicializa vari�veis de retorno
        responseCode = 3
        extraInfo = {"InstagramAppFilter": {}, "SaveResourcesFilter": []}
        
        # Executa coleta
        feedList = []
        pageCount = 0
        #mediaCount = 0
        nextUserRecentMediaPage = ""
        while (nextUserRecentMediaPage is not None):
            pageCount += 1
            echo.out(u"Collecting feed page %d." % pageCount)
            while (True):
                try:
                    # Executa requisi��o na API para obter m�dias do feed do usu�rio
                    userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=35, user_id=resourceID, return_json=True, with_next_url=nextUserRecentMediaPage, min_timestamp=minTimestamp)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        # Se o usu�rio tiver o perfil privado ou n�o existir, captura exce��o e reporta erro
                        if (error.error_type == "APINotAllowedError"):
                            responseCode = -4
                            nextUserRecentMediaPage = None
                            break
                        elif (error.error_type == "APINotFoundError"):
                            responseCode = -5
                            nextUserRecentMediaPage = None
                            break
                    else:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            echo.out(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("Maximum number of retrys exceeded.")
                else:
                    retrys = 0
                    sleepSecondsMultiply = 3
                    if (userRecentMedia):
                        #mediaCount += len(userRecentMedia)
                        feedList.extend(userRecentMedia) 
                        
                        # Extrai dados das m�dias para enviar de volta ao SaveResourcesFilter
                        for media in userRecentMedia:
                            mediaInfo = {"type": media["type"], 
                                         "url": media["images"]["standard_resolution"]["url"]}
                            extraInfo["SaveResourcesFilter"].append((media["id"], mediaInfo))
                        
                    break
        
        # Salva arquivo JSON com informa��es sobre as m�dias do feed do usu�rio
        output = open(os.path.join(feedsDataDir, "%s.feed" % resourceID), "w")
        json.dump(feedList, output)
        output.close()
        
        # Obt�m rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining)

        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                #"media_count": mediaCount},
                extraInfo,
                None)
        