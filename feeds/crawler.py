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
        responseCode = 3
        echo = common.EchoHandler(self.config)
    
        # Constrói objeto da API com as credenciais de acesso
        clientID = filters[0]["data"]["application"]["clientid"]
        clientSecret = filters[0]["data"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.default(u"App: %s." % str(filters[0]["data"]["application"]["name"]))

        # Configura tratamento de exceções
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diretórios base para armazenamento
        feedsBaseDir = "data/feeds"
        feedsDataDir = os.path.join(feedsBaseDir, str(resourceID % 1000))
        if not os.path.exists(feedsDataDir): os.makedirs(feedsDataDir)
        
        # Configura data mínima de mídias a coletar
        #timeInterval = datetime.utcnow() - timedelta(90)
        #minTimestamp = calendar.timegm(timeInterval.utctimetuple())
        minTimestamp = 1322697600 # = 12/01/2011 @ 12:00am (UTC)
        
        # Executa coleta
        feedList = []
        pageCount = 0
        #mediaCount = 0
        nextUserRecentMediaPage = ""
        while (nextUserRecentMediaPage is not None):
            while (True):
                try:
                    # Executa requisição na API para obter mídias do feed do usuário
                    userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=35, user_id=resourceID, return_json=True, with_next_url=nextUserRecentMediaPage, min_timestamp=minTimestamp)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        # Se o usuário tiver o perfil privado ou não existir, captura exceção e marca erro no banco de dados
                        if (error.error_type == "APINotAllowedError"):
                            responseCode = -4
                            responseString = "APINotAllowedError"
                            nextUserRecentMediaPage = None
                            break
                        elif (error.error_type == "APINotFoundError"):
                            responseCode = -5
                            responseString = "APINotFoundError"
                            nextUserRecentMediaPage = None
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
                    if (userRecentMedia):
                        pageCount += 1
                        #mediaCount += len(userRecentMedia)
                        echo.default(u"Collecting feed page %d of user %s." % (pageCount, resourceID))
                        feedList.extend(userRecentMedia) 
                    break
        
        # Salva arquivo JSON com informações sobre as mídias do feed do usuário
        output = open(os.path.join(feedsDataDir, "%s.feed" % resourceID), "w")
        json.dump(feedList, output)
        output.close()
        
        # Obtém rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo = {"application": {}}
        extraInfo["application"]["name"] = filters[0]["data"]["application"]["name"]
        extraInfo["application"]["rate"] = int(api.x_ratelimit_remaining)

        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                #"response_string": responseString},
                #"media_count": mediaCount},
                extraInfo,
                None)
        