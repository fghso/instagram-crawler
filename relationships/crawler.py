# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import common
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
        
        # Cria diretório para armazenar relacionamentos
        followsBaseDir = "data/relationships/follows"
        followedbyBaseDir = "data/relationships/follow"
        followsDataDir = os.path.join(followsBaseDir, str(resourceID % 1000))
        followedbyDataDir = os.path.join(followedbyBaseDir, str(resourceID % 1000))
        if not os.path.exists(followsDataDir): os.makedirs(followsDataDir)
        if not os.path.exists(followedbyDataDir): os.makedirs(followedbyDataDir)
        
        # ----- Executa coleta da lista de usuários seguidos -----
        echo.default("Collecting follows for user %s." % resourceID)
        followsList = []
        pageCount = 0
        #followsCount = 0
        nextFollowsPage = ""
        while (nextFollowsPage is not None):
            while (True):
                try:
                    follows, nextFollowsPage = api.user_follows(count=100, user_id=resourceID, return_json=True,
                    with_next_url=nextFollowsPage)
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
                    if (follows):
                        pageCount += 1
                        #followsCount += 1
                        echo.default(u"Collecting follows page %d of user %s." % (pageCount, resourceID))
                        followsList.extend(follows) 
                    break
                    
        # Salva arquivo JSON com a lista de usuários seguidos
        output = open(os.path.join(followsDataDir, "%s.follows" % resourceID), "w")
        json.dump(followsList, output)
        output.close()
        
        # ----- Executa coleta da lista de seguidores -----
        if (responseCode == 3):
            echo.default("Collecting followed_by for user %s." % resourceID)
            followedbyList = []
            pageCount = 0
            #followedByCount = 0
            nextFollowedByPage = ""
            while (nextFollowedByPage is not None):
                while (True):
                    try:
                        followedby, nextFollowedByPage = api.user_followed_by(count=100, user_id=resourceID, return_json=True, with_next_url=nextFollowedByPage)
                    except Exception as error:
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
                        if (followedby):
                            pageCount += 1
                            #followedByCount += 1
                            echo.default(u"Collecting followed_by page %d of user %s." % (pageCount, resourceID))
                            followedbyList.extend(followedby) 
                        break
                        
            # Salva arquivo JSON com a lista de seguidores
            output = open(os.path.join(followedbyDataDir, "%s.followedby" % resourceID), "w")
            json.dump(followedbyList, output)
            output.close()
            
        # Obtém rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo = {"application": {}}
        extraInfo["application"]["name"] = filters[0]["data"]["application"]["name"]
        extraInfo["application"]["rate"] = int(api.x_ratelimit_remaining)
            
        return ({#"crawler_name": socket.gethostname(), 
                 "response_code": responseCode}, 
                 #"response_string": responseString},
                 #"follows_count": followsCount, 
                 #"followed_by_count": followedByCount}, 
                 extraInfo,
                 None)
                 