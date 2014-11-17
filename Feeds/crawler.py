# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import logging
from datetime import datetime
from datetime import timedelta
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError


class Crawler:
    # Valores de retorno:
    #    3 => Coleta bem sucedida
    #   -3 => APINotAllowedError - you cannot view this resource
    #   -4 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        responseCode = 3
    
        # Constr�i objeto da API com as credenciais de acesso
        clientID = filters[0]["data"]["application"]["clientid"]
        clientSecret = filters[0]["data"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        logging.info(u"Aplicacao: %s." % str(filters[0]["data"]["application"]["name"]))

        # Configura tratamento de exce��es
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diret�rios base para armazenamento
        feedsDataDir = "../data/feeds"
        if not os.path.exists(feedsDataDir): os.makedirs(feedsDataDir)
        
        # Executa coleta
        feedList = []
        pageCounter = 0
        mediaCounter = 0
        nextUserRecentMediaPage = ""
        while (nextUserRecentMediaPage is not None):
            try:
                # Executa requisi��o na API para obter m�dias do feed do usu�rio
                userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=35, user_id=resourceID, return_json=True, with_next_url=nextUserRecentMediaPage)
            except InstagramAPIError as err:
                # Se o usu�rio tiver o perfil privado ou n�o existir, captura exce��o e marca erro no banco de dados
                if (err.error_type == "APINotAllowedError"):
                    responseCode = -3
                    break
                elif (err.error_type == "APINotFoundError"):
                    responseCode = -4
                    break
                else:
                    # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                    # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning(u"Erro na chamada a API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                    else:
                        raise
            else:
                retrys = 0
                sleepSecondsMultiply = 3
                if (userRecentMedia):
                    pageCounter += 1
                    mediaCounter += len(userRecentMedia)
                    logging.info(u"Coletando pagina %d de feeds do usuario %s." % (pageCounter, resourceID))
                    feedList.extend(userRecentMedia) 
        
        # Salva arquivo JSON com informa��es sobre as m�dias do feed do usu�rio
        output = open(os.path.join(feedsDataDir, "%s.feed" % resourceID), "w")
        json.dump(feedList, output)
        output.close()

        return ({"crawler_name": socket.gethostname(), 
                "response_code": responseCode, 
                "media_count": mediaCounter},
                None)
        