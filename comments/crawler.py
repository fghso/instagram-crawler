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
    #   -4 => Erro em alguma das m�dias
    def crawl(self, resourceID, filters):      
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Extrai filtros
        application = filters[0]["data"]["application"]
    
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
        commentsBaseDir = "../../data/comments"
        commentsDataDir = os.path.join(commentsBaseDir, str(resourceID % 1000))
        if not os.path.exists(commentsDataDir): os.makedirs(commentsDataDir)
        
        # Carrega arquivo de feed do usu�rio
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Inicializa vari�veis de retorno
        responseCode = 3
        extraInfo = {"InstagramAppFilter": {}}
        
        # Executa coleta
        comments = []
        for media in feed:
            echo.out(u"Media: %s." % media["id"])
            while (True):
                try:
                    # Executa requisi��o na API para obter coment�rios da m�dia
                    mediaComments = api.media_comments(media_id=media["id"], return_json=True)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        echo.out(error, "ERROR")
                        responseCode = -4
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
                    comments.extend(mediaComments)
                    break
        
            # Salva arquivo JSON com informa��es sobre os coment�rios do feed do usu�rio
            output = open(os.path.join(commentsDataDir, "%s.comments" % media["id"]), "w")
            json.dump(feed, output)
            output.close()
        
        # Obt�m rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining) if api.x_ratelimit_remaining else None

        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                extraInfo,
                None)
        