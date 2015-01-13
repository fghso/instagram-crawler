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
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Extrai filtros
        application = filters[0]["data"]["application"]
            
        # Constrói objeto da API com as credenciais de acesso
        clientID = application["clientid"]
        clientSecret = application["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.out(u"App: %s." % str(application["name"]))

        # Configura tratamento de exceções
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diretório base para armazenamento
        usersBaseDir = "../../data/users"
        usersDataDir = os.path.join(usersBaseDir, str(resourceID % 1000))
        if not os.path.exists(usersDataDir): os.makedirs(usersDataDir)
        
        # Inicializa variáveis de retorno
        responseCode = 3
        #extraInfo = {"InstagramAppFilter": {}, "SaveResourcesFilter": []}
        extraInfo = {"InstagramAppFilter": {}}
        
        # Executa coleta
        while (True):
            try:
                userInfo = api.user(user_id=resourceID, return_json=True)
            except (InstagramAPIError, InstagramClientError) as error:
                if (error.status_code == 400):
                    # Se o usuário tiver o perfil privado ou não existir, captura exceção e reporta erro
                    if (error.error_type == "APINotAllowedError"):
                        responseCode = -4
                        break
                    elif (error.error_type == "APINotFoundError"):
                        responseCode = -5
                        break
                else:
                    # Caso o número de tentativas não tenha ultrapassado o máximo,
                    # experimenta aguardar um certo tempo antes da próxima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        echo.out(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                    else:
                        raise SystemExit("Maximum number of retrys exceeded.")
            else:
                # Salva arquivo JSON com informações sobre o usuário
                output = open(os.path.join(usersDataDir, "%s.user" % resourceID), "w")
                json.dump(userInfo, output)
                output.close()
                
                # Extrai contadores do usuário para enviar de volta ao SaveResourcesFilter       
                # userCounts = {"counts_media": userInfo["counts"]["media"], 
                              # "counts_follows": userInfo["counts"]["follows"], 
                              # "counts_followedby": userInfo["counts"]["followed_by"]}
                # extraInfo["SaveResourcesFilter"].append((resourceID, userCounts))
                
                break

        # Obtém rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining)
        
        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode},
                extraInfo,
                None)
        