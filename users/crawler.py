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
        responseString = "OK"
        
        echo = common.EchoHandler(self.config)
        echo.default(u"User ID received: %s." % resourceID)
        
        # Constrói objeto da API com as credenciais de acesso
        clientID = filters[0]["data"]["application"]["clientid"]
        clientSecret = filters[0]["data"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.default(u"App: %s." % str(filters[0]["data"]["application"]["name"]))

        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diretório base para armazenamento
        usersDataDir = "data/users"
        if not os.path.exists(usersDataDir): os.makedirs(usersDataDir)
        
        # Executa coleta
        while (True):
            try:
                userInfo = api.user(user_id=resourceID, return_json=True)
            except (InstagramAPIError, InstagramClientError) as error:
                if (error.status_code == 400):
                    # Se o usuário tiver o perfil privado ou não existir, captura exceção e marca erro no banco de dados
                    if (error.error_type == "APINotAllowedError"):
                        responseCode = -4
                        responseString = "APINotAllowedError"
                        break
                    elif (error.error_type == "APINotFoundError"):
                        responseCode = -5
                        responseString = "APINotFoundError"
                        break
                else:
                    # Caso o número de tentativas não tenha ultrapassado o máximo,
                    # experimenta aguardar um certo tempo antes da próxima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        echo.exception(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "WARNING")
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
                break

        # Obtém rate remaining para enviar de volta ao InstagramAppFilter
        extraInfo = {"application": {}}
        extraInfo["application"]["name"] = filters[0]["data"]["application"]["name"]
        extraInfo["application"]["rate"] = int(api.x_ratelimit_remaining)
                
        return ({"crawler_name": socket.gethostname(), 
                "response_code": responseCode, 
                "response_string": responseString},
                extraInfo,
                None)
        