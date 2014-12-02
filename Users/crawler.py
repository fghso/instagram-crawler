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
    # Valores de retorno:
    #    3 => Coleta bem sucedida
    #   -3 => APINotAllowedError - you cannot view this resource
    #   -4 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        responseCode = 3
        responseString = "OK"
        
        echo = common.EchoHandler()
        echo.default(u"Usu�rio recebido para coleta: %s." % resourceID)
        
        # Constr�i objeto da API com as credenciais de acesso
        clientID = filters[0]["data"]["application"]["clientid"]
        clientSecret = filters[0]["data"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.default(u"Aplicacao: %s." % str(filters[0]["data"]["application"]["name"]))

        # Configura tratamento de exce��es
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configura diret�rio base para armazenamento
        usersDataDir = "data/users"
        if not os.path.exists(usersDataDir): os.makedirs(usersDataDir)
        
        # Executa coleta se o arquivo ainda n�o existir (isto �, se o recurso ainda n�o houver sido coletado)
        if not os.path.exists(os.path.join(usersDataDir, "%s.user" % resourceID)):
            while (True):
                try:
                    userInfo = api.user(user_id=resourceID, return_json=True)
                except (InstagramAPIError, InstagramClientError) as err:
                    if (err.status_code == 400):
                        # Se o usu�rio tiver o perfil privado ou n�o existir, captura exce��o e marca erro no banco de dados
                        if (err.error_type == "APINotAllowedError"):
                            responseCode = -3
                            responseString = "APINotAllowedError"
                            break
                        elif (err.error_type == "APINotFoundError"):
                            responseCode = -4
                            responseString = "APINotFoundError"
                            break
                    else:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            echo.default(u"Erro na chamada a API. Tentando novamente em %02d segundo(s)." % sleepSeconds, "WARNING")
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("N�mero m�ximo de tentativas excedido.")                    
                else:
                    # Salva arquivo JSON com informa��es sobre o usu�rio
                    output = open(os.path.join(usersDataDir, "%s.user" % resourceID), "w")
                    json.dump(userInfo, output)
                    output.close()
                    break

        return ({"crawler_name": socket.gethostname(), 
                "response_code": responseCode, 
                "response_string": responseString},
                None)
        