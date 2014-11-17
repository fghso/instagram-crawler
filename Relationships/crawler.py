# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import logging
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError


class Crawler:
    # Valores de retorno:
    #    3 => Coleta bem sucedida
    #   -3 => APINotAllowedError - you cannot view this resource
    #   -4 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        responseCode = 3
        followsCount = 0
        followedByCount = 0
        
        # Constrói objeto da API com as credenciais de acesso
        clientID = filters[0]["data"]["application"]["clientid"]
        clientSecret = filters[0]["data"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        logging.info("Aplicação: %s." % str(filters[0]["data"]["application"]["name"]))
    
        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Cria diretório para armazenar relacionamentos
        dir = os.path.abspath("2ndRelationships/%s" % resourceID)
        if not os.path.exists(dir):
            os.makedirs(dir)
            
        # ----- Executa coleta da lista de usuários seguidos -----
        logging.info("Coletando a lista de seguidos do usuário %s." % resourceID)
        fileNumber = 1
        nextFollowsPage = ""
        while (nextFollowsPage is not None):
            while (True):
                try:
                    follows, nextFollowsPage = api.user_follows(user_id=resourceID, return_json=True, count=100, with_next_url=nextFollowsPage)
                except InstagramAPIError as err:
                    if (err.error_type == "APINotAllowedError"):
                        responseCode = -3
                        nextFollowsPage = None
                        break
                    elif (err.error_type == "APINotFoundError"):
                        responseCode = -4
                        nextFollowsPage = None
                        break
                    else:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Falha na requisição, tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro na requisição, processamento interrompido. [Usuário: %s]" % resourceID)
                            raise
                except Exception as err:
                    # Caso o número de tentativas não tenha ultrapassado o máximo,
                    # experimenta aguardar um certo tempo antes da próxima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning(u"Falha na requisição, tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                        continue
                    else:
                        logging.exception(u"Erro na requisição, processamento interrompido. [Usuário: %s]" % resourceID)
                        raise
                else:
                    retrys = 0
                    sleepSecondsMultiply = 0
                    
                    # Salva arquivo JSON com a lista de usuários seguidos
                    filename = "follows%d.json" % fileNumber
                    output = open(os.path.join(dir, filename), "w")
                    json.dump(follows, output)
                    output.close()
                    
                    fileNumber += 1
                    followsCount += len(follows)
                    break

        # ----- Executa coleta da lista de seguidores -----
        if (responseCode == 3):
            logging.info("Coletando a lista de seguidores do usuário %s." % resourceID)
            fileNumber = 1
            nextFollowedByPage = ""
            while (nextFollowedByPage is not None):
                while (True):
                    try:
                        followedby, nextFollowedByPage = api.user_followed_by(user_id=resourceID, return_json=True, count=100, with_next_url=nextFollowedByPage)
                    except Exception as err:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Falha na requisição, tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro na requisição, processamento interrompido. [Usuário: %s]" % resourceID)
                            raise
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 0
                        
                        # Salva arquivo JSON com a lista de seguidores
                        filename = "followedby%d.json" % fileNumber
                        output = open(os.path.join(dir, filename), "w")
                        json.dump(followedby, output)
                        output.close()
                        
                        fileNumber += 1
                        followedByCount += len(followedby)
                        break
                    
        return ({"crawler_name": socket.gethostname(), 
                 "response_code": responseCode, 
                 "follows_count": followsCount, 
                 "followed_by_count": followedByCount}, 
                 None)
        
    # This function is called by the client when the crawl function raises an exception. This gives an opportunity to 
    # clean up any allocated item associated with the resource being collected (for example, erase directories created),
    # so that the system remains in a consistent state, allowing the resource to be crawled again
    def clean(self):
        pass
