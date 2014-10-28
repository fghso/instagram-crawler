# -*- coding: iso-8859-1 -*-

import os
import json
import socket
import time
import logging
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return socket.gethostname()

    # Valores de retorno:
    #    2 => Coleta bem sucedida
    #   -2 => APINotAllowedError - you cannot view this resource
    #   -3 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, loggingActive, filters):
        response = 2
    
        # Constrói objeto da API com as credenciais de acesso
        clientID = filters["InstagramAppFilter"]["application"]["clientid"]
        clientSecret = filters["InstagramAppFilter"]["application"]["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
    
        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Cria diretório para armazenar relacionamentos
        dir = os.path.abspath("../data/relationships/%s" % resourceID)
        if not os.path.exists(dir):
            os.makedirs(dir)
            
        # ----- Executa coleta da lista de usuários seguidos -----
        nextCursor = ""
        nextFollowsPage = ""
        followsList = []
        while (nextFollowsPage != None):
            while (True):
                # Parseia url da próxima página para extrair o cursor
                urlParts = nextFollowsPage.split("&")
                if len(urlParts) > 1:
                    nextCursor = urlParts[1].split("=")[1]
            
                try:
                    follows, nextFollowsPage = api.user_follows(user_id=resourceID, return_json=True, count=35, cursor=nextCursor)
                except InstagramAPIError as err:
                    if (err.error_type == "APINotAllowedError"):
                        response = -2
                        nextFollowsPage = None
                        break
                    elif (err.error_type == "APINotFoundError"):
                        response = -3
                        nextFollowsPage = None
                        break
                    else:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro no cliente. Execução abortada. [Usuário: %s]" % resourceID)
                            raise
                except Exception as err:
                    # Caso o número de tentativas não tenha ultrapassado o máximo,
                    # experimenta aguardar um certo tempo antes da próxima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                        continue
                    else:
                        logging.exception(u"Erro no cliente. Execução abortada. [Usuário: %s]" % resourceID)
                        raise
                else:
                    retrys = 0
                    sleepSecondsMultiply = 0
                    followsList.append(follows)
                    break
                    
        # Salva arquivo JSON com a lista de usuários seguidos
        filename = "%s.follows" % resourceID
        output = open(os.path.join(dir, filename), "w")
        json.dump(followsList, output)
        output.close()
                    

        # ----- Executa coleta da lista de seguidores -----
        followedByList = []
        if (response == 2):
            nextCursor = ""
            nextFollowedByPage = ""
            while (nextFollowedByPage != None):
                while (True):
                    # Parseia url da próxima página para extrair o cursor
                    urlParts = nextFollowedByPage.split("&")
                    if len(urlParts) > 1:
                        nextCursor = urlParts[1].split("=")[1]
                
                    try:
                        followedby, nextFollowedByPage = api.user_followed_by(user_id=resourceID, return_json=True, count=35, cursor=nextCursor)
                    except Exception as err:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usuário: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro no cliente. Execução abortada. [Usuário: %s]" % resourceID)
                            raise
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 0
                        followedByList.append(followedby)
                        break
                        
            # Salva arquivo JSON com a lista de seguidores
            filename = "%s.followedby" % resourceID
            output = open(os.path.join(dir, filename), "w")
            json.dump(followedByList, output)
            output.close()
                    
        return ({"responsecode": response, "follows_count": len(followsList), "followed_by_count": len(followedByList)}, None)
