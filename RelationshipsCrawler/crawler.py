# -*- coding: iso-8859-1 -*-

import os
import json
import time
import logging
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
import app


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return app.name

    # Valores de retorno:
    #    2 => Coleta bem sucedida
    #   -2 => APINotAllowedError - you cannot view this resource
    #   -3 => APINotFoundError - this user does not exist
    def crawl(self, resourceID):
        status = 2
        amount = 0
        
        # Constr�i objeto da API com as credenciais de acesso
        api = InstagramAPI(client_id = app.clientID, client_secret = app.clientSecret)
    
        # Configura logging
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                            filename="InstagramRelationshipsCrawler[%s].log" % app.name, filemode="w", level=logging.INFO)

        # Configura tratamento de exce��es
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Cria diret�rio para armazenar relacionamentos
        dir = os.path.abspath("../CrawledData/Relationships/%s" % resourceID)
        if not os.path.exists(dir):
            os.makedirs(dir)
            
        # ----- Executa coleta da lista de usu�rios seguidos -----
        fileNumber = 1
        nextCursor = ""
        nextFollowsPage = ""
        while (nextFollowsPage != None):
            while (True):
                # Parseia url da pr�xima p�gina para extrair o cursor
                urlParts = nextFollowsPage.split("&")
                if len(urlParts) > 1:
                    nextCursor = urlParts[1].split("=")[1]
            
                try:
                    follows, nextFollowsPage = api.user_follows(user_id=resourceID, return_json=True, count=30, cursor=nextCursor)
                except InstagramAPIError as err:
                    if (err.error_type == "APINotAllowedError"):
                        status = -2
                        nextFollowsPage = None
                        break
                    elif (err.error_type == "APINotFoundError"):
                        status = -3
                        nextFollowsPage = None
                        break
                    else:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usu�rio: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro no cliente. Execu��o abortada. [Usu�rio: %s]" % resourceID)
                            raise
                except Exception as err:
                    # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                    # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usu�rio: %s]" % (sleepSeconds, resourceID))
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                        continue
                    else:
                        logging.exception(u"Erro no cliente. Execu��o abortada. [Usu�rio: %s]" % resourceID)
                        raise
                else:
                    retrys = 0
                    sleepSecondsMultiply = 0
                    
                    # Salva arquivo JSON com a lista de usu�rios seguidos
                    filename = "follows%d.json" % fileNumber
                    output = open(os.path.join(dir, filename), "w")
                    json.dump(follows, output)
                    output.close()
                    
                    fileNumber += 1
                    amount += len(follows)
                    break

        # ----- Executa coleta da lista de seguidores -----
        if (status == 2):
            fileNumber = 1
            nextCursor = ""
            nextFollowedByPage = ""
            while (nextFollowedByPage != None):
                while (True):
                    # Parseia url da pr�xima p�gina para extrair o cursor
                    urlParts = nextFollowedByPage.split("&")
                    if len(urlParts) > 1:
                        nextCursor = urlParts[1].split("=")[1]
                
                    try:
                        followedby, nextFollowedByPage = api.user_followed_by(user_id=resourceID, return_json=True, count=30, cursor=nextCursor)
                    except Exception as err:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Usu�rio: %s]" % (sleepSeconds, resourceID))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro no cliente. Execu��o abortada. [Usu�rio: %s]" % resourceID)
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
                        amount += len(followedby)
                        break
                    
        return (status, amount)
