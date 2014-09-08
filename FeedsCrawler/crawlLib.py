# -*- coding: iso-8859-1 -*-

import config
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from datetime import datetime, timedelta
import mysql.connector
import json
import time
import os
import logging


class Crawler:

  #------inicializacao -------#
    def __init__(self, verbose=0):
        self.host = "localhost[%s]" % config.appName

    def gatherInfo(self, userID):
        print "iniciei parse de " + userID

        # Constr�i objeto da API com as credenciais de acesso
        api = InstagramAPI(client_id=config.clientID, client_secret=config.clientSecret)

        # Configura logging
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramFeedCrawler[%s].log" % config.appName, filemode="w", level=logging.INFO)
        requestLoggingInterval = 1000

        # Configura tratamento de exce��es
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0

        # Executa coleta
        #logging.info("Iniciando requisi��es.")
        firsRequestTime = datetime.now()
              
        while(True):
            try:
                # Executa requisi��o na API para obter dados do usu�rio
                userInfo = api.user(user_id=userID, return_json=True)
            except InstagramAPIError as err:
                # Se o usu�rio tiver o perfil privado, captura exce��o e marca erro no banco de dados
                if err.error_type == "APINotAllowedError":
                    return 4
                else:
                    # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                    # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning("Erro na chamada � API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                        continue
                    else:
                        raise
            else:
                # Cria diret�rio para armazenar feeds
                dir = os.path.abspath("../CrawledData/Feeds/%s" % userID)
                if not os.path.exists(dir):
                    os.makedirs(dir)
                               
                # Coleta feed completo do usu�rio
                j = 1
                maxID = ""
                nextUserRecentMediaPage = ""
                while (nextUserRecentMediaPage != None):
                    # Parseia url da pr�xima p�gina para extrair o max_id
                    urlParts = nextUserRecentMediaPage.split("&")
                    if len(urlParts) > 1:
                        maxID = urlParts[1].split("=")[1]
                
                    try:
                        # Executa requisi��o na API para obter m�dias do feed do usu�rio
                        userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=30, user_id=userID, max_id=maxID, return_json=True)
                    except:
                        # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                        # experimenta aguardar um certo tempo antes da pr�xima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning("Erro na chamada � API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 0
                    
                        # Salva arquivo JSON com informa��es sobre as m�dias do feed do usu�rio
                        filename = "feed%d.json" % j
                        output = open(os.path.join(dir, filename), "w")
                        json.dump(userRecentMedia, output)
                        output.close()
                        j += 1
                        
                # Salva arquivo JSON com informa��es sobre o usu�rio que postou a m�dia
                filename = os.path.abspath("../CrawledData/Users/%s.json" % userID)
                output = open(filename, "w")
                json.dump(userInfo, output)
                output.close()
                
                break
        
        return 0