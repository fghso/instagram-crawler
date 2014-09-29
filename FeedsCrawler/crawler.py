# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import logging
from datetime import datetime
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
import app


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return socket.gethostname()

    # Valores de retorno:
    #    2 => Coleta bem sucedida
    #   -2 => APINotAllowedError - you cannot view this resource
    def crawl(self, resourceID):
        # Constrói objeto da API com as credenciais de acesso
        api = InstagramAPI(client_id=app.clientID, client_secret=app.clientSecret)

        # Configura logging
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramFeedsCrawler[%s%s].log" % (socket.gethostname(), os.getpid()), filemode="w", level=logging.INFO)

        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Configura diretórios base para armazenamento
        usersDataDir = "../../CrawledData/Users"
        feedsDataDir = "../../CrawledData/Feeds"
        
        # Executa coleta
        firsRequestTime = datetime.now()
        while(True):
            try:
                # Executa requisição na API para obter dados do usuário
                userInfo = api.user(user_id=resourceID, return_json=True)
            except InstagramAPIError as err:
                # Se o usuário tiver o perfil privado, captura exceção e marca erro no banco de dados
                if err.error_type == "APINotAllowedError":
                    return (-2, 0)
                else:
                    # Caso o número de tentativas não tenha ultrapassado o máximo,
                    # experimenta aguardar um certo tempo antes da próxima tentativa 
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        logging.warning("Erro na chamada à API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                    else:
                        raise
            else:
                # Salva arquivo JSON com informações sobre o usuário
                filename = os.path.join(usersDataDir, "%s.json" % resourceID)
                output = open(filename, "w")
                json.dump(userInfo, output)
                output.close()
            
                # Cria diretório para armazenar feeds
                dir = os.path.join(feedsDataDir, resourceID)
                if not os.path.exists(dir):
                    os.makedirs(dir)
                               
                # Coleta feed completo do usuário
                j = 0
                maxID = ""
                nextUserRecentMediaPage = ""
                while (nextUserRecentMediaPage != None):
                    # Parseia url da próxima página para extrair o max_id
                    urlParts = nextUserRecentMediaPage.split("&")
                    if len(urlParts) > 1:
                        maxID = urlParts[1].split("=")[1]
                
                    try:
                        # Executa requisição na API para obter mídias do feed do usuário
                        userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=30, user_id=resourceID, max_id=maxID, return_json=True)
                    except:
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning("Erro na chamada à API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 0
                    
                        # Salva arquivo JSON com informações sobre as mídias do feed do usuário
                        j += 1
                        filename = "feed%d.json" % j
                        output = open(os.path.join(dir, filename), "w")
                        json.dump(userRecentMedia, output)
                        output.close()
        
                return (2, j)
