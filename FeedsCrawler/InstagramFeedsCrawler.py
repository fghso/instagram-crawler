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


# Constr�i objeto da API com as credenciais de acesso
api = InstagramAPI(client_id=config.clientID, client_secret=config.clientSecret)

# O limite de requisi��es estipulado nos termos de uso do Instagram � de 5000 requisi��es por hora. 
# Por garantia o n�mero m�ximo foi configurado para um valor um pouco abaixo desse limite 
maxRequestsPerHour = 4990

# # Os usu�rios s�o agrupadas em diret�rios, para facilitar a navega��o. A vari�vel 
# # bunchSize define quantos usu�rios cada diret�rio de armazenamento cont�m
# bunchSize = 5000

# Configura valores iniciais das vari�veis de controle
requestsPerHour = 0
# requestsInBunch = 1
# bunch = 1

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramFeedCrawler[%s].log" % config.appName, filemode="w", level=logging.INFO)
requestLoggingInterval = 1000

# Configura tratamento de exce��es
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Abre conex�o com o banco de dados e define queries
mysqlConnection = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
bufferedCursor = mysqlConnection.cursor(buffered=True)
cursor = mysqlConnection.cursor()
selectQuery = """SELECT * FROM users_test WHERE user_collected = 0 LIMIT 100"""
insertQuery = """INSERT INTO medias_test (media_id, media_stdres_url, media_users_test_pk) VALUES (%s, %s, %s)"""
updateQuery = """UPDATE users_test SET user_bunch = %s, user_collected = %s WHERE users_test_pk = %s""";

# Cria diret�rio para armazenar informa��es dos usu�rios
if not os.path.exists("Users"):
    os.mkdir("Users")

# Executa coleta
logging.info("Iniciando requisi��es.")
firsRequestTime = datetime.now()
while (True):
    # Recupera IDs de usu�rio a partir do banco de dados
    bufferedCursor.execute(selectQuery)
    
    for (usersTestPK, userID, userBunch, userCollected) in bufferedCursor:
        while (True):
            # Verifica se o n�mero m�ximo de requisi��es por hora foi atingido e, caso tenha sido,
            # pausa o programa por tempo suficiente para evitar infra��o dos termos de uso do Instagram
            if (requestsPerHour > maxRequestsPerHour):
                sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
                if (sleepSeconds > 0):
                    m, s = divmod(sleepSeconds, 60)
                    logging.info("Limite de requisi��es atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
                    time.sleep(sleepSeconds)
                logging.info("Reiniciando contagem de requisi��es.")
                requestsPerHour = 0
                firsRequestTime = datetime.now()
            elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
                logging.info("%d requisi��o(�es) realizada(s)." % requestsPerHour)
                
            requestsPerHour += 1
            
            try:
                # Executa requisi��o na API para obter dados do usu�rio
                userInfo = api.user(user_id=userID, return_json=True)
            except InstagramAPIError as err:
                # Se o usu�rio tiver o perfil privado, captura exce��o e marca no banco de dados
                if err.error_type == "APINotAllowedError":
                    userData = (None, 4, usersTestPK)
                    cursor.execute(updateQuery, userData)
                    mysqlConnection.commit()
                    break
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
                        logging.exception("Erro na chamada � API. Execu��o abortada.")
                        raise
            else:      
                retrys = 0
                sleepSecondsMultiply = 0
            
                # Cria diret�rio para armazenar feeds
                # dir = "bunch%s/%s" % (bunch, userID)
                dir = "Feeds/%s" % userID
                if not os.path.exists(dir):
                    os.makedirs(dir)
                               
                # Coleta feed completo do usu�rio
                j = 1
                maxID = ""
                nextUserRecentMediaPage = ""
                while (nextUserRecentMediaPage != None):
                    # Verifica se o n�mero m�ximo de requisi��es por hora foi atingido e, caso tenha sido,
                    # pausa o programa por tempo suficiente para evitar infra��o dos termos de uso do Instagram
                    if (requestsPerHour > maxRequestsPerHour):
                        sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
                        if (sleepSeconds > 0):
                            m, s = divmod(sleepSeconds, 60)
                            logging.info("Limite de requisi��es atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
                            time.sleep(sleepSeconds)
                        logging.info("Reiniciando contagem de requisi��es.")
                        requestsPerHour = 0
                        firsRequestTime = datetime.now()
                    elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
                        logging.info("%d requisi��o(�es) realizada(s)." % requestsPerHour)
                
                    requestsPerHour += 1
                    
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
                            logging.exception("Erro na chamada � API. Execu��o abortada.")
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
                        
                        # Salva informa��es b�sicas sobre cada m�dia no banco de dados
                        for media in userRecentMedia:
                            if media["type"] == "image":
                                mediaData = (media["id"], media["images"]["standard_resolution"]["url"],  usersTestPK)
                                cursor.execute(insertQuery, mediaData)
                                mysqlConnection.commit()
                
                # Salva arquivo JSON com informa��es sobre o usu�rio que postou a m�dia
                filename = "Users/%s.json" % userID
                output = open(filename, "w")
                json.dump(userInfo, output)
                output.close()
                
                # Marca usu�rio como coletado no banco de dados
                userData = (None, 1, usersTestPK)
                cursor.execute(updateQuery, userData)
                mysqlConnection.commit()
                
                # # Verifica se o n�mero m�ximo de requisi��es no bunch atual foi atingido
                # requestsInBunch += 1
                # if (requestsInBunch > bunchSize):
                    # requestsInBunch = 1
                    # bunch += 1
                
                # Passa para a pr�xima coleta
                break
