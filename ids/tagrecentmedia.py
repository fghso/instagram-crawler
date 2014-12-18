#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import time
import logging
from datetime import datetime, timedelta
from instagram.client import InstagramAPI
from mysql.connector.errors import Error
from mysql.connector import errorcode
import mysql.connector
import config


# Constrói objeto da API com as credenciais de acesso
api = InstagramAPI(client_id=config.clientID, client_secret=config.clientSecret)

# O limite de requisições estipulado nos termos de uso do Instagram é de 5000 requisições por hora. 
# Por garantia o número máximo foi configurado para um valor um pouco abaixo desse limite 
maxRequestsPerHour = 4990

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramTagIDsCrawler[%s].log" % config.appName, filemode="w", level=logging.INFO)
requestLoggingInterval = 1000

# Configura tratamento de exceções
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Abre conexão com o banco de dados e define query de inserção
mysqlConnection = mysql.connector.connect(user=config.dbUser, password=config.dbPassword, host=config.dbHost, database=config.dbName)
cursor = mysqlConnection.cursor()
query = """INSERT INTO users_test (user_id) VALUES (%s)"""

# Executa coleta
logging.info("Iniciando requisições.")
requestsPerHour = 0
firsRequestTime = datetime.now()
while (True):
    # Verifica se o número máximo de requisições por hora foi atingido e, caso tenha sido,
    # pausa o programa por tempo suficiente para evitar infração dos termos de uso do Instagram
    if (requestsPerHour > maxRequestsPerHour):
        sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
        if (sleepSeconds > 0):
            m, s = divmod(sleepSeconds, 60)
            logging.info("Limite de requisições atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
            time.sleep(sleepSeconds)
        logging.info("Reiniciando contagem de requisições.")
        requestsPerHour = 0
        firsRequestTime = datetime.now()
    elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
        logging.info("%d requisição(ões) realizada(s)." % requestsPerHour)
        
    requestsPerHour += 1

    try:
        # Executa requisição na API para obter mídias recentes com a tag selfie
        selfieRecentMedia, nextSelfieRecentMediaPage = api.tag_recent_media(count=35, tag_name="selfie")
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
            logging.exception("Erro na chamada à API. Execução abortada.")
            raise
    else:
        retrys = 0
        sleepSecondsMultiply = 0
    
        # Itera pela lista de mídias armazenando no banco de dados o ID dos usuários que as postaram
        for media in selfieRecentMedia:
            try:
                cursor.execute(query, (media.user.id,))
                mysqlConnection.commit()
            except mysql.connector.Error as err:
                if err.errno != errorcode.ER_DUP_ENTRY:
                    logging.exception("Erro na execução da query.")
                  