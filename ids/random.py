#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os
import time
import json
import logging
import random
from datetime import datetime, timedelta
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from mysql.connector.errors import Error
from mysql.connector import errorcode
import mysql.connector
import config


# Constrói objeto da API com as credenciais de acesso
api = InstagramAPI(client_id=config.clientID, client_secret=config.clientSecret)

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramRandomIDsCrawler[%s].log" % config.name, filemode="w", level=logging.INFO)

# Configura tratamento de exceções
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Abre conexão com o banco de dados e define query de inserção
mysqlConnection = mysql.connector.connect(user=config.dbUser, password=config.dbPassword, host=config.dbHost, database=config.dbName)
cursor = mysqlConnection.cursor()
query = """INSERT INTO users_to_collect (resource_id) VALUES (%s)"""

# Configura diretório base para armazenamento
usersDataDir = "data/users"

# Executa coleta
while (True):
    userID = random.randint(1, 2000000000)
    try:
        # Executa requisição na API para obter dados do usuário
        userInfo = api.user(user_id=userID, return_json=True)
    except InstagramAPIError as err:
        # Se o usuário tiver o perfil privado ou inexistente, descarta o ID
        if (err.error_type == "APINotAllowedError") or (err.error_type == "APINotFoundError"):
            pass
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
        if not os.path.exists(usersDataDir): os.makedirs(usersDataDir)
        filename = os.path.join(usersDataDir, "%s.user" % userID)
        output = open(filename, "w")
        json.dump(userInfo, output)
        output.close()
    
        # Salva ID do usuário no banco
        try:
            cursor.execute(query, (userID,))
            mysqlConnection.commit()
        except mysql.connector.Error as err:
            if err.errno != errorcode.ER_DUP_ENTRY:
                logging.exception("Erro na execução da query.")
                    
    # Caso o número máximo de requisições por hora tenha sido atingido, pausa o 
    # programa durante algum tempo para que a taxa possa ser reestabelecida
    # if (api.x_ratelimit_remaining > 0) or (api.x_ratelimit_remaining is None):
        # pass
    # else: 
        # sleepInterval = 300
        # logging.info("Limite de requisições atingido. Dormindo agora por %d minuto(s)..." % sleepInterval)
        # time.sleep(sleepInterval)
        