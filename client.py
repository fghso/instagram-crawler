#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import logging
import argparse
import crawler


# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="mostra esta mensagem de ajuda e sai")
parser.add_argument("-v", "--verbose", action="store_true", help="exibe as mensagens de log na tela")
parser.add_argument("--no-logging", action="store_true", help="desabilita logging")
args = parser.parse_args()

# Carrega configurações
config = ET.parse(args.configFilePath)
cfgAddress = config.findtext("./connection/address")
cfgPort = int(config.findtext("./connection/port"))
cfgBufsize = int(config.findtext("./connection/bufsize"))

# Cria uma instância do coletor
crawlerObj = crawler.Crawler()

# Recebe ID do servidor
processID = os.getpid()
server = socket.socket()
server.connect((cfgAddress, cfgPort))
server.send(json.dumps({"command": "GET_LOGIN", "name": crawlerObj.getName(), "processid": processID}))
response = server.recv(cfgBufsize)
message = json.loads(response)
clientID = message["clientid"]

# Configura logging
logFile = None
if (not args.no_logging):
    logFile = "client%s[%s%s].log" % (clientID, cfgAddress, cfgPort)
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename=logFile, filemode="w", level=logging.INFO)
    logging.info("Conectado ao servidor com o ID %s " % clientID)
if (args.verbose): print "Conectado ao servidor com o ID %s " % clientID

# Executa a coleta
server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
while (True):
    try:
        response = server.recv(cfgBufsize)

        # Extrai comando recebido do servidor
        message = json.loads(response)
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Repassa o ID e parâmetros para o coletor
            resourceID = message["resourceid"]
            parameters = message["params"]
            crawlerResponse = crawlerObj.crawl(resourceID, logFile, **parameters)
            
            # Comunica ao servidor que a coleta do recurso foi finalizada
            server.send(json.dumps({"command": "DONE_ID", "clientid": clientID, "resourceid": resourceID, "status": crawlerResponse[0], "amount": crawlerResponse[1]}))

        elif (command == "DID_OK"):
            # Envia comando para receber um ID de recurso
            server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
            
        elif (command == "FINISH"):
            if (not args.no_logging): logging.info("Tarefa concluida, cliente finalizado.")
            if (args.verbose): print "Tarefa concluida, cliente finalizado."
            break
            
        elif (command == "KILL"):
            if (not args.no_logging): logging.info("Cliente removido pelo servidor.")
            if (args.verbose): print "Cliente removido pelo servidor."
            break
            
    except Exception as error:
        if (not args.no_logging): logging.exception("Excecao no processamento dos dados. Execucao abortada.")
        if (args.verbose):
            print "ERRO: %s" % str(error)
            excType, excObj, excTb = sys.exc_info()
            fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
            print (excType, fileName, excTb.tb_lineno)
        break

server.shutdown(socket.SHUT_RDWR)
server.close()
