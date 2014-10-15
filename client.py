#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import json
from datetime import datetime
import logging
import argparse
from common import *
import crawler


# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=str2bool, metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Carrega configurações
config = ConfigurationHandler(args.configFilePath).getConfig()
if (args.verbose is not None): config["client"]["verbose"] = args.verbose
if (args.logging is not None): config["client"]["logging"] = args.logging

# Cria uma instância do coletor
crawlerObj = crawler.Crawler()

# Recebe ID do servidor
processID = os.getpid()
server = socket.socket()
server.connect((config["global"]["connection"]["address"], config["global"]["connection"]["port"]))
server.send(json.dumps({"command": "GET_LOGIN", "name": crawlerObj.getName(), "processid": processID}))
response = server.recv(config["global"]["connection"]["bufsize"])
message = json.loads(response)
clientID = message["clientid"]

# Configura logging
if (config["client"]["logging"]):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="client%s[%s%s].log" % (clientID, config["global"]["connection"]["address"], config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)
    logging.info("Conectado ao servidor com o ID %s " % clientID)
if (config["client"]["verbose"]): print "Conectado ao servidor com o ID %s " % clientID

# Executa a coleta
server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
while (True):
    try:
        response = server.recv(config["global"]["connection"]["bufsize"])

        # Extrai comando recebido do servidor
        message = json.loads(response)
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Repassa o ID e parâmetros para o coletor
            resourceID = message["resourceid"]
            parameters = message["params"]
            crawlerResponse = crawlerObj.crawl(resourceID, config["client"]["logging"], **parameters)
            
            # Comunica ao servidor que a coleta do recurso foi finalizada
            server.send(json.dumps({"command": "DONE_ID", "clientid": clientID, "resourceid": resourceID, "status": crawlerResponse[0], "amount": crawlerResponse[1]}))

        elif (command == "DID_OK"):
            # Envia comando para receber um ID de recurso
            server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
            
        elif (command == "FINISH"):
            if (config["client"]["logging"]): logging.info("Tarefa concluida, cliente finalizado.")
            if (config["client"]["verbose"]): print "Tarefa concluida, cliente finalizado."
            break
            
        elif (command == "KILL"):
            if (config["client"]["logging"]): logging.info("Cliente removido pelo servidor.")
            if (config["client"]["verbose"]): print "Cliente removido pelo servidor."
            break
            
    except Exception as error:
        if (config["client"]["logging"]): logging.exception("Excecao no processamento dos dados. Execucao abortada.")
        if (config["client"]["verbose"]):
            print "ERRO: %s" % str(error)
            excType, excObj, excTb = sys.exc_info()
            fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
            print (excType, fileName, excTb.tb_lineno)
        break

server.shutdown(socket.SHUT_RDWR)
server.close()
