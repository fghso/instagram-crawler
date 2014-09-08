#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import socket
import json
import xml.etree.ElementTree as ET
import argparse


# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="mostra esta mensagem de ajuda e sai")
parser.add_argument("-s", "--status", action="store_true", help="obtem o status atual de todos os clientes conectados ao servidor")
parser.add_argument("-r", "--remove", metavar="clientID", help="remove da lista do servidor o cliente com o ID especificado")
parser.add_argument("--shutdown", action="store_true", help="remove todos os clientes da lista do servidor e o desliga")
args = parser.parse_args()

# Carrega configurações
config = ET.parse(args.configFilePath)
cfgAddress = config.findtext("./connection/address")
cfgPort = int(config.findtext("./connection/port"))
cfgBufsize = int(config.findtext("./connection/bufsize"))

# Conecta-se ao servidor
try:
    server = socket.socket()
    server.connect((cfgAddress, cfgPort))
except: 
    sys.exit("ERRO: Nao foi possivel conectar-se ao servidor em %s:%s." % (cfgAddress, cfgPort))

# Remove cliente
if (args.remove):
    server.send(json.dumps({"command": "RM_CLIENT", "clientid": args.remove}))
    response = server.recv(cfgBufsize)
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    
    # Extrai comando recebido do servidor
    message = json.loads(response)
    command = message["command"]
    
    if (command == "RM_OK"):
        print "Cliente %s removido com sucesso." % args.remove
    elif (command == "RM_ERROR"):
        print "ERRO: %s." % message["reason"] 
    
# Desliga servidor
elif (args.shutdown):   
    server.send(json.dumps({"command": "SHUTDOWN"}))
    response = server.recv(cfgBufsize)
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    
    # Extrai comando recebido do servidor
    message = json.loads(response)
    command = message["command"]
    
    if (command == "SD_OK"):
        print "Servidor desligado com sucesso."
        
# Imprime status
else:
    server.send(json.dumps({"command": "GET_STATUS"}))
    response = server.recv(cfgBufsize)
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    
    # Extrai comando recebido do servidor
    message = json.loads(response)
    command = message["command"]
    
    if (command == "GIVE_STATUS"):
        print message["status"] 
