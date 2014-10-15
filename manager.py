#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import socket
import json
import argparse
import common


# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False, description="Send action commands to be performed by the server or retrieve status information. If none of the optional arguments are given, basic status information is shown.")
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-e", "--extended", action="store_true", help="show extended status information")
parser.add_argument("-r", "--remove", metavar="clientID", help="remove the client specified by the given ID from the server's list")
parser.add_argument("--shutdown", action="store_true", help="remove all clients from the server's list and shutdown server")
args = parser.parse_args()

# Carrega configurações
config = common.ConfigurationHandler(args.configFilePath).getConfig()

# Conecta-se ao servidor
try:
    server = socket.socket()
    server.connect((config["global"]["connection"]["address"], config["global"]["connection"]["port"]))
except: 
    sys.exit("ERRO: Nao foi possivel conectar-se ao servidor em %s:%s." % (config["global"]["connection"]["address"], config["global"]["connection"]["port"]))

# Remove cliente
if (args.remove):
    server.send(json.dumps({"command": "RM_CLIENT", "clientid": args.remove}))
    response = server.recv(config["global"]["connection"]["bufsize"])
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
    response = server.recv(config["global"]["connection"]["bufsize"])
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
    response = server.recv(config["global"]["connection"]["bufsize"])
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    
    # Extrai comando recebido do servidor
    message = json.loads(response)
    command = message["command"]
    
    if (command == "GIVE_STATUS"):
        print message["status"] 
