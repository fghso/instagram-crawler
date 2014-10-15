#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import json
from datetime import datetime
import SocketServer
import threading
import logging
import argparse
import mysql.connector
from common import *


# ==================== Variáveis globais ====================
# Dicionário que guarda uma lista de informações sobre cada cliente:
#   [nome do cliente, endereço de rede, número do processo (PID), 
#   ID do recurso que está sendo coletado, quantidade de recursos já 
#   coletados pelo cliente, data de início da coleta e data da última atualização]
clientsInfo = {} 

# Armazena uma referência para a thread que executa o cliente
# e um evento que permite interromper sua execução
clientsThreads = {}

# Define próximo ID a ser passado para um novo cliente
nextFreeID = 1

# Define locks para regiões críticas do código
nextFreeIDLock = threading.Lock()
getIDLock = threading.Lock()


# ==================== Classes ====================
class CrawlerParams():
    def getParams(self):
        # Este método pode ser usado para enviar parâmetros ao coletor, caso necessário, a cada vez que um ID é solicitado. # Os parâmetros devem ser nomeados e devem estar contidos em um dicionário
        return {"paramkey": "paravalue"}


class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Abre conexão com o banco de dados
        config = self.server.config
        self.mysqlConnection = mysql.connector.connect(user=config["persistence"]["database"]["user"], password=config["persistence"]["database"]["password"], host=config["persistence"]["database"]["host"], database=config["persistence"]["database"]["name"])

    def handle(self):
        config = self.server.config
        params = self.server.params
        client = self.request
        clientID = 0
        running = True
        while (running):
            try: 
                response = client.recv(config["global"]["connection"]["bufsize"])
                
                # Se o cliente houver fechado a conexão do outro lado, finaliza a execução da thread
                if (not response): 
                    if (config["server"]["logging"]): logging.info("Cliente %d desconectou-se." % clientID)
                    if (config["server"]["verbose"]): print "Cliente %d desconectou-se." % clientID
                    running = False
                    continue

                message = json.loads(response)
                command = message["command"]
                
                if (command == "GET_LOGIN"):
                    with nextFreeIDLock:
                        global nextFreeID
                        clientID = nextFreeID
                        nextFreeID += 1
                    clientName = message["name"]
                    #clientAddress = (socket.gethostbyaddr(client.getpeername()[0])[0], client.getpeername()[1])
                    clientAddress = client.getpeername()
                    clientPid = message["processid"]
                    clientsInfo[clientID] = [clientName, clientAddress, clientPid, None, 0, datetime.now(), None]
                    clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                    client.send(json.dumps({"command": "GIVE_LOGIN", "clientid": clientID}))
                    if (config["server"]["logging"]): logging.info("Novo cliente conectado: %d" % clientID)
                    if (config["server"]["verbose"]): print "Novo cliente conectado: %d" % clientID
                
                elif (command == "GET_ID"):
                    clientStopEvent = clientsThreads[clientID][1]
                    # Se o cliente não houver sido removido, verifica disponibilidade de recurso para coleta
                    if (not clientStopEvent.is_set()):
                        clientName = clientsInfo[clientID][0]
                        with getIDLock:
                            resourceID = self.selectResource()
                            if (resourceID): self.updateResource(resourceID, 1, 0, clientName)
                        # Se houver recurso disponível, envia o ID para o cliente
                        if (resourceID):
                            clientsInfo[clientID][3] = resourceID
                            clientsInfo[clientID][4] += 1
                            clientsInfo[clientID][6] = datetime.now()
                            client.send(json.dumps({"command": "GIVE_ID", "resourceid": str(resourceID), "params": params.getParams()}))
                        # Se não houver mais recursos para coletar, finaliza cliente
                        else:
                            client.send(json.dumps({"command": "FINISH"}))
                            del clientsInfo[clientID]
                            running = False
                            # Se não houver mais clientes para finalizar, finaliza servidor
                            if (not clientsInfo):
                                self.server.shutdown()
                                if (config["server"]["logging"]): logging.info("Tarefa concluida, servidor finalizado.")
                                if (config["server"]["verbose"]): print "Tarefa concluida, servidor finalizado."
                    # Se o cliente houver sido removido, sinaliza para que ele termine
                    else:
                        client.send(json.dumps({"command": "KILL"}))
                        del clientsInfo[clientID]
                        if (config["server"]["logging"]): logging.info("Cliente %d removido." % clientID)
                        if (config["server"]["verbose"]): print "Cliente %d removido." % clientID
                        running = False
                    
                elif (command == "DONE_ID"):
                    clientName = clientsInfo[clientID][0]
                    clientResourceID = message["resourceid"]
                    clientStatus = message["status"]
                    #clientAnnotation = message["annotation"]
                    clientAmount = message["amount"]
                    self.updateResource(clientResourceID, clientStatus, clientAmount, clientName)
                    client.send(json.dumps({"command": "DID_OK"}))
                    
                elif (command == "GET_STATUS"):
                    status = "\n" + (" Status (%s:%s/%s) " % (config["global"]["connection"]["address"], config["global"]["connection"]["port"], os.getpid())).center(50, ':') + "\n\n"
                    if (clientsInfo): 
                        for (ID, clientInfo) in clientsInfo.iteritems():
                            clientAlive = (" " if clientsThreads[ID][0].is_alive() else "+")
                            clientName = clientInfo[0]
                            clientAddress = clientInfo[1]
                            clientPid = clientInfo[2]
                            clientResourceID = clientInfo[3]
                            clientAmount = clientInfo[4]
                            clientStartTime = clientInfo[5]
                            clientUpdatedAt = clientInfo[6]
                            elapsedTime = datetime.now() - clientStartTime
                            elapsedMinSec = divmod(elapsedTime.seconds, 60)
                            elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                            status += "  #%d %s %s (%s:%s/%s): %s desde %s [%d coletado(s) em %s]\n" % (ID, clientAlive, clientName, clientAddress[0], clientAddress[1], clientPid, clientResourceID, clientUpdatedAt.strftime("%d/%m/%Y %H:%M:%S"), clientAmount, "%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1]))
                    else:
                        status += "  Nenhum cliente conectado no momento.\n"
                    status += "\n" + (" Status (%.1f%% coletado) " % (self.collectedResourcesPercent())).center(50, ':') + "\n"
                    client.send(json.dumps({"command": "GIVE_STATUS", "status": status}))
                    running = False
                    
                elif (command == "RM_CLIENT"):
                    ID = int(message["clientid"])
                    if (ID in clientsThreads):
                        # Se a thread estava ativa, sinaliza para que ela termine de maneira segura e aguarda
                        if (clientsThreads[ID][0].is_alive()):
                            clientsThreads[ID][1].set()
                            while (clientsThreads[ID][0].is_alive()): pass
                        # Se a thread não estava ativa, marca o último ID solicitado como não 
                        # coletado para que a coleta seja refeita, garantindo a consistência
                        else:
                            clientName = clientsInfo[ID][0]
                            clientResourceID = clientsInfo[ID][3]
                            self.updateResource(clientResourceID, 0, None, clientName)
                            if (config["server"]["logging"]): logging.info("Cliente %d removido." % ID)
                            if (config["server"]["verbose"]): print "Cliente %d removido." % ID
                        del clientsThreads[ID]
                        client.send(json.dumps({"command": "RM_OK"}))
                    else:
                        client.send(json.dumps({"command": "RM_ERROR", "reason": "ID inexistente"}))
                    running = False
                        
                elif (command == "SHUTDOWN"):
                    # Sinaliza para que todos os clientes ativos terminem e marca recursos dos clientes inativos
                    # como não coletados. Em seguida, desliga o servidor
                    if (config["server"]["logging"]): logging.info("Removendo todos os clientes para desligar...")
                    if (config["server"]["verbose"]): print "Removendo todos os clientes para desligar..."
                    for ID in clientsThreads.keys():
                        if (clientsThreads[ID][0].is_alive()):
                            clientsThreads[ID][1].set()
                        else:
                            clientName = clientsInfo[ID][0]
                            clientResourceID = clientsInfo[ID][3]
                            self.updateResource(clientResourceID, 0, None, clientName)
                    while (threading.active_count() > 2): pass
                    self.server.shutdown()    
                    client.send(json.dumps({"command": "SD_OK"}))
                    if (config["server"]["logging"]): logging.info("Servidor desligado manualmente.")
                    if (config["server"]["verbose"]): print "Servidor desligado manualmente."
                    running = False
            
            except Exception as error:
                if (config["server"]["logging"]): logging.exception("Excecao no processamento da requisicao do cliente %d. Thread '%s' abortada." % (clientID, threading.current_thread().name))
                if (config["server"]["verbose"]): 
                    print "ERRO: %s" % str(error)
                    excType, excObj, excTb = sys.exc_info()
                    fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    print (excType, fileName, excTb.tb_lineno)
                running = False
            
    # Funções de interação com o banco de dados
    def selectResource(self):
        query = "SELECT resource_id FROM " + self.server.config["persistence"]["database"]["table"] + " WHERE status IS NULL OR status = 0 ORDER BY rand() LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resource = cursor.fetchone()
        return resource[0] if (resource) else resource
        
    def updateResource(self, resourceID, status, amount, crawler):
        query = "UPDATE " + self.server.config["persistence"]["database"]["table"] + " SET status = %s, amount = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (status, amount, crawler, resourceID))
        self.mysqlConnection.commit()
        
    def collectedResourcesPercent(self):
        query = "SELECT count(resource_id) FROM " + self.server.config["persistence"]["database"]["table"]
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesTotal = float(cursor.fetchone()[0])
        query = "SELECT count(resource_id) FROM " + self.server.config["persistence"]["database"]["table"] + " WHERE status IS NOT NULL AND status != 0 AND status != 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesCollected = float(cursor.fetchone()[0])
        return (resourcesCollected / resourcesTotal) * 100
        
        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configuration, crawlerParameters, RequestHandlerClass):
        self.config = configuration
        self.params = crawlerParameters
        SocketServer.TCPServer.__init__(self, (self.config["global"]["connection"]["address"], self.config["global"]["connection"]["port"]), RequestHandlerClass)
        
        
# ==================== Programa principal ====================

# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=str2bool, metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Carrega configurações
config = ConfigurationHandler(args.configFilePath).getConfig()
if (args.verbose is not None): config["server"]["verbose"] = args.verbose
if (args.logging is not None): config["server"]["logging"] = args.logging

# Carrega parâmetros dos clientes
crawlerParameters = CrawlerParams()

# Configura logging
if (config["server"]["logging"]):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="server[%s%s].log" % (config["global"]["connection"]["address"], config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)

# Inicia servidor
if (config["server"]["logging"]): logging.info("Servidor pronto. Aguardando conexoes...")
if (config["server"]["verbose"]): print "Servidor pronto. Aguardando conexoes..."
server = ThreadedTCPServer(config, crawlerParameters, ServerHandler)
server.serve_forever()
                