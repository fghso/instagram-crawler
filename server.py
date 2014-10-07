#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import SocketServer
import threading
import logging
import argparse
import mysql.connector


# ==================== Variáveis globais ====================
# Dicionário que guarda uma lista de informações sobre cada cliente:
#   [nome do cliente, endereço de rede, número do processo (PID), 
#   ID do recurso que está sendo coletado, quantidade de recursos já 
#   coletados pelo cliente, data de início da coleta e data da última atualização]
clientsInfo = {} 

# Armazena uma referência para a thread que executa o cliente
# e um evento que permite interromper sua execução
clientsThreads = {}

# Define próximo ID a ser passado para um novo cliente,
# bem como um lock para alteração dessa variável
nextFreeID = 1
nextFreeIDLock = threading.Lock()


# ==================== Classes ====================
class CrawlerParams():
    def getParamsDict(self):
        # Este método pode ser usado para enviar parâmetros ao coletor, caso necessário, a cada vez que um ID é solicitado. # Os parâmetros devem ser nomeados e devem estar contidos em um dicionário
        return {"paramkey": "paravalue"}


class ServerConfig():
    Address = Port = Bufsize = DBUser = DBPassword = DBHost = DBName = DBTable = ""
    
    def __init__(self, configFile):
        config = ET.parse(configFile)
        ServerConfig.Address = config.findtext("./connection/address")
        ServerConfig.Port = int(config.findtext("./connection/port"))
        ServerConfig.Bufsize = int(config.findtext("./connection/bufsize"))
        ServerConfig.DBUser = config.findtext("./database/user")
        ServerConfig.DBPassword = config.findtext("./database/password")
        ServerConfig.DBHost = config.findtext("./database/host")
        ServerConfig.DBName = config.findtext("./database/name")
        ServerConfig.DBTable = config.findtext("./database/table")

        
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Abre conexão com o banco de dados
        cfg = self.server.cfg
        self.mysqlConnection = mysql.connector.connect(user=cfg.DBUser, password=cfg.DBPassword, host=cfg.DBHost, database=cfg.DBName)

    def handle(self):
        cfg = self.server.cfg
        params = self.server.params
        client = self.request
        running = True
        while (running):
            try: 
                response = client.recv(cfg.Bufsize)
                
                # Se o cliente houver fechado a conexão do outro lado, finaliza a execução da thread
                if (not response): 
                    clientID = int(threading.current_thread().name)
                    if (not args.no_logging): logging.info("Cliente %d desconectou-se." % clientID)
                    if (args.verbose): print "Cliente %d desconectou-se." % clientID
                    running = False
                    continue

                message = json.loads(response)
                command = message["command"]
                
                if (command == "GET_LOGIN"):
                    #clientID = int(threading.current_thread().name.split('-')[1])
                    with nextFreeIDLock:
                        global nextFreeID
                        clientID = nextFreeID
                        nextFreeID += 1
                    threading.current_thread().name = clientID
                    clientName = message["name"]
                    #clientAddress = (socket.gethostbyaddr(client.getpeername()[0])[0], client.getpeername()[1])
                    clientAddress = client.getpeername()
                    clientPid = message["processid"]
                    clientsInfo[clientID] = [clientName, clientAddress, clientPid, None, 0, datetime.now(), None]
                    clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                    client.send(json.dumps({"command": "GIVE_LOGIN", "clientid": clientID}))
                    if (not args.no_logging): logging.info("Novo cliente conectado: %d" % clientID)
                    if (args.verbose): print "Novo cliente conectado: %d" % clientID
                
                elif (command == "GET_ID"):
                    clientID = message["clientid"]
                    clientStopEvent = clientsThreads[clientID][1]
                    # Se o cliente não houver sido removido, verifica disponibilidade de recurso para coleta
                    if (not clientStopEvent.is_set()):
                        clientName = clientsInfo[clientID][0]
                        resourceID = self.selectResource()
                        # Se não houver mais recursos para coletar, finaliza cliente
                        if (resourceID == None):
                            client.send(json.dumps({"command": "FINISH"}))
                            del clientsInfo[clientID]
                            running = False
                            # Se não houver mais clientes para finalizar, finaliza servidor
                            if (not clientsInfo):
                                self.server.shutdown()
                                if (not args.no_logging): logging.info("Tarefa concluida, servidor finalizado.")
                                if (args.verbose): print "Tarefa concluida, servidor finalizado."
                        # Envia ID do recurso para o cliente
                        else:
                            clientsInfo[clientID][3] = resourceID
                            clientsInfo[clientID][4] += 1
                            clientsInfo[clientID][6] = datetime.now()
                            self.updateResource(resourceID, 1, 0, clientName)
                            client.send(json.dumps({"command": "GIVE_ID", "resourceid": str(resourceID), "params": params.getParamsDict()}))
                    # Se o cliente houver sido removido, sinaliza para que ele termine
                    else:
                        client.send(json.dumps({"command": "KILL"}))
                        if (not args.no_logging): logging.info("Cliente %d removido." % clientID)
                        if (args.verbose): print "Cliente %d removido." % clientID
                        running = False
                    
                elif (command == "DONE_ID"):
                    clientID = int(message["clientid"])
                    clientName = clientsInfo[clientID][0]
                    clientResourceID = message["resourceid"]
                    clientStatus = message["status"]
                    clientAmount = message["amount"]
                    self.updateResource(clientResourceID, clientStatus, clientAmount, clientName)
                    client.send(json.dumps({"command": "DID_OK"}))
                    
                elif (command == "GET_STATUS"):
                    status = "\n" + (" Status (%s:%s/%s) " % (cfg.Address, cfg.Port, os.getpid())).center(50, ':') + "\n\n"
                    if (clientsInfo): 
                        for (clientID, clientInfo) in clientsInfo.iteritems():
                            clientName = clientInfo[0]
                            clientAddress = clientInfo[1]
                            clientPid = clientInfo[2]
                            clientResourceID = clientInfo[3]
                            clientAmount = clientInfo[4]
                            clientStartTime = clientInfo[5]
                            clientUpdatedAt = clientInfo[6]
                            elapsedTime = datetime.now() - clientStartTime
                            elapsedMinSec = divmod(elapsedTime.seconds, 60)
                            elapsedHours = (elapsedMinSec[0] // 60,)
                            status += "  #%d %s (%s:%s/%s): %s desde %s [%d coletado(s) em %s]\n" % (clientID, clientName, clientAddress[0], clientAddress[1], clientPid, clientResourceID, clientUpdatedAt.strftime("%d/%m/%Y %H:%M:%S"), clientAmount, "%02dh%02dm%02ds" % (elapsedHours + elapsedMinSec))
                    else:
                        status += "  Nenhum cliente conectado no momento.\n"
                    status += "\n" + (" Status (%.1f%% coletado) " % (self.collectedResourcesPercent())).center(50, ':') + "\n"
                    client.send(json.dumps({"command": "GIVE_STATUS", "status": status}))
                    running = False
                    
                elif (command == "RM_CLIENT"):
                    clientID = int(message["clientid"])
                    if (clientID in clientsThreads):
                        # Se a thread estava ativa, sinaliza para que ela termine de maneira segura e aguarda
                        if (clientsThreads[clientID][0].is_alive()):
                            clientsThreads[clientID][1].set()
                            while (clientsThreads[clientID][0].is_alive()): pass
                        # Se a thread não estava ativa, marca o último ID solicitado como não 
                        # coletado para que a coleta  seja refeita, garantindo a consistência
                        else:
                            clientName = clientsInfo[clientID][0]
                            clientResourceID = clientsInfo[clientID][3]
                            self.updateResource(clientResourceID, 0, 0, clientName)
                            if (not args.no_logging): logging.info("Cliente %d removido." % clientID)
                            if (args.verbose): print "Cliente %d removido." % clientID
                        del clientsInfo[clientID]
                        del clientsThreads[clientID]
                        client.send(json.dumps({"command": "RM_OK"}))
                    else:
                        client.send(json.dumps({"command": "RM_ERROR", "reason": "ID inexistente"}))
                    running = False
                        
                elif (command == "SHUTDOWN"):
                    # Sinaliza para que todos os clientes terminem e então desliga o servidor
                    if (not args.no_logging): logging.info("Removendo todos os clientes para desligar...")                    
                    if (args.verbose): print "Removendo todos os clientes para desligar..."
                    for clientID in clientsThreads.keys():
                        clientsThreads[clientID][1].set()
                    while (threading.active_count() > 2): pass
                    self.server.shutdown()    
                    client.send(json.dumps({"command": "SD_OK"}))
                    if (not args.no_logging): logging.info("Servidor desligado manualmente.")
                    if (args.verbose): print "Servidor desligado manualmente."
                    running = False
            
            except Exception as error:
                clientID = int(threading.current_thread().name)
                if (not args.no_logging): logging.exception("Excecao no processamento da requisicao do cliente %d. Thread '%s' abortada." % (clientID, threading.current_thread().name))
                if (args.verbose): 
                    print "ERRO: %s" % str(error)
                    excType, excObj, excTb = sys.exc_info()
                    fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    print (excType, fileName, excTb.tb_lineno)
                running = False
            
    # Funções de interação com o banco de dados
    def selectResource(self):
        query = "SELECT resource_id FROM " + self.server.cfg.DBTable + " WHERE status IS NULL OR status = 0 ORDER BY rand() LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resource = cursor.fetchone()
        return resource[0] if (resource != None) else resource
        
    def updateResource(self, resourceID, status, amount, crawler):
        query = "UPDATE " + self.server.cfg.DBTable + " SET status = %s, amount = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (status, amount, crawler, resourceID))
        self.mysqlConnection.commit()
        
    def collectedResourcesPercent(self):
        query = "SELECT count(resource_id) FROM " + self.server.cfg.DBTable
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesTotal = float(cursor.fetchone()[0])
        query = "SELECT count(resource_id) FROM " + self.server.cfg.DBTable + " WHERE status IS NOT NULL AND status != 0 AND status != 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesCollected = float(cursor.fetchone()[0])
        return (resourcesCollected / resourcesTotal) * 100
        
        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configuration, crawlerParameters, RequestHandlerClass):
        self.cfg = configuration
        self.params = crawlerParameters
        SocketServer.TCPServer.__init__(self, (self.cfg.Address, self.cfg.Port), RequestHandlerClass)
        
        
# ==================== Programa principal ====================

# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="mostra esta mensagem de ajuda e sai")
parser.add_argument("-v", "--verbose", action="store_true", help="exibe as mensagens de log na tela")
parser.add_argument("--no-logging", action="store_true", help="desabilita logging")
args = parser.parse_args()

# Carrega configurações
configuration = ServerConfig(args.configFilePath)

# Carrega parâmetros dos clientes
crawlerParameters = CrawlerParams()

# Configura logging
if (not args.no_logging):
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="server[%s%s].log" % (configuration.Address, configuration.Port), filemode="w", level=logging.INFO)

# Inicia servidor
if (not args.no_logging): logging.info("Servidor pronto. Aguardando conexoes...")
if (args.verbose): print "Servidor pronto. Aguardando conexoes..."
server = ThreadedTCPServer(configuration, crawlerParameters, ServerHandler)
server.serve_forever()
                