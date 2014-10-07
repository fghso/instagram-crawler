# -*- coding: iso-8859-1 -*-

import os
import time
import logging


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return os.getpid()

    # Este � o m�todo que efetivamente realiza a coleta. O ID do recurso a ser coletado � recebido como par�metro e o 
    # retorno deve ser uma tupla contendo o status da coleta (qualquer valor inteiro diferente de 0 - que � usado pelo 
    # servidor para indicar que o recurso ainda n�o foi coletado - e 1 - que � usado pelo servidor para indicar que o 
    # recurso est� sendo coletado) e a quantidade coletada. Essa quantidade pode ou n�o ser pertinente, dependendo do 
    # tipo de recurso que est� sendo coletado. No caso trivial, ela usualmente � 1 ap�s uma coleta bem sucedida. Al�m
    # do ID do recurso, o m�todo recebe o valor da op��o de logging configurada no cliente (True ou False) e pode,
    # opcionalmente, receber tamb�m par�metros enviados pelo servidor
    def crawl(self, resourceID, noLogging, **kwargs):
        if (not noLogging):
            logging.info("Recurso recebido para coleta: %s" % resourceID)
            logging.info("Parametros: %s" % kwargs)

        print "Recurso recebido para coleta: %s" % resourceID        
        print "Parametros: %s" % kwargs
        
        print "Dormindo..."
        time.sleep(30)
        print "Acordado!"
        
        return (2, 1)
