# -*- coding: iso-8859-1 -*-

import os
import time
import logging


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return os.getpid()

    # Este é o método que efetivamente realiza a coleta. O ID do recurso a ser coletado é recebido como parâmetro e o 
    # retorno deve ser uma tupla contendo o status da coleta (qualquer valor inteiro diferente de 0 - que é usado pelo 
    # servidor para indicar que o recurso ainda não foi coletado - e 1 - que é usado pelo servidor para indicar que o 
    # recurso está sendo coletado) e a quantidade coletada. Essa quantidade pode ou não ser pertinente, dependendo do 
    # tipo de recurso que está sendo coletado. No caso trivial, ela usualmente é 1 após uma coleta bem sucedida. Além
    # do ID do recurso, o método recebe o valor da opção de logging configurada no cliente (True ou False) e pode,
    # opcionalmente, receber também parâmetros enviados pelo servidor
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
