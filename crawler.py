# -*- coding: iso-8859-1 -*-

import time
import os


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return os.getpid()

    # Este é o método que efetivamente realiza a coleta. O ID do recurso a ser coletado é recebido como parâmetro e o 
    # retorno deve ser uma tupla contendo o status da coleta (qualquer valor inteiro diferente de 0 - que é usado pelo 
    # servidor para indicar que o recurso ainda não foi coletado - e 1 - que é usado pelo servidor para indicar que o 
    # recurso está sendo coletado) e a quantidade coletada. Essa quantidade pode ou não ser pertinente, dependendo do 
    # tipo de recurso que está sendo coletado. No caso trivial, ela usualmente é 1 após uma coleta bem sucedida
    def crawl(self, resourceID):
        print "Recurso recebido para coleta: %s" % resourceID

        print "Dormindo..."
        time.sleep(60)
        print "Acordado!"
        
        return (2, 1)
