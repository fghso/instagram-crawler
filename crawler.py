# -*- coding: iso-8859-1 -*-

import time
import os


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName(self):
        return os.getpid()

    # Este � o m�todo que efetivamente realiza a coleta. O ID do recurso a ser coletado � recebido como par�metro e o 
    # retorno deve ser uma tupla contendo o status da coleta (qualquer valor inteiro diferente de 0 - que � usado pelo 
    # servidor para indicar que o recurso ainda n�o foi coletado - e 1 - que � usado pelo servidor para indicar que o 
    # recurso est� sendo coletado) e a quantidade coletada. Essa quantidade pode ou n�o ser pertinente, dependendo do 
    # tipo de recurso que est� sendo coletado. No caso trivial, ela usualmente � 1 ap�s uma coleta bem sucedida
    def crawl(self, resourceID):
        print "Recurso recebido para coleta: %s" % resourceID

        print "Dormindo..."
        time.sleep(60)
        print "Acordado!"
        
        return (2, 1)
