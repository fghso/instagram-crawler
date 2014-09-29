instagram-crawler (camps-crawler)
=====

Os arquivos python no diret�rio raiz deste projeto definem o coletor gen�rico, denominado *camps-crawler*. Os demais diret�rios cont�m arquivos referentes � coleta de dados espec�fica do Instagram, cujo projeto foi denominado *instagram-crawler*.

Para utilizar o coletor gen�rico, basta executar o servidor (`server.py`) e o cliente (`client.py`) em qualquer m�quina. O servidor espera receber como entrada o caminho para um arquivo de configura��o XML, que deve estar de acordo com o formato abaixo:

```xml
<?xml version="1.0" encoding="ISO8859-1" ?>
<config>
    <connection>
        <address>server_address</address>
        <port>port_number</port>
        <bufsize>buffer_size</bufsize>
    </connection>
    <database>
        <user>db_user</user>
        <password>db_password</password>
        <host>db_host</host>
        <name>db_name</name>
        <table>db_table_name</table>
    </database>
</config>
```

Al�m do endere�o e porta de conex�o, o arquivo de configura��o especifica o banco de dados MySQL que ser� usado para obter as informa��es dos recursos a serem coletados. � necess�rio designar a tabela onde essas informa��es est�o armazenadas. A tabela deve possuir os seguintes campos:

| resource_id  | status | amount | crawler | updated_at |
| ------------ | ------ | ------ | ------- | ---------- |

Abaixo est� um exemplo de como essa tabela pode ser criada:

```sql
CREATE TABLE `resources` (
  `resources_pk` int(10) NOT NULL AUTO_INCREMENT,
  `resource_id` varchar(45) NOT NULL,
  `status` tinyint(5) NOT NULL DEFAULT '0',
  `amount` int(10) unsigned NOT NULL DEFAULT '0',
  `crawler` varchar(45) DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`resources_pk`),
  UNIQUE KEY `resource_id_UNIQUE` (`resource_id`)
);
```

O servidor espera que a tabela j� esteja populada com os IDs dos recursos quando o programa for iniciado, isto �, a tabela deve ser populada antes de iniciar a coleta. Os IDs podem ser qualquer informa��o que fa�a sentido para o c�digo que efetivamente far� a coleta (podem ser, por exemplo, identificadores de usu�rios, hashtags, URLs, etc.). O servidor apenas gerencia qual ID est� designado para cada cliente.

Assim como o servidor, o cliente espera receber como entrada o caminho para um arquivo de configura��o XML, tamb�m de acordo com o formato descrito acima. No caso do cliente, por�m, apenas as informa��es de conex�o s�o necess�rias. O endere�o e porta informados devem ser os do servidor.

No mesmo diret�rio de execu��o do cliente deve existir um arquivo com o nome `crawler.py`. Esse � o arquivo que conter� o c�digo de coleta. O arquivo deve seguir o template contido no `crawler.py` que est� no reposit�rio. 


Gerenciamento
-----

Para saber a situa��o atual da coleta, � poss�vel utilizar o programa `manager.py`. Ele deve ser chamado de maneira similar ao cliente, designando-se o caminho para o arquivo de configura��o XML que cont�m as informa��es de conex�o com o servidor.

Os seguintes argumentos opcionais est�o dispon�veis para esse programa:

```
-h, --help                      Mostra a mensagem de ajuda e sai
-s, --status                    Obt�m o status atual de todos os clientes conectados ao servidor
-r clientID, --remove clientID  Remove da lista do servidor o cliente com o ID especificado
--shutdown                      Remove todos os clientes da lista do servidor e o desliga
```

A pen�ltima op��o (`-r clientID, --remove clientID`) permite excluir um cliente da lista do servidor informando seu ID. O cliente pode estar ativo ou inativo quando for exclu�do (se estiver ativo, assim que fizer um novo contato com o servidor ir� receber deste uma mensagem para que termine sua execu��o). De maneira semelhante, no caso da �ltima op��o (`--shutdown`) o servidor notifica todos os clientes ativos para que terminem sua execu��o antes que o pr�prio servidor seja desligado.

Se nenhum argumento opcional for informado, o programa, por padr�o, exibe o status.


Fluxo b�sico
-----

De maneira suscinta, o fluxo de funcionamento do coletor � o seguinte: 

1. Inicia-se o servidor
2. O servidor aguarda novas conex�es de clientes
3. Inicia-se o cliente
4. O cliente conversa com o servidor, identifica-se e, em seguida, pede um novo ID para coletar
5. O servidor procura um ID ainda n�o coletado no banco de dados e o repassa ao cliente, marcando esse ID com o status *sendo coletado*
6. O cliente recebe o ID e chama a fun��o `crawl(resourceID)`, da classe `Crawler`, que deve estar contida no arquivo `crawler.py`
7. A fun��o `crawl(resourceID)` faz a coleta do recurso e retorna o status e a quantidade para o cliente
8. O cliente repassa esses valores para o servidor
9. O servidor marca o recurso com o valor de status recebido do cliente e armazena tamb�m o valor de quantidade informado
10. O cliente pede um novo ID ao servidor para coletar e o fluxo retorna ao passo 5

Como a ideia do coletor � proporcionar uma coleta distribu�da, pode-se iniciar v�rios clientes simultaneamente, a partir de qualquer m�quina na mesma rede do servidor.
    