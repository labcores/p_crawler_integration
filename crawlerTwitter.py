# -*- coding: utf-8 -*-
import re
import urllib2
import oauth2 as oauth
import pymongo
import json
import time
import codecs
import sys
import datetime
#import tweepy

def logError(log, msg):
    log.write(msg+"\n")
    

def salvarArquivo(data, destino, log):
    #for registro in data:
    #print "escreveu no arquivo"
    data = json.loads(data)
    #print data
    for registro in data:
        
        try:
            #print "vaiEscreverNoArquivo"
            destino.write(json.dumps(registro)+"\n")
            #print "Escreveu"
            #destino.flush()
            #print "flush"
        except:
            msg = "Ao Salvar o Arquivo"
            logError(log, msg)
            print "Unexpected error:" + str(sys.exc_info()[0])
            

def pegarTweets(ids, destino, log):

    listaIds = ""
    URL = "https://api.twitter.com/1.1/statuses/lookup.json?id="
    for j in ids:
        listaIds += str(j)+","

    listaIds = listaIds[:-1]#remover ultima virgula
    URL += listaIds
    #print "vaiEnviarReqisição"
    response,data = client.request(URL,"GET")

    #print "enviouRequisiçãoVaiSalvar"
    salvarArquivo(data, destino,log)
    #print "Salvou"


def coletarTweetsAntigos(arqIDs, destino, log):
    ids = []

    arqIDs = ["577982721596723200"]
    for i in arqIDs:
        
        #i = i.replace("\n","")
        #i = i.replace("\r","")
        i = re.sub("[^0-9]","",i)


        ids.append(i)
        
            #URL = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=tcruzfranca&count=1"
        if len(ids) == 100:
            try:
                print "pegaTweets"
                pegarTweets(ids,destino,log)
                print "depoisPegarTweets"
                time.sleep(5)                        
            except:
                msg = "Erro ao coletar."                
                logErro(log,msg)
                print "Unexpected error:"+ str(sys.exc_info()[0])
                print "dormiu"
                time.sleep(60*15)
                pegarTweets(ids,destino,log)
            finally:
                ids = []
    else:
        pegarTweets(ids,destino, log)

def twitterRateInfo(client):

    '''
        Metodo esta funcional, mas precisa ser adaptado caso se deseje algo específico.
        Quando fiz o metodo o objetivo era pegar informacoes sobre limite de requisicao para o crawler (max permitido e quantos ainda sao possiveis dentro de uma janela - o que muda a cada servico). Mas percebi que e possivel pegar essas informacoes no cabecalho de cada requisicao enviada mesmo que nao seja para esse metodo. Ex: se estiver usando o Twitter API da pra saber todas as informacoes desse servico pelo cabecalho da resposta para o recurso solicitado.
    '''
    urlRateLimit = "https://api.twitter.com/1.1/application/rate_limit_status.json?resources=search"       
    response = client.request(urlRateLimit,"GET")
    
    #pegando a hora no Twitter
    cabecalho = response[0]
    cabecalho = json.loads(json.dumps(cabecalho))
    horaDaConsulta = cabecalho['date']
    
    response = response[1]
    response = json.loads(response)#neste caso nao precisa do json.dumps necessario quando o conteudo nao e uma string
    
    requisicoesPossiveisNaJanela = int(response['resources']['search']['/search/tweets']['remaining'])
    fimDaJanelaDeBuffer = float(response['resources']['search']['/search/tweets']['reset'])
    horaDaConsulta = datetime.datetime.strptime(horaDaConsulta,'%a, %d %b %Y %H:%M:%S %Z')#definindo formato da daa
    horaDaConsulta = horaDaConsulta.strftime("%s")#convertendo para milliseconds, retorna uma string
    horaDaConsulta = float(horaDaConsulta)
    
    fimDaJanelaDeBuffer = fimDaJanelaDeBuffer - horaDaConsulta
    fimDaJanelaDeBuffer = round(fimDaJanelaDeBuffer/1000.0)

    print "requisicoesPossiveisNaJanela:",requisicoesPossiveisNaJanela
    print 'fimDaJanelaDeBuffer:',fimDaJanelaDeBuffer
   
def getResponseInformations(cabecalho):
    cabecalho = json.loads(json.dumps(cabecalho))

    responseStatus = cabecalho['status']
    data = cabecalho['date']
    requisicoesPossiveisNaJanela = cabecalho['x-rate-limit-remaining']      
    fimDaJanelaDeBuffer = cabecalho['x-rate-limit-reset']

    horaDaConsulta = datetime.datetime.strptime(data,'%a, %d %b %Y %H:%M:%S %Z')#definindo formato da daa
    horaDaConsulta = horaDaConsulta.strftime("%s")#convertendo para milliseconds, retorna uma string
    horaDaConsulta = float(horaDaConsulta)
    fimDaJanelaDeBuffer = float(fimDaJanelaDeBuffer) - horaDaConsulta
    
    fimDaJanelaDeBuffer = round(fimDaJanelaDeBuffer/1000.0)

    return (responseStatus,requisicoesPossiveisNaJanela,fimDaJanelaDeBuffer,data)
    
def _getDataFormatedToURL(data=''):

    #iniciando coleta a partir de uma determinada data
    if (data == ''):
        data = datetime.datetime.now()
    else:
        data = datetime.datetime.strptime(data,'%a, %d %b %Y %H:%M:%S %Z')
        
    if (data.month < 10):
        mes = "0"+str(data.month)
    else:
        mes = data.month
        
    if (data.day < 10):
        dia = "0" + str(data.day)
    else:
        dia = data.day       
    data = str(data.year)+"-"+str(mes)+"-"+str(dia)
    
    return data

def urlEncodeQueryAndFilter(q,geocode="", lang="", locale="", result_type="mixed", count=100, until="", since_id="", max_id="", include_entities="", callback=""):
    '''
       Explicacao em: https://dev.twitter.com/rest/reference/get/search/tweets
    '''

    urlParameters = "?q="+q
    urlParameters += "&result_type="+result_type 
    urlParameters += "&count="+str(count)

    if (geocode != ""):
        urlParameters += "&geocode="+geocode
    if (lang != ""):
        urlParameters += "&lang="+lang
    if (locale != ""):
        urlParameters += "&locale="+locale
    if (until != ""):
        urlParameters += "&until="+until
    if (since_id != ""):
        urlParameters += "&since_id="+since_id
    if (max_id != ""):
        urlParameters += "&max_id="+max_id
    if (include_entities != ""):
        urlParameters += "&include_entities="+include_entities
    if (callback != ""):
        urlParameters += "&callback="+callback

    return urlParameters


def pegarTweetsAPI(client,collection,query,log, minId="",maxId=""):

    requisicoesPossiveisNaJanela = 180 #posso pegar automaticamente para o servico
    status = 0
    fimDaJanelaDeBuffer = 0
    dataParaURL = ""


    while True:
        try:
            dataParaURL = _getDataFormatedToURL(dataParaURL)
                        
            if maxId != "":
                dataParaURL=""
            
            urlParameters = urlEncodeQueryAndFilter(q=query,until=dataParaURL,since_id=str(minId), max_id=str(maxId))

            url = "https://api.twitter.com/1.1/search/tweets.json"            
            url += urlParameters

            responseHead,resource = client.request(url,"GET")

            status,requisicoesPossiveisNaJanela,fimDaJanelaDeBuffer,dataParaURL = getResponseInformations(responseHead)

            if (requisicoesPossiveisNaJanela > 0):
    
                resource = json.loads(resource)
                resource = resource['statuses']                
                cont = 0

                ids = []                
                for item in resource:
                    collection.insert(item)
                    cont += 1
                    ids.append(item['id'])
                              
                ids.sort()                
                maxId = ids[0]
                maxId -= 1

                if (len(resource) < 1): #parando o crawler por causa da baixa quantidade de tweets
                    print 'poucos tweets'
                    time.sleep(60*30)

            else:
                time.sleep(fimDaJanelaDeBuffer) #aguardando tempo restante da janela para voltar as consultas
                print "limite da janela alcancado"
                
            time.sleep(1)
        except:
            print "exception: ver arquivo de log para maiores detalhes."            
            msg = "\n\nAo Salvar o Arquivo"+"\nHTTP response:"+"\nstatus:"+str(status)+"\nError:" + str(sys.exc_info()[0])
            logError(log, msg)
            time.sleep(15*60)#in seconds
            print "voltando da excecao"

def pegarTweetsAPIEntreDatasAdapter(client,collection,query,log, minId="",maxId=""):

    requisicoesPossiveisNaJanela = 180 #posso pegar automaticamente para o servico
    status = 0
    fimDaJanelaDeBuffer = 0
    #dataParaURL = ""
    while True:
        try:
            
            urlParameters = urlEncodeQueryAndFilter(q=query,since_id=str(minId), max_id=str(maxId))

            url = "https://api.twitter.com/1.1/search/tweets.json"            
            url += urlParameters
            responseHead,resource = client.request(url,"GET")

            status,requisicoesPossiveisNaJanela,fimDaJanelaDeBuffer,dataParaURL = getResponseInformations(responseHead)
            if (requisicoesPossiveisNaJanela > 0):
    
                resource = json.loads(resource)
                resource = resource['statuses']                
                cont = 0

                ids = []                
                for item in resource:
                    collection.insert(item)
                    cont += 1
                    ids.append(item['id'])
                    
                ids.sort()                
                maxId = ids[0]
                maxId -= 1
                if (len(resource) < 1): #parando o crawler por causa da baixa quantidade de tweets
                    print 'poucos tweets'
                    time.sleep(60*30)
            else:
                time.sleep(fimDaJanelaDeBuffer) #aguardando tempo restante da janela para voltar as consultas
                print "limite da janela alcancado"
                
            time.sleep(1)
        except:
            print "exception: ver arquivo de log para maiores detalhes."            
            msg = "\n\nAo Salvar o Arquivo"+"\nHTTP response:"+"\nstatus:"+str(status)+"\nError:" + str(sys.exc_info()[0])
            logError(log, msg)
            time.sleep(15*60)#in seconds
            print "voltando da excecao"




def pegarTweetsUsandoTwitterStream():
    pass
    #ver estrategia para fechar e reiniciar conexao a cada 90 segundos sem dados
    #stream API envia dados a cada 30 segundos

def getLastIdInACollection(mongo, db, collection):
      
      maxId = ""
      minId = ""

      #verificando se o banco existe
      if not(db in mongo.database_names()):
        return minId,maxId
            
      db = mongo[db]
      if not(collection in db.collection_names()):
        return minId,maxId

      collection = db[collection]

      tweet = collection.find().sort("id",1).limit(1)
      minId = tweet[0]['id']
            
      tweet = collection.find().sort("id",-1).limit(1)
      maxId = tweet[0]['id']
      
      return minId,maxId
      
      
'''
    As proximas funcoes (exceto a funcao principal) possuem as configuracoes para coletar os dados de:
        - iniciarColetaTweetsAntigos - tweets antigos (precisa de um arquivo com os ids dos tweets antigos que podem ser obtidos com o crawler com o selenium simulando um usuario navegando na pagina). Nesta versao, os tweets estao sendo salvos em um arquivo porque ainda nao alterei para salvar direto no mongo. Entao o arquivo de destino, alem do arquivo com os ids, precisa ser definido.
        tweets API (pegar tweets com ate 7 dias de postagem).
        tweets stream
'''
def iniciarColetaTweetsAntigos(client, log):
    arq = open("idsTweets")
    destino = codecs.open("protests_collections_x.json",'w',"utf-8")

    coletarTweetsAntigos(arq, destino,log)

    arq.close()
    destino.close()
    log.close()

def escolherEstrategiaColeta(minId,maxId):
    
    #opcao = -1    
    #while (opcao == -1):
    print "Escolha uma opcao: (1-padrao) Se deseja coletar tweets mais antigos a partir do id mais antigo na colecao; ou (2) se deseja coletar tweets da data atual ate o mais recente na colecao."
    print "Ex: quero coletar tweets mais antigos dos que o que peguei ate ocorrer um erro ou quero coletar tweets do dia de hoje ate o mais recente na minha base para evitar repeticoes?"
    opcao = input("opcao 1 ou 2?")

    if (opcao == 2):
        return maxId,""
    else:
        return "",maxId

def iniciarColetaComTwitterAPI(client,log,q,dbName="",collectionName=""):

    mongo = pymongo.MongoClient()    
    #pega id minimo e maximo de uma colecao que ja possua tweet
    minId, maxId = getLastIdInACollection(mongo, dbName, collectionName)
    #banco no mongo e colecao
    db = mongo[dbName]    
    collection = db[collectionName]


    if (maxId != "" or minId != ""):
        #RESOLVI OS PROBLEMAS DESCRITOS ABAIXO COM UM IF com as opcoes apresentadas no momento de rodar a aplicacao
        #Se ja existir algo no banco, entao minId = maxId e maxId = "" porque eu vou pegar a partir da data
        #do dia ate o id minimo ja existente no banco quando rodei o codigo
        #ou seja, vou pegar tweets mais recentes a partir da data que o codigo foi executado ate o id que ja
        #foi coletado
        #minId = maxId #essa solucao mata minha aplicacao se eu coletei e parei e preciso retomar a coleta, porque ele faz do dia atual pra tras, entao ele faz do momento ate o maxId do banco
        
        #como eu coleto do maior pra tras, eu tenho que pegar o menorId, associar ele ao maxId para continuar coletando tweets mais antigos ja que coleto regressivo
        #o ruim disso e que quando rodar a aplicacao preciso decidir se quero coletar ate o mais atingo ou da data atual ate a data que ja possuo algo no banco (solucao acima)
        #maxId = minId
        minId,maxId = escolherEstrategiaColeta(minId,maxId)

    pegarTweetsAPI(client,collection,q,log,minId,maxId)

def iniciarColetaComTwitterAPIEntreDatas(client,log,q,since,until,dbName="",collectionName=""):
    mongo = pymongo.MongoClient()    
    #pega id minimo e maximo de uma colecao que ja possua tweet
    minId, maxId = getLastIdInACollection(mongo, dbName, collectionName)
    #banco no mongo e colecao
    db = mongo[dbName]    
    collection = db[collectionName]

    if (maxId != "" or minId != ""):
        minId,maxId = escolherEstrategiaColeta(minId,maxId)

    q = "?q="+q+"&since="+since+"&until="+until
    pegarTweetsAPIEntreDatasAdapter(client,collection,q,log,minId,maxId)



if __name__ == "__main__":

    '''
        Este codigo espera apenas o tempo necessario para encerrar uma janela de requisicoes no Twitter ao inves de esperar 15 minutos ao ser bloqueado.
        Evito tomar bloqueios, se atingi o maximo de requisicoes para de solicitar dados
        Para rodar este codigo, tenha instalado as seguintes dependencias.
            urllib2
            oauth2
            pymongo
            json
            time
            codecs
            sys
            datetime
    '''

    print 'crie um arquivo e uso um dos 3 ultimos metodos fornecendo seus parametros'

    
