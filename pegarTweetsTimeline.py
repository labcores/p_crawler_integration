# -*- coding: utf-8 -*-
import re
#import urllib2
import oauth2 as oauth
#import pymongo
import json
import time
import codecs
import sys

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
            #print registro
            destino.write(json.dumps(registro)+"\n")
            #print "Escreveu"
            #destino.flush()
            #print "flush"
        except:
            msg = "Ao Salvar o Arquivo"
            logError(log, msg)
            print "Unexpected error:" + str(sys.exc_info()[0])

#Salvar somente o texto de um tweet
def customTweet(data, destino, ids):
    data = json.loads(data)
    cont = 0
    for row in data:
        destino.write(json.dumps(row["text"])+'\n')
        ids.write(json.dumps(str(row["id"]))+"\n")
        cont+=1
        #timeStamp.write(json.dumps(row["created_at"],'\n'))

'''
No trabalho de formacao de timeline, alem de coletar a user_home quero coletar dados de uma timeline especificada pelo usuário: coletar a timeline de duas contas que sigo?
O metodo abaixo faz isso. Preciso agora penas monitorar para pegar as mensagens mais recentes sem repetições. Melhor do que isso, so se for possivel usar usar a API de stream e pegar apenas novas publicacoes.
'''
def pegarTweetsDeUmUsuario(user, tweetTexto,destino, ids,maxTweets=200):

    if maxTweets > 200:
        maxTweets = 200
        print "A quantidade maxima de tweets é 200"
    maxId = 0

    cont = 0
    while True:
        URL = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+user+"&count="+str(maxTweets)
        if (maxId > 0):
            URL +="&max_id=" + str(maxId)
        response,data = client.request(URL,"GET")

        maxId = (json.loads(data)[-1]['id'] - 1)

        if (cont >= 10):
            break;
        cont += 1

        customTweet(data, tweetTexto, ids)

        salvarArquivo(data, destino,log)

#Criado Por Eliel
def pegarTextosDeUmPerfil(user, destino, ids, maxTweets=200):


    if maxTweets > 800:
        maxTweets = 200
        print "A quantidade maxima de tweets é 200"
    maxId = 0
    maxTweets = 2

    cont = 0
    while True:
        URL = "https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+user+"&count="+str(maxTweets)
        if (maxId > 0):
            URL +="&max_id=" + str(maxId)
        response,data = client.request(URL,"GET")

        maxId = json.loads(data)[-1]['id']

        if (cont >= 600 ):
            break;
        cont += 1

    customTweet(data, destino, ids)

'''
Esta funcao pega da home_timeline (linha do tempo do usuario com publicações dele e de quem ele segue) Pega a quantidade especificada em maxTweets (maximo de 200) dos tweets mais recentes na timeline.
Como no trabalho nos queremos pegar repeticoes (tweets que ele ja viu, nao vou controlar se ele pega tweets que ja foram recuperados antes - que seria feito com since_id, no caso).
Na nossa pergunta seria sobre a time line "como pegar apenas as mensagens mais recentes (20 últimas publicadas)?"... resolvido a explicacao acima.
'''
def pegarTimelineAuthUser(tweetTexto,destino,ids,maxTweets=100):

    '''
        Returns a collection of the most recent Tweets and Retweets posted by the authenticating user and the users they follow. The home timeline is central to how most users interact with the Twitter service.
        Up to 800 Tweets are obtainable on the home timeline. It is more volatile for users that follow many users or follow users who Tweet frequently.

        Parameters:
           tweetTexto - file where only the tweets text will be saved.
           destino - file where the tweet json will be saved.
           ids - file where tweets ids will be saved. This is an important file because tweets ids can be saved more than 30 days and it is possible recovery old tweets by ids.
           maxTweets is used to set the number of tweets to retrieve. The default value is 20 and the maximum is 200. maxTweets and max_id are different.

        Information:
            Response formats: JSON
            Require authentication
            Rate limits: 15 to each 15 minutes

        It is possible to use others parameters. See the reference.
        Reference: https://developer.twitter.com/en/docs/tweets/timelines/api-reference/get-statuses-home_timeline
    

    '''

    if maxTweets > 200:
        maxTweets = 200
        print "A quantidade maxima de tweets é 200"
    maxId = 0

    cont = 0
    while True:
        URL = "https://api.twitter.com/1.1/statuses/home_timeline.json?count="+str(maxTweets)#+"&include_entities=false"#to exclude entites from json response
        if (maxId > 0):
            URL +="&max_id=" + str(maxId)
        response,data = client.request(URL,"GET")

        maxId = (json.loads(data)[-1]['id'] - 1)

        '''incluir testes do tamanho da janela'''

        if cont > 0:
            break       

        cont += 1

        customTweet(data, tweetTexto, ids)

        salvarArquivo(data, destino,log)


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

    #arqIDs = ["577982721596723200"]

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
                #logErro(log,msg)
                print "Unexpected error:"+ str(sys.exc_info()[0])
                print "dormiu"
                time.sleep(60*15)
                pegarTweets(ids,destino,log)
            finally:
                ids = []

    else:
        pegarTweets(ids,destino, log)


if __name__ == "__main__":
    
    '''
        dependencias: oauth2 e codecs
        Use uma das seguintes funcoes e comente a outra.
        pegarTimelineAuthUser: pegar conteudo da timeline do usuario proprietario das credenciais.
        pegarTweetsDeUmUsuario: pegar conteudo de timeline de um usuario especifico (o que ele publicou)
    '''
    #crie suas credenciais em https://apps.twitter.com/. voce precisa estar logado no twitter com uma conta (tem que ter fornecido um numero de celular que sera validado por eles).
    Consumer_key = ""
    Consumer_secret = ""
    Access_token = ""
    Access_token_secret = ""

    consumer = oauth.Consumer(key=Consumer_key, secret=Consumer_secret)
    access_token = oauth.Token(key=Access_token, secret=Access_token_secret)
    client = oauth.Client(consumer, access_token)

    #substitua os nomes dos arquivos
    destino = codecs.open("hora.txt", "w", "utf-8")
    tweets_ids = codecs.open("hora_ids.txt", "w", "utf-8")
    tweets_texto= codecs.open("hora_texto.txt", "w", "utf-8")
    #tweets_timeStamp = codecs.open("fogoCruzado_timeStamp", "w", "utf-8")
    log = open("log.txt","a")
    #coletarTweetsAntigos(arq, destino,log)#coleta por ids que estao no arquivo idsTweets
    #pegarTimelineAuthUser(tweets_texto,destino, tweets_ids)
    pegarTweetsDeUmUsuario("nome perfil",tweets_texto,destino, tweets_ids)

    destino.close()
    tweets_texto.close()
    log.close()
