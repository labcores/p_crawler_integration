# -*- coding: utf-8 -*-
import crawlerTwitter
import oauth2 as oauth

if __name__ == "__main__":

    '''
        script para python 2, portar para o 3.
        Evite usar a mesma credencia em coletores que estejam funcionando ao mesmo tempo. Voce sofrera com as restricoes impostas.
        Tenha o codigo crawlerTwitter com as devidas funcoes.
        Instale o oauth2 para usar este arquivo.
        Preencha os campos abaixo.
    '''
    #Obtenha as credenciais no twitter https://apps.twitter.com/
    Consumer_key = ""
    Consumer_secret = ""
    Access_token = ""
    Access_token_secret = ""

    consumer = oauth.Consumer(key=Consumer_key, secret=Consumer_secret)
    access_token = oauth.Token(key=Access_token, secret=Access_token_secret)
    client = oauth.Client(consumer, access_token)

    #substitua os valores abaixo (nome do arquivo de log, nome do banco e da colecao no mongo).
    log = open("NOMEARQUIVOLOG","a")    
    dbName = 'NOMEBANCOMONGO'
    collectionName = 'NOMECOLECAOMONGO'

    #defina o conteudo da busca, para hashtag substitua o # por %23 (encoding do #)
    q = "%23StrangersOutTOMORROW OR %23QuintaDetremuraSDV OR %23CiteUmProblema OR %23OQueTeFazFeliz OR %2325deMayo OR brasilia OR %23prayforjakarta OR dilma OR %23foratemer"
    crawlerTwitter.iniciarColetaComTwitterAPI(client,log,q,dbName,collectionName)
    #OR %23agrevefracassou OR %23euvoutrabalhar OR %23brasiltrabalhador
    log.close()
    #rode o arquivo.
