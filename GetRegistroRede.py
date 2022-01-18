#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests, os, urllib3, time,json, pandas as pd, csv, warnings, shutil, sys, lxml, re, itertools, openpyxl, glob, mysql.connector, sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from requests.exceptions import ConnectionError
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


#VARIAVEIS DE TEMPO E DIRETORIOS
time = datetime.now() - timedelta()
today = time.strftime('%y%m%d')
dataBanco = time.strftime('%Y-%m-%d %H:%M:%S')
dataLog = time.strftime('%Y-%m-%d_%H-%M-%S')



#QUERYS
queryConsumo = '''SELECT ...)
;'''
queryToken = '''SELECT ...;'''
#========================================================================================


#METODO DE CONEXÕES AO BANCO
engine = create_engine("mysql+pymysql://{user}:{pw}/{db}"
                       .format(user="",
                               pw="",
                               db=""))
def open_connection_noc():
    conn = mysql.connector.connect(
    host="",
    user="",
    password="",
    database="")
    return conn
        
def open_connection_bss():
    conn = mysql.connector.connect(
    host="",
    user="",
    password="",
    database="")
    return conn
def close_connection(x):
    x.close()
    
def write_log(file,texto):
    detailed_log = datetime.now().strftime("-%Y-%m-%d-")+ 'log.txt'
    file = file +  detailed_log
    print(texto)
    if os.path.exists(file):
        logs = open(file, 'a+')
        logs.write(texto)
        logs.close()
    else: 
        logs = open(file, 'w+')
        logs.write(texto)
        logs.close()
    return texto




def convert(list):
    return str(tuple(list)) 



def auth():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    url = ''
    headers = {''}
    try:
        r = requests.post()
    except ConnectionError as e:    # This is the correct syntax
            r = { "text": "Erro de conexão ao gerar o token", "status": 3333}
            
            return ''
    json_object = json.loads(r.text)
    return json_object["access_token"]


def get_hlr(api_token, msisdn):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    baseUrl = ''
    token = 'Bearer ' + api_token
    headers = {'Authorization': token, 'Content-Type': 'application/json'}
    url = baseUrl + ""
    urltail = ""
    if msisdn.startswith('') and len(msisdn) == 15:
        urltail = f'?imsi={ msisdn}'
    elif msisdn.startswith('') and len(msisdn) == 13:
        urltail = f'?msisdn={ msisdn}'
    else:
        erro = 'Entrada inválida informada||status:404' 
        r = { "text": erro + '\n Entrada: '+ str(msisdn), "status": "404"}
        
        return r
    url = url+urltail
    try:
        r = requests.get(url, headers=headers,verify=False)
        r = { "text": json.loads(r.text), "status": r.status_code}
    except ConnectionError as e:   
            r = { "text": "Erro de conexão, tente novamente ou contate o suporte", "status": "404"}
            erro = 'Erro de conexão, tente novamente ou contate o suporte || status:404'
            
    return r


#=======================================================================================================================


#BUSCANDO TOKEN NO BANCO DO NOC
try:
    connNoc = open_connection_noc()
    dfToken = pd.read_sql(queryToken, con=connNoc).astype(str)
    cursor = connNoc.cursor()
except ConnectionError as e: 
    erro = 'Falha ao conectar ao banco do NOC'
    

#VERIFICANDO SE HÁ TOKEN DO DIA, CASO CONTRARIO GERAR UM NOVO
if dfToken.empty:

    token = auth()
    cursor.execute('INSERT INTO api_tokens_tim(token_api, generation_date) VALUES("'+token+'","'+dataBanco+'");')
    connNoc.commit()

    
else:
    
    token = dfToken['token_api'].values[0]


#EXTRAINDO NÚMEROS DA TABELA DE SEM CONSUMO
try:
    dfNoc = pd.read_sql(queryConsumo, con=connNoc).astype(str)
    close_connection(connNoc)
    print('Query Extraida do Banco do NOC!!!')
except:
    print('Falha ao extrair query do banco do NOC!!!!')
    erro = 'Falha ao extrair query do banco do NOC!!!!'
    
    close_connection(connNoc)
    sys.exit()

#DANDO GET NOS MSISDNS DA TABELA SEM CONSUMO
column_names = []

dfRegistro = pd.DataFrame(columns = column_names)

count = 0

for i in dfNoc.itertuples():
    
    print('Dando Get em: ' + str(i.MSISDN) + ' Contagem em:' + str(count))
    count = int(count) + 1
    response = get_hlr(token, i.MSISDN)
    status = response['status']
    print(status)
    
    if status == '':
        vlr = response['text']['']['']['']['']
        if vlr != '':
            
            
            registro = {
                "id":[i.ID],
                "registroRede": [1]
            }

            df = pd.DataFrame(registro)
            dfRegistro = dfRegistro.append(df)
            print(f'Msisdn: {i.MSISDN} está registrado na rede')
            print(vlr)
        else:
            registro = {
                "id":[i.ID],
                "registroRede": [0]
            }
            df = pd.DataFrame(registro)
            dfRegistro = dfRegistro.append(df)
            print(f'Msisdn: {i.MSISDN} não está registrado na rede')
            print(vlr)

    
    #SEPARANDO OS DATAFRAMES QUE RETORNARAM O VLR, SEJA ELE REGISTRADO OU UNKNOWN
    try:
        comRegistro =[1]
        semRegistro =[0]

        dfComRegistro = dfRegistro[dfRegistro['registroRede'].isin(comRegistro)].reset_index(drop=True)
        dfSemRegistro = dfRegistro[dfRegistro['registroRede'].isin(semRegistro)].reset_index(drop=True)

        listaComRegistro = dfComRegistro['id'].to_list()
        tuplaComRegistro = convert(listaComRegistro)

        listaSemRegistro = dfSemRegistro['id'].to_list()
        tuplaSemRegistro = convert(listaSemRegistro)
    except:
        erro = 'erro de analise pandas'
        print(erro)
        
        
    else:
        print()

try:

    #ATUALIZANDO O BANCO COM AS QUE TIVERAM REGISTRO (REGISTROEEDE=1)
    connNoc = open_connection_noc()
    cursor = connNoc.cursor()
    cursor.execute('''UPDATE  SET  = 1 WHERE id IN'''+tuplaComRegistro+''';''')
    connNoc.commit()

except:
    erro = 'Falha na execução de update de sucessos existentes'
    
    
try:

    #ATUALIZANDO O BANCO COM AS QUE NÃO TIVERAM REGISTRO (REGISTROEEDE=0)
    connNoc = open_connection_noc()
    cursor = connNoc.cursor()
    cursor.execute('''UPDATE  SET  = 0 WHERE id IN'''+tuplaSemRegistro+''';''')
    connNoc.commit()
except:
    erro = 'Falha na execução de update'

close_connection(connNoc)
print('Batimento de registro de rede realizado, atualizações feitas')

