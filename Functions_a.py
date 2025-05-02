import pandas as pd 
import numpy as np
import gspread
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import requests
import platform
# import dataframe_image as dfi
# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
import time
import os
import certifi
import slack
import sys
import urllib.parse

def AttPBI(dataset_id):
    
    import requests
    import sys
    sys.path.append(r'\\192.168.5.15\mis\Funcoes')
    import encrypt as enc


    var = r'\\192.168.5.15\mis\Funcoes\vars'
    var = enc.read_encrypted_file(var + r'\.env',var)
    
    # Obter o token de autenticação
    tenant = var.get('tenant_id')
    token_url = f'https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token'
    
    
    token_data = {

        'client_id': var.get('client_id'),
        'client_secret': var.get('client_secret'),
        'grant_type': 'password',
        'username': var.get('USUARIO_EMAIL'),
        'password': var.get('USUARIO_SENHA'),
        'scope': 'https://analysis.windows.net/powerbi/api/.default'
        
    }


    body = {
        'Content-Type' : 'application/x-www-form-urlencoded'
    }

    token_response = requests.post(token_url, data=token_data, json=body)
    token_response.raise_for_status()
    access_token = token_response.json().get('access_token')

    refresh_url = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes'
    headers = {
        'Authorization': rf'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    body ={
        'notifyOption':'NoNotificatioan'
    }

    refresh_response = requests.post(refresh_url, headers=headers)
    if refresh_response.status_code == 202:
        print("Solicitação de atualização enviada com sucesso.")
    else:
        print("Erro ao enviar a solicitação de atualização.")
        print(refresh_response.status_code)

def insert_dataframe_to_sql(df, table_name, conn):
    cursor = conn.cursor()

    # Extrair os nomes das colunas do DataFrame
    columns = ', '.join(df.columns)
        
        # Criar os placeholders para os valores (?, ?, ?...)
    placeholders = ', '.join(['?'] * len(df.columns))
        
        # Gerar o SQL INSERT dinâmico
    insert_query = f"INSERT INTO tbl_000_Telefonia_Nexus_Docktech_testes ([{columns}]) VALUES "
    
    # Iterar sobre as linhas do DataFrame e executar as inserções

    errors = 0
    listErrors = []
    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query + str(tuple(row)))
        except Exception as e:
            
            errors +=1
            listErrors.append(tuple(row))
 


    conn.commit()  # Salva as alterações no banco de dados
    cursor.close()
    conn.close()

    print(f"Quantidade de erros no insert = {errors}")

    return listErrors

#========================================================= CONECT DB'S LINUX

def SecaoSpark(Nome):
    from pyspark.sql import SparkSession as ss
    
    SparkSechi = (ss.builder.appName(Nome)
                  .config('spark.sql.replEval.enabled', True)
                  .getOrCreate()
                  )
    
    return SparkSechi
    
#========================================================== GOOGLE SHEETS

def GoogleSheets(conect_json,url,page,jump = 0):
    gc = gspread.service_account(filename=conect_json)
    dados = gc.open_by_url(url).worksheet(page)
    colunas = dados.get_all_values().pop(jump)
    valores = dados.get_all_values()
    df = pd.DataFrame(valores,columns = colunas).drop(index=0).reset_index(drop=True)
    df = df[jump:]
    return df

#========================================================== ALTERAÇÃO DE DADOS

# TIME DELTA PARA STRING ESTILO TIME HH:MM:SS
#LEMBRANDO QUE SÓ CALCULA ATÉ OS MINUTOS

# TESTE É UM DATAFRAME E COLUNA É UMA COLUNA DESSE DATAFRAME (COLUNA TEM QUE ESTAR EM FORMATO STRING!!!)

def AlteraDeltaToString(teste,coluna):
    teste[coluna] = teste[coluna].astype(str)
    teste[['dias','dropa','hms']] = teste[coluna].str.split(' ',expand=True,n=2)
    teste = teste.assign(dias = teste.dias.astype(int)*24)
    teste[['hora','minuto','segundo']] = teste.hms.str.split(':',expand=True,n=2)
    teste1 = teste.query('dias < 0')
    teste = teste.query('dias >= 0')
    teste = teste.assign(hora = teste.hora.astype(int) + teste.dias)
    teste = teste.assign(hora = teste.hora.astype(str))
    teste[coluna] = teste.hora + ":" + teste.minuto + ":" + teste.segundo
    teste1 = teste1.assign(hora = teste1.hora.str[1:].astype(int) +1 + teste1.dias,
                minuto = (teste1.minuto.astype(int) * -1) + 60)
    teste1 = teste1.assign(hora = teste1.hora.astype(str),
                        minuto = teste1.minuto.astype(str))
    teste1.hora = teste1.hora.str.replace("-","")
    teste1 = teste1.assign(
                    hora = np.where(teste1.hora.str.len() >= 2,teste1.hora,"0" + teste1.hora),
                    minuto = np.where(teste1.minuto.str.len() >= 2,teste1.minuto,"0" + teste1.minuto)
                )
    teste1[coluna] = "-"+ teste1.hora + ":" + teste1.minuto + ":" + teste1.segundo
    teste = pd.concat([teste,teste1])
    teste = teste.drop(columns=['dias','dropa','hms','hora','minuto','segundo'])    
    return teste



def Dt_inicio_fim(data): 
    datastr = datetime.strftime(data,r'%Y-%m-%d')
    ano = data.year 
    mes = data.month
    dia = data.day
    x=1
    if mes + x > 12:
        anoprox = ano + 1
        mesprox = 1
    else: 
        anoprox = ano
        mesprox = mes + 1
        
    datadu1 = datetime.strftime(datetime(ano,mes,1),r'%Y-%d-%m')
    datadu1Correto = datetime.strftime(datetime(ano,mes,1),r'%Y-%m-%d')
    #dataUltimoDu = datetime.strftime(datetime(anoprox,mesprox,1) - timedelta(days=1),r'%Y-%d-%m')
    dataUltimoDu = datetime.strftime(data,r'%Y-%d-%m')
    dataUltimoDuCorreto = datetime.strftime(data,r'%Y-%m-%d')


    # Quadro dock, tem que retirar mais tarde de uma tabela do google_sheets
    return datadu1Correto, dataUltimoDuCorreto

#=============================================================== LOG'S

def point(string):

    print("=========================================================")
    print('-----------------------------------------------------------------')
    print('')
    print(string)
    print(str(datetime.now())[:-7])
    print('')
    print('-----------------------------------------------------------------')
    print("=========================================================")
    
    
#================================================================= MENSAGENS TELEGRAM

# enviar mensagens utilizando o bot para um chat específico
def send_message(token, chat_id, message):
    try:
        data = {"chat_id": chat_id, "text": message}
        url = "https://api.telegram.org/bot{}/sendMessage".format(token)
        requests.post(url, data)
    except Exception as e:
        print("Erro no sendMessage:", e)
        
def send_message_slack(message,token,channel):
    error = '<urlopen error [WinError 10054] Foi forçado o cancelamento de uma conexão existente pelo host remoto>'
    while error == '<urlopen error [WinError 10054] Foi forçado o cancelamento de uma conexão existente pelo host remoto>':
        try:
        #time.sleep(1)
            client = slack.WebClient(token= token)
            client.chat_postMessage(channel=channel, text=message)
            error='Sem erro'
            
        except Exception as e:
            print('erro no envio menssagem slack')
            error = e
            time.sleep(1)

        
def send_df_telegram(token_id, chat_id, message, df):
    ComplementoNomeArq = str(datetime.today())[:-7].replace(' ','').replace(':','')
    print(ComplementoNomeArq)
    pasta = rf'C:\Users\{os.getlogin()}\TempImagens'
    if not os.path.exists(pasta):
        os.makedirs(pasta)
        
    url_df = pasta + rf"\Image_temp_telegram_{ComplementoNomeArq}.jpeg"
    
    dfi.export(df, url_df, table_conversion='matplotlib', max_rows=-1)

    
    try:
        data = {'chat_id': chat_id, 'parse_mode': 'HTML', 'caption': message}
        url = "https://api.telegram.org/bot{}/sendPhoto?".format(token_id)
        files = {'photo': open(url_df, 'rb')}
        certificates = certifi.where()
        requests.post(url, data=data, files=files, stream=True, verify=certificates)
    except Exception as e:
        print("Erro no sendMessage:", e)
        
        
        
        

def upBD(banco,tabela,df):

    engine = banco
    batch_size = 5000
    count = 1
    # Dividir o DataFrame em lotes e carregar na tabela
    for i in range(0, len(df), batch_size):
        df_batch = df[i:i+batch_size]
        df_batch.to_sql(tabela, engine, if_exists='append', index=False)
        print(f'Batch {count} processado.')
        count = count + 1
    print('Finalizou update em banco de dados')
    # Fechar a conexão com o banco de dados
    engine.dispose()
    
    
    

def upBD_v2(banco,tabela,df):
    import logging
    engine = banco
    batch_size = 5000
    count = 1
    # Dividir o DataFrame em lotes e carregar na tabela
    for i in range(0, len(df), batch_size):
        df_batch = df[i:i+batch_size]
        df_batch.to_sql(tabela, engine, if_exists='append', index=False)
        logging.info(f'Batch {count} processado.')
        count = count + 1
    print('Finalizou update em banco de dados')
    # Fechar a conexão com o banco de dados
    engine.dispose()
    
# cria lista com datas de todos os dias do mes
    
def ListaDiasMes(referencia):
    feriado = ['2022-01-01']
    ano = referencia.year
    mes = referencia.month
    diaa = 1 
    mes1 = timedelta(days=32)
    dataMesFrente = datetime(ano,mes,diaa) + mes1
    dia = datetime(dataMesFrente.year,dataMesFrente.month,1) - timedelta(days=1)

    mesReff = dia.month

    primeiroDia = 1
    ateDia = dia.day
    anoReff = dia.year
    datas = []

    for day in range(primeiroDia,ateDia+1):
        dia = str(day)
        data = ''
        if len(dia) == 1:
            dia = f'0{str(day)}'
        else:
            dia=str(day)
        data = f"{str(anoReff)}-{str(mesReff)}-{dia}"
        dataTratada = datetime.strptime(data,r'%Y-%m-%d')
        
        datas.append(str(dataTratada)[0:10])
            
    return datas

# trata resgistro = segunda-feira, 4 de março de 2024 para 04/03/2024

def altMes(df,col):
    
    df[col + '2']  = df[col].str[3:6]
    
    meses_pt = [df[col + '2'] == 'jan',
        df[col + '2'] == 'fev',
        df[col + '2'] == 'mar',
        df[col + '2'] == 'abr',
        df[col + '2'] == 'mai',
        df[col + '2'] == 'jun',
        df[col + '2'] == 'jul',
        df[col + '2'] == 'ago',
        df[col + '2'] == 'set',
        df[col + '2'] == 'out',
        df[col + '2'] == 'nov',
        df[col + '2'] == 'dez'
    ]

    meses_en = [ 'Jan',
        'Feb',
        'Mar',
        'Apr',
        'May',
        'Jun',
        'Jul',
        'Aug',
        'Sep',
        'Oct',
        'Nov',
        'Dec'
    ]
    
    df[col + '2']  = np.select(meses_pt,meses_en,default='')
    
    df[col] = df[col].str[:3] +  df[col + '2'] + df[col].str[6:]
    
    df[col] =  pd.to_datetime(df[col], format= '%d/%b/%y %I:%M %p') 
    
    df = df.drop(columns=f"{col}2")

def TrataData(Emissores,DataContestacao):
    
    Tratativas = Emissores.query(f'{DataContestacao}.str.contains("de ")').copy()

    Emissores = Emissores.query(f'~{DataContestacao}.str.contains("de ")')

    Tratativas[['limbo','data']] = Tratativas[DataContestacao].str.split(', ',expand=True,n=1)

    Tratativas[['dia','mes','ano']] = Tratativas.data.str.split(' de ',expand=True,n=2)

    mesDict = {'01':'janeiro',
                '02':'fevereiro',
                '03':'março',
                '04':'abril',
                '05':'maio',
                '06':'junho',
                '07':'julho',
                '08':'agosto',
                '09':'setembro',
                '10':'outubro',
                '11':'novembro',
                '12':'dezembro'}

    condicoes = [Tratativas['mes'] == mes_nome for mes_nome in mesDict.values()]

    resultados = [mes_num for mes_num in mesDict.keys()]

    Tratativas['mes'] = np.select(condicoes, resultados, default='')

    Tratativas = Tratativas.assign(dia = np.where(Tratativas.dia.astype(int) < 10,"0" + Tratativas.dia,Tratativas.dia))

    Tratativas[DataContestacao] = Tratativas.dia + '/' + Tratativas.mes + '/' + Tratativas.ano

    Tratativas = Tratativas.drop(columns=['limbo','data','dia','mes','ano'])

    Emissores = pd.concat([Emissores,Tratativas])
    
    return Emissores


# Trata datas como 02-Dec-2023
def Tratamento_dia_mess_ano(MASTER,DataContestacao):
    Tratamento = MASTER.query(f'{DataContestacao}.str.contains("-")')
    MASTER = MASTER.query(f'~{DataContestacao}.str.contains("-")')
    if len(Tratamento) >=1:
        
        Tratamento[DataContestacao] = Tratamento[DataContestacao].str[:11]
        
        Tratamento[['dia','mes','ano']] = Tratamento[DataContestacao].str.split(r'-',expand = True,n=2)

        condicoes = [
            Tratamento.mes == 'Jan',
            Tratamento.mes == 'Feb',
            Tratamento.mes == 'Mar',
            Tratamento.mes == 'Apr',
            Tratamento.mes == 'May',
            Tratamento.mes == 'Jun',
            Tratamento.mes == 'Jul',
            Tratamento.mes == 'Aug',
            Tratamento.mes == 'Sep',
            Tratamento.mes == 'Oct',
            Tratamento.mes == 'Nov',
            Tratamento.mes == 'Dec',
        ]


        resultados = [
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '09',
            '10',
            '11',
            '12'
        ]

        Tratamento.mes = np.select(condicoes,resultados,default=None)

        Tratamento[DataContestacao] = Tratamento.dia + '/' + Tratamento.mes + '/' + Tratamento.ano

        Tratamento = Tratamento.drop(columns=['dia','mes','ano'])
        
        MASTER = pd.concat([MASTER,Tratamento])
        
    return MASTER

# mais uma funcao desgraçada para tratar datas exemplo 3/16/23 = 16/03/2023

def Tratamento_dia_mes_an(MASTER,DataContestacao):
    Tratamento = MASTER.query(f"{DataContestacao}.str.contains('/24')")
    MASTER = MASTER.query(f"~{DataContestacao}.str.contains('/24')")
    if len(Tratamento) >=1:       
        Tratamento[['mes','dia','ano']] = Tratamento[f'{DataContestacao}'].str[:-5].str.split(r'/',expand=True,n=2)
        Tratamento.dia = ("000" + Tratamento.dia).str[-2:]
        Tratamento.mes = ("000" + Tratamento.mes).str[-2:]
        Tratamento[f'{DataContestacao}'] = Tratamento.dia + '/' + Tratamento.mes + '/20' + Tratamento.ano
        Tratamento = Tratamento.drop(columns=['ano','mes','dia'])
        MASTER = pd.concat([MASTER,Tratamento])
    return MASTER


def ende(caminho):
    sistema_operacional = platform.system()
    #sistema_operacional = 'linux'
    if sistema_operacional == 'Windows':
        caminho =  mis + caminho
    else :
        caminho = mis + caminho.replace('\\',r'/')
    return caminho


def descobreWKSHEETS(acessoJson, url):
    gc = gspread.service_account(filename=acessoJson)
    dados = gc.open_by_url(url).worksheets() # caso queira pegar todas as planilhas pegue aqui, se dejesa uma palabra especifica no nome da planilha segue embaixo
    return dados

    


def separar_registro(df, coluna='registro', separador=' --- '):
    # Usa str.split para dividir a coluna em duas novas colunas
    novas_colunas = df[coluna].str.split(separador, n=1, expand=True)
    
    # Renomeia as novas colunas conforme necessário
    df[f'{coluna}_1'] = novas_colunas[0]
    df[f'{coluna}_2'] = novas_colunas[1]
    
    return df
    




def gerar_create_table_sql(df, table_name):
    """
    Gera uma instrução CREATE TABLE para SQL Server com base no DataFrame fornecido.
    
    :param df: DataFrame do pandas contendo os dados.
    :param table_name: Nome da tabela a ser criada no SQL Server.
    :return: String contendo a instrução CREATE TABLE.
    """
    
    # Função auxiliar para determinar o tipo INT adequado
    def determinar_tipo_int(coluna):
        min_val = coluna.min()
        max_val = coluna.max()
        
        if min_val >= 0:
            if max_val <= 255:
                return 'TINYINT'
            elif max_val <= 65535:
                return 'SMALLINT'
            elif max_val <= 2147483647:
                return 'INT'
            else:
                return 'BIGINT'
        else:
            if -128 <= min_val <= 127 and -128 <= max_val <= 127:
                return 'TINYINT'
            elif -32768 <= min_val <= 32767:
                return 'SMALLINT'
            elif -2147483648 <= min_val <= 2147483647:
                return 'INT'
            else:
                return 'BIGINT'
    
    # Função auxiliar para determinar o tipo FLOAT adequado
    def determinar_tipo_float(coluna):
        # Simplificação: usar FLOAT para todos os floats
        return 'FLOAT'
    
    # Mapeamento de tipos de dados do pandas para SQL Server
    type_mapping = {
        'object': 'VARCHAR',       # Será ajustado com o tamanho
        'int64': 'BIGINT',           # Será ajustado conforme os dados
        'int32': 'INT',               # Será ajustado conforme os dados
        'float64': 'FLOAT',           # Pode ajustar para REAL se necessário
        'float32': 'REAL',
        'bool': 'BIT',
        'datetime64[ns]': 'DATETIME',
        'timedelta[ns]': 'TIME',      # Considerar se TIME é adequado
        'category': 'VARCHAR',       # Será ajustado com o tamanho
        'datetime64[ns, UTC]': 'DATETIME',
        'Int64': 'BIGINT',            # Suporte para nullable integers
        'boolean': 'BIT',             # Suporte para nullable booleans
    }
    
    columns_definitions = []
    
    for column_name, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        sql_type = type_mapping.get(dtype_str, 'VARCHAR')  # Default para VARCHAR se tipo não mapeado
        
        nullable = 'NULL' if df[column_name].isnull().any() else 'NOT NULL'
        
        # Tratamento específico para tipos de dados
        if sql_type == 'VARCHAR':
            # Determinar o comprimento máximo na coluna
            max_len = df[column_name].dropna().astype(str).map(len).max()
            if pd.isna(max_len):
                max_len = 255  # Valor padrão se coluna está completamente vazia
            else:
                # Adicionar 10%
                max_len = int(np.ceil(max_len * 1.1))
                # Definir um limite superior para evitar tamanhos excessivos
                max_len = min(max_len, 4000)  # VARCHAR(MAX) se exceder 4000
            if max_len > 4000:
                sql_type = 'VARCHAR(MAX)'
            else:
                sql_type = f'VARCHAR({max_len})'
        
        elif sql_type in ['INT', 'BIGINT']:
            # Determinar o tipo INT adequado
            sql_type = determinar_tipo_int(df[column_name])
        
        elif sql_type in ['FLOAT', 'REAL']:
            # Determinar o tipo FLOAT adequado (pode ser aprimorado)
            sql_type = determinar_tipo_float(df[column_name])
        
        elif sql_type == 'TIME':
            # timedelta para TIME pode não ser adequado se ultrapassar 24 horas
            # Considerar usar BIGINT para armazenar total de segundos ou outra abordagem
            # Aqui, vou usar TIME, mas isso pode precisar de ajuste
            sql_type = 'TIME'
        
        # Adicionar a definição da coluna com NULL ou NOT NULL
        columns_definitions.append(f"    [{column_name}] {sql_type} {nullable}")
    
    # Combinar todas as definições de colunas
    columns_sql = ",\n".join(columns_definitions)
    
    # Construir a instrução CREATE TABLE completa
    create_table_sql = f"CREATE TABLE [{table_name}] (\n{columns_sql}\n);"
    
    return create_table_sql


def envia_email(emails, emails_cc, assunto, corpo, cam_arq):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import smtplib
    
    smtp_server = "mail.dbm.com.br"
    smtp_port = 587
    email_user = "andre.nobrega@dbm.com.br"
    email_password = "3Uqu3r0m0rr3r@"

    
    """Envia o arquivo por email"""
    start_time = time.time()
    msg = MIMEMultipart()
    msg['From'] = email_user
    msg['To'] = ', '.join(emails)
    msg['Cc'] = ', '.join(emails_cc)
    msg['Subject'] = assunto

    msg.attach(MIMEText(corpo, 'plain'))

    if cam_arq and os.path.isfile(cam_arq):
        attachment = open(cam_arq, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(cam_arq)}')
        msg.attach(part)
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(email_user, email_password)
    text = msg.as_string()
    
    server.sendmail(email_user, (emails + emails_cc), text)

    server.quit()

    end_time = time.time()
    
