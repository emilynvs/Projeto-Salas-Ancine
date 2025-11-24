import pandas as pd
import mysql.connector
import numpy as np
import unidecode

arquivo = 'salas_df_normal.xlsx'

def normalize(value):
    if pd.isna(value):
        return None
    return unidecode.unidecode(str(value)).upper().strip()

def valor_mysql(x):
    return None if pd.isna(x) else x

tabela_exibidor = pd.read_excel(arquivo, usecols= ['NOME_EXIBIDOR', 'REGISTRO_EXIBIDOR', 'CNPJ_EXIBIDOR', 'SITUACAO_EXIBIDOR', 'NOME_GRUPO_EXIBIDOR', 'OPERACAO_USUAL'])
tabela_endereco = pd.read_excel(arquivo, usecols=['NUMERO_ENDERECO_COMPLEXO', 'COMPLEMENTO_COMPLEXO', 'BAIRRO_COMPLEXO', 'MUNICIPIO_COMPLEXO', 'CEP_COMPLEXO', 'UF_COMPLEXO', 'ENDERECO_COMPLEXO', 'NUMERO', 'LOGRADOURO', 'RESTO'])
tabela_complexo = pd.read_excel(arquivo, usecols=[
    'NOME_COMPLEXO',
    'REGISTRO_COMPLEXO',
    'SITUACAO_COMPLEXO',
    'DATA_SITUACAO_COMPLEXO',
    'PAGINA_ELETRONICA_COMPLEXO',
    'COMPLEXO_ITINERANTE',
    'REGISTRO_EXIBIDOR',       # para mapear id_exibidor
    'ENDERECO_COMPLEXO',        # para mapear id_endereco
    'NUMERO_ENDERECO_COMPLEXO'  # para mapear id_endereco
])
tabela_sala = pd.read_excel(arquivo, usecols=['NOME_SALA', 'REGISTRO_SALA','CNPJ_SALA','SITUACAO_SALA', 'DATA_SITUACAO_SALA', 'DATA_INICIO_FUNCIONAMENTO_SALA', 'ASSENTOS_SALA', 'ASSENTOS_CADEIRANTES', 'ASSENTOS_MOBILIDADE_REDUZIDA','ASSENTOS_OBESIDADE', 'ACESSO_ASSENTOS_COM_RAMPA', 'ACESSO_SALA_COM_RAMPA', 'BANHEIROS_ACESSIVEIS', 'REGISTRO_COMPLEXO'])

df = pd.read_excel(arquivo)

conexao = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Noah",
        database="salas_ancine"
)

cursor = conexao.cursor()

sql = 'INSERT INTO exibidor (nome_exibidor, registro_exibidor, cnpj_exibidor, situacao_exibidor, nome_grupo_exibidor, operacao_usual) VALUES (%s, %s, %s, %s, %s, %s)'

for _, row in tabela_exibidor.iterrows():
    cursor.execute(sql, (
        row['NOME_EXIBIDOR'],
        row['REGISTRO_EXIBIDOR'],
        row['CNPJ_EXIBIDOR'],
        row['SITUACAO_EXIBIDOR'],
        row['NOME_GRUPO_EXIBIDOR'],
        row['OPERACAO_USUAL']
    ))
cursor.execute("SELECT id_exibidor, registro_exibidor FROM exibidor")
exibidor_map = {str(registro).strip(): id_exibidor for id_exibidor, registro in cursor.fetchall()}


sql = 'INSERT INTO endereco(numero_endereco_complexo, complemento_complexo,bairro_complexo, municipio_complexo, cep_complexo, uf_complexo, endereco_complexo, numero_complexo,logradouro_complexo, resto_complexo)  VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s)'
for _, row in tabela_endereco.iterrows():
    # algumas linhas aparecem vazias ou algo assim, gerando um erro NaN que o mySql nao compreende, usando isso aqui abaixo,já resolve
    cursor.execute(sql, (
        row['NUMERO_ENDERECO_COMPLEXO'] if not pd.isna(row['NUMERO_ENDERECO_COMPLEXO']) else None,
        row['COMPLEMENTO_COMPLEXO'] if not pd.isna(row['COMPLEMENTO_COMPLEXO']) else None,
        row['BAIRRO_COMPLEXO'] if not pd.isna(row['BAIRRO_COMPLEXO']) else None,
        row['MUNICIPIO_COMPLEXO'] if not pd.isna(row['MUNICIPIO_COMPLEXO']) else None,
        row['CEP_COMPLEXO'] if not pd.isna(row['CEP_COMPLEXO']) else None,
        row['UF_COMPLEXO'] if not pd.isna(row['UF_COMPLEXO']) else None,
        row['ENDERECO_COMPLEXO'] if not pd.isna(row['ENDERECO_COMPLEXO']) else None,
        row['NUMERO'] if not pd.isna(row['NUMERO']) else None,
        row['LOGRADOURO'] if not pd.isna(row['LOGRADOURO']) else None,
        row['RESTO'] if not pd.isna(row['RESTO']) else None
    ))
cursor.execute("SELECT id_endereco, endereco_complexo, numero_endereco_complexo FROM endereco")
endereco_map = {(endereco_complexo, numero): id_endereco for id_endereco, endereco_complexo, numero in cursor.fetchall()}

sql = 'INSERT INTO complexo(id_exibidor, id_endereco, nome_complexo, complexo_itinerante, situacao_complexo, data_situacao_complexo, registro_complexo, pagina_eletronica_complexo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

for _, row in tabela_complexo.iterrows():
    reg_exib = str(row['REGISTRO_EXIBIDOR']).strip()       # converte para string
    id_exibidor = exibidor_map.get(reg_exib)               # agora encontra no map
    id_endereco = endereco_map.get((row['ENDERECO_COMPLEXO'], row['NUMERO_ENDERECO_COMPLEXO']))
    cursor.execute(sql, (
        id_exibidor,
        id_endereco,
        valor_mysql(row['NOME_COMPLEXO']),
        valor_mysql(row['COMPLEXO_ITINERANTE']),
        valor_mysql(row['SITUACAO_COMPLEXO']),
        valor_mysql(row['DATA_SITUACAO_COMPLEXO']),
        valor_mysql(row['REGISTRO_COMPLEXO']),
        valor_mysql(row['PAGINA_ELETRONICA_COMPLEXO'])
))
    
    
cursor.execute("SELECT id_complexo, registro_complexo FROM complexo")
complexo_map = {
    unidecode.unidecode(str(registro)).upper().strip(): id_complexo
    for id_complexo, registro in cursor.fetchall()
}


sql = 'INSERT INTO sala(id_complexo, nome_sala, registro_sala, cnpj_sala, situacao_sala,data_situacao_sala, data_inicio_funcionamento_sala, assentos_sala, assentos_cadeirantes, assentos_mobilidade_reduzida, assentos_obesidade, acesso_assentos_com_rampa, acesso_sala_com_rampa, banheiros_acessiveis) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
for _, row in df.iterrows():

    reg_complexo_norm = normalize(row.get('REGISTRO_COMPLEXO'))
    id_complexo = complexo_map.get(reg_complexo_norm)

    if id_complexo is None:
        print(f"⚠ COMPLEXO NÃO ENCONTRADO PARA SALA: {row.get('NOME_SALA')}  (REGISTRO: {row.get('REGISTRO_COMPLEXO')})")
        continue

    cursor.execute(sql, (
        id_complexo,
        valor_mysql(row.get('NOME_SALA')),
        valor_mysql(row.get('REGISTRO_SALA')),
        valor_mysql(row.get('CNPJ_SALA')),
        valor_mysql(row.get('SITUACAO_SALA')),
        valor_mysql(row.get('DATA_SITUACAO_SALA')),
        valor_mysql(row.get('DATA_INICIO_FUNCIONAMENTO_SALA')),
        valor_mysql(row.get('ASSENTOS_SALA')),
        valor_mysql(row.get('ASSENTOS_CADEIRANTES')),
        valor_mysql(row.get('ASSENTOS_MOBILIDADE_REDUZIDA')),
        valor_mysql(row.get('ASSENTOS_OBESIDADE')),
        valor_mysql(row.get('ACESSO_ASSENTOS_COM_RAMPA')),
        valor_mysql(row.get('ACESSO_SALA_COM_RAMPA')),
        valor_mysql(row.get('BANHEIROS_ACESSIVEIS'))
    ))

conexao.commit()
cursor.close()
conexao.close()