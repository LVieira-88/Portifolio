#!/usr/bin/env python
# coding: utf-8

# # Desafio Técnico Builder

# In[84]:


# Importação das bibliotecas
from pymongo import MongoClient
import pandas as pd

# Configurações de conexão MongoBD
conexao_mongo = {
    'username': 'teste_dados_leitura',
    'password': 'o7c4Cc8NDeXYbAMH',
    'host': 'mongodbtestebuilders.vuzqjs5.mongodb.net',
    'port': 27017,
    'authSource': 'admin',
    'ssl': True
}

# Conectar ao servidor MongoDB
client = MongoClient(conexao_mongo['host'],
                     username=conexao_mongo['username'],
                     password=conexao_mongo['password'],
                     authSource=conexao_mongo['authSource'],
                     ssl=conexao_mongo['ssl']
)


# Conectar ao servidor MongoDB
uri = f"mongodb+srv://{conexao_mongo['username']}:{conexao_mongo['password']}@{conexao_mongo['host']}/{conexao_mongo['authSource']}?ssl={conexao_mongo['ssl']}"
client = MongoClient(uri)

print('Conexão bem sucedida')

# Acessar um banco de dados
banco_de_dados = client['teste_dados']

# Obter lista de coleções
colecoes = banco_de_dados.list_collection_names()

# Acessar uma coleção
colecao = banco_de_dados['multas']

# Recuperar os dados da coleção
dados_mongo = list(colecao.find())

# Gerar DataFrame no Pandas
df_mongo = pd.DataFrame(dados_mongo)

# Exportar DataFrame para arquivo CSV
df_mongo.to_csv( 'Multas.csv', index=False)

# Fechar a conexão
client.close()
print('BD Desconectado')


# In[85]:


# Importação das bibliotecas
import mysql.connector
import pyodbc
import pandas as pd

# Configurações de conexão
conexao = {
    'host': '34.95.170.227',
    'port': '3306',
    'user': 'teste-dados-leitura',
    'password': 'o7c4Cc8NDeXYbAMH',
    'database': 'teste_dados',
}

# Conectar ao banco de dados
conn = mysql.connector.connect(**conexao)

print('Conexão bem sucedida')

# Criar um objeto cursor
cursor = conn.cursor()

# Consulta para recuperar as tabelas do banco de dados
#consulta = "SHOW TABLES"

# Executar uma consulta SQL
consulta = "SELECT * FROM DADOS_COVID"
cursor.execute(consulta)

# Recuperar os resultados
resultados = cursor.fetchall()

# Obter os nomes das colunas
colunas = [column[0] for column in cursor.description]

# Criar um DataFrame com os resultados
df_covid = pd.DataFrame(resultados, columns=colunas)

# Exportar o DataFrame para um arquivo CSV
df_covid.to_csv('Covid.csv', index=False)

# Fechar o cursor e a conexão
cursor.close()
conn.close()
print('BD Desconectado')


# In[ ]:




