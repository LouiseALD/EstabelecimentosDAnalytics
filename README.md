<h1>Estabelecimentos Analytics</h1>

O projeto EstabelecimentosAnalytics é um data-pipeline de análise e processamento de dados relacionados a estabelecimentos empresariais. Ele oferece uma variedade de funcionalidades para extrair informações relevantes e insights a partir de conjuntos de dados do Cadastro Nacional da Pessoa Jurídica (CNPJ) disponibilizados pelo governo.

O projeto está divido em 3 etapas.
Etapa 1 (Célula 1) - Extração dos dados ( fonte: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj )
Etapa 2 (Célula 2) - Armazenamento dos dados extraídos no MongoBD
Etapa 3 (Célula 3) - Realização das consultas no banco, transformações das consultas em CSV e correlação de tabela.

VIDEO DE APRESENTAÇÃO: https://youtu.be/XswCzip13Ss

## Pré-requisitos ( Linguagens, dependências e bibliotecas )

:warning: [Python]
:warning: [MongoBD]
:warning: [Excel]
:warning: [CSV]
:warning: [Time]
:warning: [Pymongo]
:warning: [Os]
:warning: [Dask]
:warning: [Collections]
:warning: [Zipfile]
:warning: [Json]
:warning: [Datetime]
:warning: [Dateutil]
:warning: [Geopy]
:warning: [Pandas]
:warning: [Re]
:warning: [Functools]
:warning: [Selenium]


<h2> Passo a passo do funcionamento</h2>


<h2> Etapa 1 - Extração dos dados </h2>

Esta etapa do código é responsável por automatizar o processo de download dos arquivos CSVs contendo os dados do Cadastro Nacional da Pessoa Jurídica (CNPJ) do site dados.gov.br.
Obs: Os arquivos serão todos baixados localmente, com o tempo de download variando de acordo com a velocidade da internet utilizada.


## Import das bibliotecas necessárias:

```
from selenium import webdriver
import time
from selenium.webdriver.common.by import By

```
    
## Configuração das opções do WebDriver para o Chrome.
As opções configuradas incluem ignorar erros de certificado, abrir em modo anônimo e executar em segundo plano (headless). ( Essas opções podem ser ajustadas de acordo com necessidades )

```
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')

``` 


## Inicialização do WebDriver do Chrome.

```
driver = webdriver.Chrome(executable_path='C:\chromedriver')

``` 


## Navegação até a página do dados.gov.br contendo os dados do CNPJ

```
driver.get("https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj")
time.sleep(2)

``` 

## Clicando no botão de download dos arquivos.

```
driver.find_element("xpath", '/html/body/div/section/div/div[3]/div[2]/div[3]/div[2]').click()
``` 

## Localizando e clique nos botões de download para os recursos desejados.


```
resources = [resource.find_element("xpath", './/button[@id="btnDownloadUrl"]') for resource in 
             driver.find_elements("xpath", '//*[@id="collapse-recursos"]//div[@class="row flex mb-5"]') 
             if "Estabelecimento" in resource.text ]

for button in resources:
    time.sleep(1)
    button.click()

``` 

## Encerramento da sessão do WebDriver.

```
driver.close()
``` 

<h2> Etapa 2 -  Armazenamento dos dados extraídos no MongoBD </h2>


## Import das bibliotecas necessárias:

```
import pymongo
import os
import zipfile
import pandas as pd
import time
from datetime import datetime
import dateutil.parser

``` 

## Defina a função dateparse para fazer o parsing das datas.

```
def dateparse(s):
    try:
        return datetime.strptime(s, '%Y%m%d')
    except ValueError:
        return datetime.min

``` 

## Conexão do MongoDB.

```
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["estabelecimentos"]
mycol = mydb["estabelecimentos"]

``` 


## Definição do caminho para os arquivos ZIP contendo os dados.

```
path = r"C:\Users\louis\Downloads"

``` 

## Iteração dos arquivos ZIP e inserção dos dados no MongoDB.


```
for i in range(10):
    file = os.path.join(path, f"Estabelecimentos{i}.zip")
    zf = zipfile.ZipFile(file) 
    filename = zf.namelist()[0]
    chunksize = 10 ** 4
    names = ["CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "IDENTIFICADOR", "FANTASIA", "SIT_CADASTRAL", "DATA_SIT_CADASTRAL", "MOT_SIT_CADASTRAL", "NOME_EXT", "PAIS", "DATA_INI", "CNAE_PRINCIPAL", "CNAE_SECUNDARIA", "TIPO_LOGRADOURO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CEP", "UF", "MUNICIPIO", "DDD1", "TEL1", "DDD2", "TEL2", "DDD_FAX", "FAX", "EMAIL", "SIT_ESP","DATA_ST_ESP"]
    date_cols = ['DATA_SIT_CADASTRAL', 'DATA_INI']

    print(f"Estabelecimentos{i}")
    
    start = time.time()

    with pd.read_csv(zf.open(filename), dtype = str, date_parser=dateparse, parse_dates=date_cols, chunksize = chunksize, names = names, header = None, encoding="ISO-8859-1", delimiter=';', keep_default_na=False) as reader:
        for chunk in reader:
             mycol.insert_many(chunk.to_dict(orient='records'))
        print('.', end='')
        
print("\nTotal Write Time on mongo: ", (time.time() - start), "sec")

``` 

## Definição do tamanho de chuck ( blocos ) para processamentos dos dados

``` 
chunksize = 10 ** 4

```

## Definindo os nomes das colunas para o DataFrame.

``` 
names = ["CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "IDENTIFICADOR", "FANTASIA", "SIT_CADASTRAL", "DATA_SIT_CADASTRAL", "MOT_SIT_CADASTRAL", "NOME_EXT", "PAIS", "DATA_INI", "CNAE_PRINCIPAL", "CNAE_SECUNDARIA", "TIPO_LOGRADOURO", "LOGRADOURO", "NUMERO", "COMPLEMENTO", "BAIRRO", "CEP", "UF", "MUNICIPIO", "DDD1", "TEL1", "DDD2", "TEL2", "DDD_FAX", "FAX", "EMAIL", "SIT_ESP","DATA_ST_ESP"]

```

## Defina as colunas de data para fazer o parsing.

``` 
date_cols = ['DATA_SIT_CADASTRAL', 'DATA_INI']

```

## Iteração dos chunks do arquivo CSV e inserção dos dados no MongoDB.

```
with pd.read_csv(zf.open(filename), dtype=str, date_parser=dateparse, parse_dates=date_cols, chunksize=chunksize, names=names, header=None, encoding="ISO-8859-1", delimiter=';', keep_default_na=False) as reader:
    for chunk in reader:
        mycol.insert_many(chunk.to_dict(orient='records'))
        print('.', end='')
```

## Print do tempo de gravação no MongoBD

```
print("\nTotal Write Time on mongo: ", (time.time() - start), "sec")

```


<h2> Etapa 3 - Realização das consultas no banco, transformações das consultas em CSV e correlação de tabela. </h2>

Nesse código, ocorre a realização das consultas dos dados inseridos no banco, além de que, para cada consulta realizada, serão salvos os dados em arquivos no formato CSV.

Ordens de consultas ( Podendo ser consultado de acordo com o comentário do código )

4.1 - getActiveEnterprisesPercentage(collection)
Esta função calcula a porcentagem de empresas ativas em relação ao total de empresas no banco de dados. Ela utiliza a biblioteca pandas para manipular os dados e retorna um DataFrame contendo o resultado.

4.2 - getRestaurantsByYear(collection)
Essa função retorna a quantidade de restaurantes registrados no banco de dados agrupados por ano. Ela utiliza expressões regulares e a biblioteca pandas para processar os dados e retorna um DataFrame com o resultado.

4.3 - getDistinctAddress(collection, prefix)
Essa função recupera os endereços distintos dos estabelecimentos no banco de dados que correspondem a um determinado prefixo de CEP. Ela utiliza expressões regulares e a biblioteca pandas para manipular os dados e retorna uma lista de objetos contendo os detalhes dos endereços.

4.3 - getCnpjsByDistanceToOrigin(collection, ceps, locator, pre_coords={})
Essa função calcula a distância entre os endereços dos estabelecimentos e um endereço de origem específico. Ela utiliza a biblioteca geopy para obter as coordenadas geográficas dos endereços e calcular as distâncias. A função retorna um dicionário com os pares de CEPs e CNPJs que estão a uma distância de até 5 km do endereço de origem.

4.3 - getExistentCoords(collection)
Essa função recupera as coordenadas geográficas existentes no banco de dados.

updateCoords(collection, coords_to_update)
Essa função atualiza as coordenadas geográficas no banco de dados com as novas informações fornecidas.

insertCoords(collection, coords)
Essa função insere as coordenadas geográficas no banco de dados.

4.4 - getCnaeCorrelation(collection)
Essa função calcula a correlação entre os códigos CNAE (Classificação Nacional de Atividades Econômicas) principais e secundários dos estabelecimentos. Ela utiliza a biblioteca pandas para manipular os dados e retorna um DataFrame com os resultados da correlação.

## Observações:
    
Configuração do serviço de geocodificação:

3 - Criação de uma instância do geolocalizador Nominatim a partir da classe Nominatim da biblioteca geopy.geocoders.
Conexão ao banco de dados:

4 - Criação de uma instância do cliente MongoDB através da classe MongoClient da biblioteca pymongo.
Definição do banco de dados e da coleção a serem utilizadas.
Execução das funcionalidades:

5 - Utilização das funções definidas para realizar as consultas e análises no banco de dados.
Os resultados são armazenados em DataFrames da biblioteca pandas e salvos em arquivos CSV.
Controle do tempo de execução:

6 - Utilização das funções time.time() para medir o tempo de execução das operações.
Impressão do tempo total de gravação no MongoDB.

