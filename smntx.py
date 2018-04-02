from pyspark import SparkContext
import re, logging, shutil, os

# Configuracoes da geracao do log
logging.basicConfig(\
filename='/home/training/Downloads/SMNTX/smntx.log'\
,filemode='w'\
,format='[%(asctime)s] %(message)s'\
,datefmt='%m/%d/%Y %I:%M:%S %p'\
,level=logging.INFO\
)

# Formatacao Log
separador="*************************************************************"
tabulacao='\n\t\t\t\t'

logging.info(separador)
logging.info('INICIO EXECUCAO APLICACAO')
logging.info(separador)
logging.info('DISCLAIMER')
logging.info('Versao Python: Python 2.6.6')
logging.info('Versao Spark: Spark 1.0.0')
logging.info('Pacotes: logging, re, shutil, os, pyspark(SparkContext)')
logging.info('Candidata: JML')

logging.info(separador)
logging.info('PREPARO APLICACAO')

logging.info('Verifica Execucoes Anteriores')
if os.path.exists('/home/training/Downloads/SMNTX/Respostas'):
	logging.info('Deleta arquivos antigos encontrados')
	shutil.rmtree('/home/training/Downloads/SMNTX/Respostas')
else:
	logging.info('Nova Execucao. Nao foram encontrados arquivos a serem deletados')
	
logging.info('Reserva Recurso - Iniciando SparkContext')
sc = SparkContext("local")

logging.info('Identificando diretorio e arquivos para analise')
nasa = "file:/home/training/Downloads/SMNTX/ORIGEM/*.gz"

# Alocando em memoria
n = sc.textFile(nasa).cache()

def verifica_numero(numeros):
## Funcao para verificar se ha apenas numeros no conteudo
## Se tiver, retorna o conteudo, caso contrario, retorna falso.

    if len(re.findall('[0-9]+',numeros)) == 0:
        return False
    else:
        return numeros

# Respostas
# 1
logging.info(separador)
logging.info('\
INICIO PROCESSO_QUESTAO_1 {0}\
01 - Identifica todos os hosts,{0}\
02 - Seleciona distintos,{0}\
03 - Contagem'.format(tabulacao))
hosts_unicos = n\
.map(lambda x: x.split(" ")[0]) \
.distinct() \
.count()
logging.info('FIM PROCESSO_QUESTAO_1 ')

logging.info(separador)
logging.info('\
INICIO PREPARO_QUESTAO_2,3,4 {0}\
01 - Filtra linhas dos log com erros 404'.format(tabulacao))
e404 = n\
.filter(lambda x: " 404 " in x )
logging.info('FIM PREPARO_QUESTAO_2,3,4 ')

# 2
logging.info(separador)
logging.info('\
INICIO PROCESSO_QUESTAO_2 {0}\
02 - A partir das linhas com erro 404, Realiza contagem'.format(tabulacao))
qtd_404 = e404\
.count()
logging.info('FIM PROCESSO_QUESTAO_2 ')

# 3
logging.info(separador)
logging.info('\
INICIO PROCESSO_QUESTAO_3 {0}\
02 - A partir das linhas com erro 404, Seleciona hosts,{0}\
03 - Soma a quantidade de erros por host,{0}\
04 - Ordena da maior quantidade para a menor,{0}\
05 - Seleciona o top 5'.format(tabulacao))
top5_hosts_404 = e404\
.map(lambda y: (y.split(" ")[0],1)) \
.reduceByKey(lambda x,y:x+y) \
.map(lambda (x,y): (y,x)) \
.sortByKey(ascending=False) \
.take(5)
logging.info('FIM PROCESSO_QUESTAO_3 ')

# 4
logging.info(separador)
logging.info('\
INICIO PROCESSO_QUESTAO_4 {0}\
02 - A partir das linhas com erro 404, Seleciona as datas, {0}\
03 - Soma a quantidade de erros por data'.format(tabulacao))
e404_dia = e404\
.map(lambda x: (x.split(" ")[3].split(":")[0][1:],1)) \
.reduceByKey(lambda x,y: x+y)

logging.info('\t04 - Gera resultado')
qtd_404_dia = e404_dia\
.collect()

logging.info('\t05 - Salva resultado em arquivo')
qtd_404_dia_arq = e404_dia\
.saveAsTextFile("file:/home/training/Downloads/SMNTX/Respostas/Q4")

logging.info('FIM PROCESSO_QUESTAO_4 ')

# 5
logging.info(separador)
logging.info('INICIO PROCESSO_QUESTAO_5 {0}\
01 - Filtra as linhas que possuem numeros em "bytes"{0}\
02 - Seleciona os bytes {0}\
03 - Soma todos os bytes '.format(tabulacao))
total_bytes = n\
.filter(lambda x: verifica_numero(x.split(" ")[-1])) \
.map(lambda x: int(x.split(" ")[-1])) \
.sum()
logging.info('FIM PROCESSO_QUESTAO_5 ')

logging.info('{0}\n\n\
RELATORIO RESPOSTAS CONSOLIDADAS : \n\n\n\
1 - Numero de hosts unicos: {1} \n\n\
2 - Total de erros 404: {2} \n\n\
3 - Os 5 URLs que mais causaram erro 404: \n{3} \n\n\
4 - Quantidade de erros 404 por dia: \n{4} \n\n\
5 - O total de bytes retornados: {5} \n\n\
FIM RELATORIO COM AS RESPOSTAS CONSOLIDADA\n\n'
.format(separador,hosts_unicos,qtd_404,top5_hosts_404,qtd_404_dia,total_bytes))
logging.info('FIM GERACAO RELATORIO')
logging.info(separador)
logging.info('INICIO POS PROCESSAMENTO APLICACAO')
logging.info('Desalocando Recurso - Parando SparkContext')
sc.stop()
logging.info('FIM POS PROCESSAMENTO APLICACAO')
logging.info(separador)
logging.info('FIM EXECUCAO APLICACAO')
logging.info(separador)
