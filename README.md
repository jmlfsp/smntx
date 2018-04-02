## Engenharia de Dados - Jackeline Melo de Lima

## Respostas - Parte Teórica
### 1) Qual o objetivo do comando cache em Spark?
Alocar os dados para serem armazenados em memoria, podendo melhorar a performance pois não haverá operações de I/O para acesso aos dados.

### 2) O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Porque, além de poder trabalhar o dado na memória, o Spark otimiza a alocação de recursos para os passos de execução, gerando o melhor plano.

### 3) Qual a função do SparkContext?
É uma função para que o Spark prepare/reserve os recursos necessários no cluster (computação e armazenamento) para a execução da aplicação.

### 4) Explique com suas palavras o que é Resilient Distributed Datasets (RDDs).
É a unidade fundamental e imutável do Spark. É resiliente (Resilient) porque confia na origem da informação para, sempre que necessário, recriar os dados em memória. Além disso, tem fator de replicação para processar (e, se necessário, armazenar) os dados distribuídos em memória (Distributed) nas máquinas do cluster, garantindo grande poder computacional para processamento de vários tipos de dados (Datasets) diferentes, como vídeos, imagens, textos, etc.

### 5) GroupByKey é menos eficiente que reduceByKey em grandes datasets. Por quê?
Porque o groupByKey combina os dados apenas após o shuffle, já o reduceByKey inicia combinando as chaves semelhantes nos blocos antes de realizar o shuffle. Dessa forma, menos dados são trafegados.

### 6) Explique o que o código Scala abaixo faz.
Através de um contexto Spark, identifica um arquivo texto do HDFS. Em seguida, realiza 3 transformações e 1 ação. A primeira transformação identifica os espaços em cada linha para separar as palavras. A segunda transformação atrela para cada palavra, o valor "1", gerando pares de chave e valor. A terceira transformação combina as chaves das palavras e soma os valores. Enfim, a ação salva esse conjunto de transformações em um arquivo de texto no HDFS. É o wordcount em Scala.

## Respostas - Parte prática

Publicado o log da aplicação (smntx.log) e o código da aplicação (smntx.py) construída para a realização dos testes. É gerado um relatório com as respostas para cada pergunta.
A aplicação foi desenvolvida em Python (v.2.6.6) e Spark (v.1.0.0), utilizando RDDs e foram utilizados os pacotes : logging, re, shutil, os, pyspark(SparkContext).
Para a construção da aplicação, foram consultados materiais das minhas aulas, além das documentações do Python e Spark, bem como StackOverflow.

### 1 - Numero de hosts unicos: 
137979 

### 2 - Total de erros 404: 
20901 

### 3 - Os 5 URLs que mais causaram erro 404: 
[(251, u'hoohoo.ncsa.uiuc.edu'), (157, u'piweba3y.prodigy.com'), (132, u'jbiagioni.npt.nuwc.navy.mil'), (114, u'piweba1y.prodigy.com'), (91, u'www-d4.proxy.aol.com')] 

### 4 - Quantidade de erros 404 por dia: 
[(u'07/Jul/1995', 570), (u'28/Aug/1995', 410), (u'04/Aug/1995', 346), (u'10/Aug/1995', 315), (u'24/Aug/1995', 420), (u'21/Jul/1995', 334), (u'27/Aug/1995', 370), (u'10/Jul/1995', 398), (u'16/Aug/1995', 259), (u'17/Jul/1995', 406), (u'01/Jul/1995', 316), (u'13/Aug/1995', 216), (u'22/Aug/1995', 288), (u'20/Jul/1995', 428), (u'06/Jul/1995', 640), (u'01/Aug/1995', 243), (u'08/Aug/1995', 391), (u'14/Aug/1995', 287), (u'20/Aug/1995', 312), (u'06/Aug/1995', 373), (u'25/Aug/1995', 415), (u'29/Aug/1995', 420), (u'09/Jul/1995', 348), (u'05/Jul/1995', 497), (u'03/Jul/1995', 474), (u'19/Aug/1995', 209), (u'26/Jul/1995', 336), (u'28/Jul/1995', 94), (u'25/Jul/1995', 461), (u'02/Jul/1995', 291), (u'09/Aug/1995', 279), (u'14/Jul/1995', 413), (u'05/Aug/1995', 236), (u'17/Aug/1995', 271), (u'03/Aug/1995', 304), (u'11/Aug/1995', 263), (u'22/Jul/1995', 192), (u'23/Aug/1995', 345), (u'04/Jul/1995', 359), (u'12/Jul/1995', 471), (u'26/Aug/1995', 366), (u'15/Jul/1995', 254), (u'23/Jul/1995', 233), (u'18/Jul/1995', 465), (u'12/Aug/1995', 196), (u'31/Aug/1995', 526), (u'21/Aug/1995', 305), (u'15/Aug/1995', 327), (u'27/Jul/1995', 336), (u'08/Jul/1995', 302), (u'16/Jul/1995', 257), (u'19/Jul/1995', 639), (u'07/Aug/1995', 537), (u'18/Aug/1995', 256), (u'24/Jul/1995', 328), (u'13/Jul/1995', 532), (u'11/Jul/1995', 471), (u'30/Aug/1995', 571)] 

### 5 - O total de bytes retornados: 
65524314915 
