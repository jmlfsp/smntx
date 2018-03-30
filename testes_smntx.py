from pyspark import SparkContext

nasa1 = "file:/home/training/Downloads/NASA_access_log_Jul95.gz"
nasa2 = "file:/home/training/Downloads/NASA_access_log_Aug95.gz"

# Criando Contexto
sc = SparkContext("local", "interview")

n1 = sc.textFile(nasa1)
n2 = sc.textFile(nasa2)

# T - Concatenando os 2 arquivos
final = n1.union(n2)

# T - Buscando os hostnames unicos
result = final.map(lambda x: x.split(" ")[0]).distinct()

# A - Contando hostnames unicos
r1 = result.count()


# 
#r_final = "1 - Numero de hosts unicos: {0}\n ".format(r1)
#2 - Total de erros 404: {1} \n \
#3 - Os 5 URLs que mais causaram erro 404: {2} \n \
#4 - Quantidade de error 404 por dia {3} \n \
#5 - O total de byter retornados. {4} \n


url_404 = n.map(lambda x: x.split(" ")[0],x.split(" ")[3]).distinct()


total = 3461613
qtd_host_unico = 137979
qtd_404 = 20901


r1.saveAsTextFile("/home/training/Downloads/Respostas_Lab.txt")



(.*) - - .(\d{2}/.+/\d{4}):(\d{2}:\d{2}:\d{2})\s-(\d{4}).\s\"(.*)\s(.*)\s(.*)\"\s(\d{3})\s(\d*)
campo1 = n.map(lambda x: (re.split(padrao,x)))

'(.*)','( - - .)','(\d{2}/.+/\d{4})'

n.map(lambda x: x.split('(.*)','( - - .)','(\d{2}/.+/\d{4})')).\
    map(lambda y: (y[0], y[2], y[1]))


(.*)( - - .)(\d{2}/.+/\d{4})(:)(\d{2}:\d{2}:\d{2})(\s-)(\d{4})(\]\s\")(.*)(\s)(.*)(\s)(.*)(\s)(\d*)(\s)(\d+)
campo1 = n.map(lambda x: re.split(padrao,x)[1])



campo1 = n.map(lambda x: re.split(padrao,x)[1],re.split(padrao,x)[] )

sc = SparkContext("local", "interview")
nasa = "file:/home/training/Downloads/*.gz"
n= sc.textFile(nasa)



padrao=padrao="(.*)( - - .)(\d{2}/.+/\d{4})(:)(\d{2}:\d{2}:\d{2})(\s-)(\d{4})(\]\s\")(.*)(\s)(.*)(\s)(.*)(\s)(\d*)(\s)(\d+)"
campo1 = n.map(lambda x: re.split(padrao,x)[1]+" "+re.split(padrao,x)[3]+" "+re.split(padrao,x)[15]+" "+re.split(padrao,x)[17])
t = campo1.map(lambda x: x.split(" ",3))
t2 = t.map(lambda x: Row(host=x[0],date=x[1],cod=x[2],bytes=x[3]))


t2 = t.map(lambda x: {'host':x[0],'date':x[1],'cod':x[2],'bytes':x[3]})



schema = StructType(Array(StructField("host",StringType,true),StructField("date",StringType,true),StructField("code",IntegerType,true),StructField("bytes",IntegerType,true)))

from pyspark.sql import SQLContext, Row
from pysparl.sql import types.{StructType, StructField, StringType}

n.filter(lambda x: " 404 " in x).count()


5775
hosts_404_agg = hosts_404.reduceByKey(lambda x,y: x+y)


hosts_unicos = n.map(lambda x: x.split(" ")[0]).distinct().count()
qtd_404 = n.filter(lambda x: " 404 " in x).count()
hosts_404 = n.filter(lambda x: " 404 " in x).map(lambda y: (y.split(" ")[0],1))

teste = hosts_404.sortByKey(ascending=False)
top_red = teste.reduceByKey(lambda x,y:x+y)
result = top_red.map(lambda (x,y): (y,x)).sortByKey(ascending=False)
result.saveAsTextFile(file:/home/training/Downloads/Respostas_Lab.*)





from pyspark import SparkContext
import re

sc = SparkContext("local")

nasa = "file:/home/training/Downloads/*.gz"
n = sc.textFile(nasa).cache()

# 1
hosts_unicos = n.map(lambda x: x.split(" ")[0]) \
.distinct() \
.count()

# 2
qtd_404 = n.filter(lambda x: " 404 " in x)
.count()

# 3
top5_hosts_404 = n.filter(lambda x: " 404 " in x) \
.map(lambda y: (y.split(" ")[0],1)) \
.sortByKey(ascending=False) \
.reduceByKey(lambda x,y:x+y) \
.map(lambda (x,y): (y,x)) \
.sortByKey(ascending=False) \
.take(5)

# 4

#1 - Numero de hosts unicos: {0}\n \
#2 - Total de erros 404: {1} \n \
#3 - Os 5 URLs que mais causaram erro 404: {2} \n \
#4 - Quantidade de erros 404 por dia {3} \n \
#5 - O total de byter retornados. {4} \n

qtd_404_dia = n.filter(lambda x: " 404 " in x)\
.map(lambda x: (x.split(" ")[3].split(":")[0][1:],1))\
.reduceByKey(lambda x,y: x+y)\
.saveAsTextFile("file:/home/training/Downloads/R4")

# 5
bytes = n \
.map(lambda y: y.split(" ")[10]) \


padrao=padrao="(.*)( - - .)(\d{2}/.+/\d{4})(:)(\d{2}:\d{2}:\d{2})(\s-)(\d{4})(\]\s\")(.*)(\s)(.*)(\s)(.*)(\s)(\d*)(\s)(\d+)"


sc.stop()
