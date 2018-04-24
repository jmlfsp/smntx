import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object App {
  def main(args: Array[String]): Unit = {

    nasa

  }

  def nasa: Unit = {
    val conf = new SparkConf().setAppName("teste").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Teste")

      .enableHiveSupport()
      .getOrCreate()

    /*Lendo os arquivos de origem*/
    val distData = sc.textFile("file:///home/semantix/Downloads/NASA*")
    distData.cache()


    /*questao 1    */
    println(distData.map(x => x.split(" ")(0)).distinct().count())  /* qtd hosts unicos 3461613 */


    /* Filtrando apenas linhas com erro 404*/
    val regex_404 = "([\\w|\\.]+)(.*\\[)(\\d{2}/\\w+/\\d{4})(:)(\\d{2}:\\d{2}:\\d{2})(.*)(\\d{4})(] \\\")(.+\\\" )(404)( )(.*)"

    val erro404 = distData.filter(x => x matches regex_404)
    erro404.cache()


    /*questao 2*/
    print(erro404.count())


    /*questao 3*/
    erro404.map(x => (x.split(" ")(0),1)).reduceByKey((x,y) => x+y).map(x=>(x._2->x._1)).sortByKey(ascending=false).take(5).foreach(println(_))
    

  }
}
