import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object App {
  def main(args: Array[String]): Unit = {

    nasa

  }

  def extracao(linha: String, campo: String): String = {

    val regex_arq = "(.+)( - - \\[)(\\d{2}/\\w+/\\d{4})(:)(\\d{2}:\\d{2}:\\d{2})(.*)(\\d{4})(] \\\")(.+\\\" )(\\d{3})( )(.*)".r
    val numero = "(\\d+)".r

    linha match {
      case regex_arq( hostname, f1, data, f2, hora, f3, codigo, f4, requisicao, erro , f5, qtd_bytes) =>  {
        campo match {
            case "hostname" => hostname
            case "data" => data
            case "qtd" => { qtd_bytes match { case numero(qtd) => qtd; case _ => "0" } }
            case _ => "_"
        }
      }
      case _ => "0"
    }

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

    /*questao 1*/
    println(distData.map(x => extracao(x,campo="hostname")).distinct().count())


    /* Filtrando apenas linhas com erro 404*/
    val regex_404 = "([\\w|\\.]+)(.*\\[)(\\d{2}/\\w+/\\d{4})(:)(\\d{2}:\\d{2}:\\d{2})(.*)(\\d{4})(] \\\")(.+\\\" )(404)( )(.*)"

    val erro404 = distData.filter(x => x matches regex_404)
    erro404.cache()

    
    /*questao 2*/
    print(erro404.count())


    /*questao 3*/
    erro404.map(x => (extracao(x,campo="hostname"),1)).reduceByKey((x,y) => x+y).map(x=>(x._2->x._1)).sortByKey(ascending=false).take(5).foreach(println(_))


    /*questao 4 */
    erro404.map( x => (extracao(x, campo= "data"),1)).reduceByKey((x,y) => x+y).collect().foreach(println)


    /*questao 5 */
    val total_bytes = distData.map(x => extracao(x,"qtd").toInt).sum()
    println(f"$total_bytes%1.0f")


  }
}
