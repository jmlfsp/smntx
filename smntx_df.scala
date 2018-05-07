import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import sqlContext.implicits._


case class LogRecord( hostname: String, data: String, erro: String, qtd_bytes: String )


object Logs {
    val PATTERN = "(.+)( - - \\[)(\\d{2}/\\w+/\\d{4})(:)(\\d{2}:\\d{2}:\\d{2})(.*)(\\d{4})(] \\\")(.+\\\" )(\\d{3})( )(.*)".r
 
    def parseLogLine(log: String): LogRecord = {

        val res = PATTERN.findFirstMatchIn(log)
 
          val m = res.get
          // NOTE:   HEAD does not have a content size.
          if (m.group(12).equals("-")) {
            LogRecord(m.group(1), m.group(3), m.group(10), "0")
          }
          else {
            LogRecord(m.group(1), m.group(3), m.group(10), m.group(12))
          }
    }
	
	val logFile = sc.textFile("file:///home/training/Downloads/NASA*.gz")
	val accessLogs = logFile.map(parseLogLine)
	val sqlContext = new SQLContext(sc)
	accessLogs.take(5)
	val df1 = accessLogs.toDF()
	df1.registerTempTable("accessLogsDF")
    df1.printSchema()
	df1.show(5)
	
	df1.select($"hostname").show(5)
	
	
}
