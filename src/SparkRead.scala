

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext };
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.{ get_json_object, json_tuple }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.count
import br.com.experian.ingestion.score.config.KafkaConfig
import br.com.experian.ingestion.score.config.SparkConfig




object SparkRead {
  

  def main(args: Array[String]): Unit = {

    
    KafkaConfig.configuration();
    
    // CONFIG SPARK LOCAL
    System.setProperty("hadoop.home.dir", "C://winutils")

    // LOGS
    import org.apache.log4j.{ Level, Logger }
    // This import is needed to use the $-notation
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
 
    val spark = SparkConfig.openSession(); 
      
      
      SparkSession
      .builder()
      .appName("LowLevelKafkaConsumer").master("local[*]")
      .getOrCreate()

    //Create SparkContext
    //    val conf = new SparkConf()
    //      .setMaster("local[*]")
    //      .setAppName("LowLevelKafkaConsumer")

    //    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    //val sqlContext = new SQLContext(sc)

    // config kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", // servidor kakfa local
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-read",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    //    var streamingInputDF =
    //      spark.readStream
    //        .format("kafka")
    //        .option("kafka.bootstrap.servers", "<server:ip")
    //        .option("subscribe", "topic1")
    //        .option("startingOffsets", "latest")
    //        .option("minPartitions", "10")
    //        .option("failOnDataLoss", "true")
    //        .load()

    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array("sql-insert"),  KafkaConfig.configuration()))
    import spark.implicits._
    inputStream.foreachRDD { (rdd, time) =>
      val data = rdd.map(record => record.value)

      if (data.collect().size > 0) {
        //spark.read.schema(schema).json(data)

               val df = spark.read.json(data)
         
               val teste = df.select("AUD_ENTTYP").first().getString(0)
               
               if(teste.trim() == "" || teste == null){
                 
                 
                 
               }
               
               
               
//               println(teste.getClass.getName)
               println(teste)
               
       
       
      }

      //      json.show
    }

    
    ssc.start()
    ssc.awaitTermination()
  }

}




