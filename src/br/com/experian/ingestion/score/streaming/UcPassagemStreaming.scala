package br.com.experian.ingestion.score.streaming

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
import br.com.experian.ingestion.score.service.validation.UcPassagemValidationService
import br.com.experian.ingestion.score.service.validation.UcPassagemLogValidationService

object UcPassagemStreaming {

  def execution(args: Array[String]) {

    val spark = SparkConfig.openSession();

    //    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val inputStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](Array("UC_PASSAGEM_EXC", "UC_PASSAGEM_LOG"), KafkaConfig.configuration()))
    import spark.implicits._
    inputStream.foreachRDD { (rdd, time) =>
      val data = rdd.map(record => record.value)

      if (data.collect().size > 0) {
        //spark.read.schema(schema).json(data)

        val ucPassaDataFrame = spark.read.json(data)

        println(ucPassaDataFrame.getClass.getName())

        //KAFKA UC_PASSAGEM_EXEC
        if (UcPassagemValidationService.validateColumnsDF(ucPassaDataFrame)) {
          
          println("Validado com sucesso !!")

          // chamar curation service

          // chamar pinning service

          // chamar consistency service

          // chamar calculation service

        } else {
          // verificar oque fazer, quando der erro  na validação
          
        }

        //KAFKA UC_PASSAGEM_LOG
        if (UcPassagemLogValidationService.validateColumnsDF(ucPassaDataFrame)) {

          println("Validado com sucesso !!")   
         
          // chamar curation service

          // chamar pinning service

          // chamar consistency service

          // chamar calculation service

        } else {
          // verificar oque fazer, quando der erro  na validação
          
        }

        ucPassaDataFrame.show()

      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}