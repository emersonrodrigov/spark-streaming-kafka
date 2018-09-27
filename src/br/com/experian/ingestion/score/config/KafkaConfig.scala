package br.com.experian.ingestion.score.config

import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.Map

object KafkaConfig {
  
  
  
  def configuration() : Map[String, Object] = {
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", // servidor kakfa local
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-read",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
      
      return kafkaParams;
    
  }
   
  
}