package br.com.experian.ingestion.score.config

import org.apache.spark.sql.SparkSession

object SparkConfig {

  def openSession(): SparkSession = {

    return SparkSession
      .builder()
      .appName("LowLevelKafkaConsumer").master("local[*]")
      .getOrCreate()

  }
}