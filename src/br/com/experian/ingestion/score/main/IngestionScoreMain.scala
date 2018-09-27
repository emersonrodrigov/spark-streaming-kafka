package br.com.experian.ingestion.score.main

import br.com.experian.ingestion.score.streaming.UcPassagemStreaming  

object IngestionScoreMain {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{ Level, Logger }
    // This import is needed to use the $-notation
    
    // Desativando LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    // CONFIG SPARK LOCAL
    System.setProperty("hadoop.home.dir", "C://winutils")
    
    // UC_PASSAGEM_EXC
    UcPassagemStreaming.execution(args);

    // UC_PASSAGEM_LOG
    //UcPassagemLogStreaming.execution(args);

  }

}