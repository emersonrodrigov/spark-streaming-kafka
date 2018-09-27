package br.com.experian.ingestion.score.service.validation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object UcPassagemLogValidationService {

  def validateColumnsDF[T](df:Dataset[T]): Boolean = {

        var isValido = true;
        val AUD_ENTTYP = df.select("AUD_ENTTYP").first().getString(0)
        if (AUD_ENTTYP == null || AUD_ENTTYP.trim() == "") {
          isValido = false
        }
      
        val AUD_APPLY_TIMESTAMP = df.select("AUD_APPLY_TIMESTAMP").first().getString(0)
        if (AUD_APPLY_TIMESTAMP == null || AUD_APPLY_TIMESTAMP.trim() == "") {
           isValido = false
        }
      
        val NU_DOCUMENTO_CAD = df.select("NU_DOCUMENTO_CAD").first.getString(0)
        if (NU_DOCUMENTO_CAD == null || NU_DOCUMENTO_CAD.trim() == "") {
           isValido = false
        }
      
        val CO_TIPO_DOC_CAD = df.select("CO_TIPO_DOC_CAD").first().getString(0)
        if (CO_TIPO_DOC_CAD == null || CO_TIPO_DOC_CAD.trim() == "") {
           isValido = false
        }
      
        val NU_CGC_FRN = df.select("NU_CGC_FRN").first().getString(0)
        if (NU_CGC_FRN == null || NU_CGC_FRN.trim() == "") {
          isValido = false
        }
      
      
        val DT_CONSULTA_PSE = df.select("DT_CONSULTA_PSE").first().getString(0)
        if (DT_CONSULTA_PSE == null || DT_CONSULTA_PSE.trim() == "") {
          isValido = false
        }
      
        val HO_CONSULTA_PSE = df.select("HO_CONSULTA_PSE").first().getString(0)
        if (HO_CONSULTA_PSE == null || HO_CONSULTA_PSE.trim() == "") {
          isValido = false
        }
      
        val CO_PRODUTO_PSE = df.select("CO_PRODUTO_PSE").first().getString(0)
        if (CO_PRODUTO_PSE == null || CO_PRODUTO_PSE.trim() == "") {
           isValido = false
        }
      
      
        val CO_TIPO_EMPR_PSE = df.select("CO_TIPO_EMPR_PSE").first().getString(0)
        if (CO_TIPO_EMPR_PSE == null || CO_TIPO_EMPR_PSE.trim() == "") {
           isValido = false
        }
      
        val VL_CONTRATO_PSE = df.select("VL_CONTRATO_PSE").first().getString(0)
        if (VL_CONTRATO_PSE == null || VL_CONTRATO_PSE.trim() == "") {
           isValido = false
        }
      
        val CO_MODALIDADE_PSE = df.select("CO_MODALIDADE_PSE").first().getString(0)
        if (CO_MODALIDADE_PSE == null || CO_MODALIDADE_PSE.trim() == "") {
           isValido = false
        }
      
        val CO_USERIDEXC_PSE = df.select("CO_USERIDEXC_PSE").first().getString(0)
        if (CO_USERIDEXC_PSE == null || CO_USERIDEXC_PSE.trim() == "") {
          isValido = false
        }
      
        val DE_MOTIVOEXC_PSE = df.select("DE_MOTIVOEXC_PSE").first().getString(0)
        if (DE_MOTIVOEXC_PSE == null || DE_MOTIVOEXC_PSE.trim() == "") {
          isValido = false
        }
      
        val QT_CONSULTA_PSE = df.select("QT_CONSULTA_PSE").first().getString(0)
        if (QT_CONSULTA_PSE == null || QT_CONSULTA_PSE.trim() == "") {
           isValido = false
        }
      
        val CO_CDL_ORIGEM_PSE = df.select("CO_CDL_ORIGEM_PSE").first().getString(0)
        if (CO_CDL_ORIGEM_PSE == null || CO_CDL_ORIGEM_PSE.trim() == "") {
           isValido = false
        }
      
        val CO_LISTA_SCORE_PSE = df.select("CO_LISTA_SCORE_PSE").first().getString(0)
        if (CO_LISTA_SCORE_PSE == null || CO_LISTA_SCORE_PSE.trim() == "") {
           isValido = false
        }
      
        val ENTTYPE = df.select("ENTTYPE").first().getString(0)
        if (ENTTYPE == null || ENTTYPE.trim() == "") {
           isValido = false
        }
        
        return isValido;
  }

}