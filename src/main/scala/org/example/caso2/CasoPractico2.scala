package org.example.caso2

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.functions.desc

object CasoPractico2 {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass.getSimpleName)
    Try {
      val levelStatisticsSpark = args(0).toInt
      val partitionId= args(1)

      logger.info("Lectura de datos parquet")
      val spark: SparkSession = SparkSession.builder().
        enableHiveSupport().
        getOrCreate()

      val ConsRutaPrmProcessed="D:/processed/"
      val dfMarcoDatos = spark.read.parquet(ConsRutaPrmProcessed)

      logger.info("Evaluacion de nivel de estadisticas")
      evaluateStatisticsSpark(
        levelStatisticsSpark,
        dfMarcoDatos,
        partitionId
      )



    } match {
      case Failure(exception) => {
        logger.error("ERROR", exception)
        logger.error(s"ERROR : $exception + ${exception.getCause}")
        throw new Exception(s"ERROR : $exception + ${exception.getCause}", exception)
      }
      case Success(_) => {
        logger.info("PROCESS OK")
      }
    }
  }

  def evaluateStatisticsSpark(levelStatistics:Int,
                              df:DataFrame,
                              partitionId:String):Unit={
    val ConsZeroValue=0
    val ConsMessageCount=Seq(2,4)
    val ConsMessageCountGrouped=Seq(3,4)
    val ConsMessageShowData=Seq(8)

    val numberPartitions = df.rdd.getNumPartitions

    if(levelStatistics>ConsZeroValue){
      showMessageLevelMorethanZero(df, numberPartitions)
    }
    if(ConsMessageCount.contains(levelStatistics)){
      showMessageCount(df)
    }
    if(ConsMessageCountGrouped.contains(levelStatistics)){
      showMessageCountGrouped(df, partitionId, numberPartitions)
    }
    if(ConsMessageShowData.contains(levelStatistics)){
      showMessageShowData(df)
    }

  }

  def showMessageLevelMorethanZero(df:DataFrame,numberOfPartitions:Long):Unit={
    val sizeInBytes: BigInt = df.queryExecution.optimizedPlan.stats.sizeInBytes
    val sizeInMegabytes: Float = sizeInBytes.toFloat / 1024.toFloat / 1024.toFloat
    val sizeEstimatorInMegabytes: Float = SizeEstimator.estimate(df).toFloat / 1024.toFloat / 1024.toFloat

    println(s""" *** STATS *** Df Stats PlanSizeMB: ${sizeInMegabytes},
           EstimatorSizeMB: ${sizeEstimatorInMegabytes},
           PartitionNum: ${numberOfPartitions},
           PartitionSizeMB:${sizeInMegabytes / numberOfPartitions},
           PartitionSizeMB:${sizeEstimatorInMegabytes / numberOfPartitions}""")
  }

  def showMessageCount(df:DataFrame):Unit={
    println(s"*** STATS *** Df Partitioon record count ${df.count()}")
  }

  def showMessageCountGrouped(df:DataFrame,
                              partitionId:String,
                              numberPartitions:Int):Unit={
    println(s"*** STATS *** Df Partitioon Info")
    val numberRowsSample=evaluateNumberPartitions(numberPartitions)
    df.groupBy(partitionId)
      .count()
      .orderBy(desc("count"))
      .show(numberRowsSample)
  }

  def showMessageShowData(df:DataFrame):Unit={
    println(s"*** STATS *** Show data:")
    df.show()
  }

  def evaluateNumberPartitions(numberPartitions:Int):Int={
    val ConsDefaultShow=200
    if(numberPartitions>0) numberPartitions
    else ConsDefaultShow
  }


}
