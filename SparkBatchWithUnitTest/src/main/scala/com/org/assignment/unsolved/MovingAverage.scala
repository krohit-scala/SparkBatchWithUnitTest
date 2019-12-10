package com.org.assignment.unsolved

import com.org.assignment.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window._


object OutputWriter {
  def write(outDF: DataFrame, path: String) = {
    outDF.write.format("csv").option("path", path).save
  }
}

case class MovingAverage(sparkSession: SparkSession, stockPriceInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
    
    val inputDf = sparkSession.read
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("path", stockPriceInputDir)
                    .load
    
    val windowSpec = Window.partitionBy(col("stockId")).orderBy(col("stockId"), col("timeStamp"))
    val stockWithRowNumber = inputDf.withColumn(
                               "rn",
                               row_number().over(windowSpec)
                             )
    
    val a = stockWithRowNumber.select(
                        col("stockId"), 
                        col("timeStamp"), 
                        when(col("stockPrice").isNull, 0).otherwise(col("stockPrice")).alias("stockPrice"), 
                        col("rn")
                    )

    val b = stockWithRowNumber.select(
                        col("stockId").alias("sid"),
                        col("timeStamp").alias("ts"), 
                        when(col("stockPrice").isNull, 0).otherwise(col("stockPrice")).alias("stockPrice1"),
                        col("rn").alias("rn1")
                    )
    
    val joinCondition = (col("stockId") === col("sid")) && ( col("rn") >= col("rn1") ) && (col("rn") - col("rn1") < size)
    
    val joinedDf = a.join(
                    b,
                    joinCondition,
                    "left"
                  ).select(
                      col("stockId"), 
                      col("timeStamp"), 
                      when(col("stockPrice").isNull, 0).otherwise(col("stockPrice")).alias("stockPrice"), 
                      when(col("stockPrice1").isNull, 0).otherwise(col("stockPrice1")).alias("stockPrice1"),
                      col("rn"),
                      col("rn1")
                  )
    val stageDf = joinedDf.groupBy(col("stockId"), col("timeStamp"), col("stockPrice"))
                  .agg(sum(col("stockPrice1")).alias("moving_average_summation"))
                  .withColumn(
                      "moving_average",
                      col("moving_average_summation")/size
                  )
                  
    // joinedDf.orderBy(col("stockId"), col("timeStamp"))show()
    // stageDf.orderBy(col("stockId"), col("timeStamp"))show()
    
    val movingAvg = stageDf
                    .select(
                        "stockId", 
                        "timeStamp", 
                        "stockPrice", 
                        "moving_average"
                    )
                    .orderBy(
                        col("stockId"),
                        col("timeStamp")
                    )
    
    movingAvg.show()
    movingAvg
  }
}

object MovingAverage {
  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSessionBuilder.build
    
    if (args.length != 3) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <outputDir>]")
    }

    // val outputDF = MovingAverage(spark, args(1), Integer.parseInt(args(0))).calculate()
    val outputDF = MovingAverage(spark, "C://Users//kumar.rohit//Documents//ThoughtWorks//SparkBatchWithUnitTest//src//test//resources//input//stock_prices//0.csv", 3).calculate()
    // OutputWriter.write(outputDF, args(2))
    OutputWriter.write(outputDF, args(2))
  }

}

case class MovingAverageWithStockInfo(sparkSession: SparkSession, stockPriceInputDir: String, stockInfoInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
    // val movingAverage = MovingAverage(sparkSession, stockPriceInputDir, size).calculate()
    val movingAverage = MovingAverage(sparkSession, stockPriceInputDir, 3).calculate()
    val stockPriceDf = sparkSession.read.format("csv")
                                   .option("header", "true")
                                   .option("inferSchema", "true")
                                   .option("path", stockInfoInputDir)
                                   .load
    
    val joinedDf = movingAverage.join(
                    stockPriceDf.select(col("stockId").alias("sid"), col("stockName"), col("stockCategory")),
                    col("stockId") === col("sid"),
                    "inner"
                  ).select("stockId", "timeStamp", "stockPrice", "moving_average", "stockName", "stockCategory")
    joinedDf
  }
  
  def calculateForAStock(stockId: String) : DataFrame = {
    val outputDF = MovingAverageWithStockInfo(sparkSession, stockPriceInputDir, stockInfoInputDir, size).calculate
    return outputDF.filter(col("stockId") === stockId)
  }
}

object MovingAverageWithStockInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build

    if (args.length >= 4) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <stockInfoInputDir> <outputDir> [<stockId>]]")
    }

    val movingAverageWithStockInfo = MovingAverageWithStockInfo(
                                      spark, 
                                      "C://Users//kumar.rohit//Documents//ThoughtWorks//SparkBatchWithUnitTest//src//test//resources//input//stock_prices//0.csv", 
                                      "C://Users//kumar.rohit//Documents//ThoughtWorks//SparkBatchWithUnitTest//src//test//resources//input//stock_info//0.csv", 
                                      3
                                    )
    val outputDF = args.lift(4) match {
      case Some(stockId) => movingAverageWithStockInfo.calculateForAStock(stockId)
      case _ => movingAverageWithStockInfo.calculate()
    }
    
    outputDF.show()
    OutputWriter.write(outputDF, args(3))
  }
}