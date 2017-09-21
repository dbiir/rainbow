package cn.edu.ruc.iir.rainbow.eva

import cn.edu.ruc.iir.rainbow.eva.metrics.{Crawler, StageMetrics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
/**
 * Created by hank on 2017/9/20.
 */
object SparkV2Evaluator {

  def execute (appName:String, masterHostName:String, appPort:Int, driverWebappsPort:Int,
               warehouseDir:String, executorCores:Int, executorMemory:String, format:String,
               hdfsPath:String, columns:String, orderByColumn:String):
  StageMetrics =
  {
    // this is for Spark 2.1
    var spark:SparkSession = null;
    var df:DataFrame = null;
    if (format.equalsIgnoreCase("PARQUET")) {
      spark = SparkSession
        .builder()
        .master("spark://" + masterHostName + ":" + appPort)
        .appName(appName)
        .config("spark.executor.memory", executorMemory)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", executorCores)
        .config("spark.sql.warehouse.dir", warehouseDir)
        .config("spark.storage.memoryFraction", "0")
        //.config("spark.driver.maxResultSize", "1g")
        .getOrCreate();
      df = spark.read.parquet(hdfsPath);
    } else if (format.equalsIgnoreCase("ORC")) {
      spark = SparkSession
        .builder()
        .master("spark://" + masterHostName + ":" + appPort)
        .appName(appName)
        .config("spark.executor.memory", executorMemory)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.cores", executorCores)
        .config("spark.sql.warehouse.dir", warehouseDir)
        .config("spark.storage.memoryFraction", "0")
        //.config("spark.driver.maxResultSize", "1g")
        .enableHiveSupport()
        .getOrCreate();
      df = spark.read.orc(hdfsPath);
    }

    df.createOrReplaceTempView("t1");
    val res = spark.sql("select " + columns + " from t1 order by " + orderByColumn + " limit 10");
    res.count();
    val metricses = Crawler.Instance().getSparkV2StageMetricses("localhost", driverWebappsPort).asScala;
    spark.stop();
    spark.close();

    var metrics:StageMetrics = null;

    breakable {
      for (m <- metricses) {
        if (format.equalsIgnoreCase("PARQUET")) {
          if (m.getId == 1) {
            metrics = m;
            break;
          }
        } else if (format.equalsIgnoreCase("ORC")) {
          if (m.getId == 0) {
            metrics = m;
            break;
          }
        }
      }
    }

    metricses.clear();

    return metrics;
  }

}
