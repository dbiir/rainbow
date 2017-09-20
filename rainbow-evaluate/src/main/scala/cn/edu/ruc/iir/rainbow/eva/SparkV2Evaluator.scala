package cn.edu.ruc.iir.rainbow.eva

import cn.edu.ruc.iir.rainbow.eva.metrics.{Crawler, StageMetrics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
/**
 * Created by hank on 2017/9/20.
 */
object SparkV2Evaluator {

  def execute (appName:String, masterHostName:String, appPort:Int, driverWebappsPort:Int, warehouseDir:String, executorCores:Int, executorMemory:String, hdfsPath:String, columns:String, orderByColumn:String):StageMetrics =
  {
    // this is for Spark 2.1
    val spark = SparkSession
      .builder()
      .master("spark://" + masterHostName + ":" + appPort)
      .appName(appName)
      .config("spark.executor.memory", executorMemory)
      .config("spark.executor.cores", executorCores)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .getOrCreate();
    val df = spark.read.parquet(hdfsPath);
    df.createOrReplaceTempView("parq");
    val res = spark.sql("select " + columns + " from parq order by " + orderByColumn + " limit 10");
    res.count();
    val metricses = Crawler.Instance().getSparkV2StageMetricses("localhost", driverWebappsPort).asScala;
    spark.stop();

    var metrics:StageMetrics = null;

    breakable {
      for (m <- metricses) {
        if (m.getId == 1) {
          metrics = m;
          break;
        }
      }
    }

    return metrics;
  }

}
