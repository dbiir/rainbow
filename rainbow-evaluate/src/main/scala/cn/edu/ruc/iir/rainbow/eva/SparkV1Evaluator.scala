package cn.edu.ruc.iir.rainbow.eva

import cn.edu.ruc.iir.rainbow.eva.metrics.{Crawler, StageMetrics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

/**
 * Created by hank on 2015/2/5.
 */
object SparkV1Evaluator {

  def execute (appName:String, masterHostName:String, appPort:Int, driverWebappsPort:Int,
               warehouseDir:String, executorCores:Int, executorMemory:String,
               hdfsPath:String, columns:String, orderByColumn:String):
  StageMetrics =
  {
    // this is for Spark 1.2.x and 1.3.x
    val conf = new SparkConf().setAppName(appName)
      .setMaster("spark://" + masterHostName + ":" + appPort)
      .set("spark.executor.memory", executorMemory)
      .set("spark.executor.cores", executorCores+"")
      //.set("spark.driver.maxResultSize", "4g")
      //.set("spark.eventLog.enabled", "true")
      //.set("spark.eventLog.dir", "hdfs://presto00:9000/spark-events");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val parquetFile = sqlContext.parquetFile(hdfsPath);
    parquetFile.registerTempTable("parq");
    val res = sqlContext.sql("select " + columns + " from parq order by " + orderByColumn + " limit 10");
    res.count();
    val metricses = Crawler.Instance().getSparkV1StageMetricses(masterHostName, driverWebappsPort).asScala;
    sc.stop();

    var metrics:StageMetrics = null;

    breakable {
      for (m <- metricses) {
        if (m.getId == 0) {
          metrics = m;
          break;
        }
      }
    }

    return metrics;
  }

}
