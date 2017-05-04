package cn.edu.ruc.iir.rainbow.eva

import cn.edu.ruc.iir.rainbow.eva.metrics.{Crawler, StageMetrics}
import cn.edu.ruc.iir.rainbow.eva.metrics.{Crawler, StageMetrics}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
/**
 * Created by hank on 2015/2/5.
 */
object SparkEvaluator {

  def execute (appName:String, masterHostName:String, hdfsPath:String, columns:String, orderByColumn:String):StageMetrics =
  {
    val conf = new SparkConf().setAppName(appName)
      .setMaster("spark://" + masterHostName + ":7077")
      .set("spark.executor.memory","20g")
      .set("spark.driver.memory", "4g")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val parquetFile = sqlContext.parquetFile(hdfsPath);
    parquetFile.registerTempTable("parq");
    val res = sqlContext.sql("select " + columns + " from parq order by " + orderByColumn + " limit 10");
    res.count();
    val metricses = Crawler.getInstance().getAllStageMetricses(masterHostName, 4040).asScala;
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
