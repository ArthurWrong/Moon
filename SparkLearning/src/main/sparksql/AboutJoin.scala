package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author tao_chen
 *         date: 2020/8/28
 *         time: 10:38
 *         description:
 */
object AboutJoin {
  def main(args: Array[String]): Unit = {
    //设置本机Spark配置
    val conf = new SparkConf()
      .setAppName(AboutJoin.getClass.getSimpleName)
      .setMaster("local")
    //创建Spark上下
    val ss = SparkSession
      .builder
      .master("local")
      .config(conf)
      .getOrCreate()

    val students = ss
      .read
      .option("header", "true")
      .csv("SparkLearning/src/main/resources/classes.csv")
    val classes = ss
      .read
      .option("header", "true")
      .csv("SparkLearning/src/main/resources/students.csv")
    val res = students.join(
      classes, Seq("class"), "full"
    )
    res.show()

  }
}
