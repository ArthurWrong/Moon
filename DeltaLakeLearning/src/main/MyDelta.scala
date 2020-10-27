import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 *@author tao_chen
 *
 *@date 2020/10/20
 *
 * */
object MyDelta {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(MyDelta.getClass.getSimpleName)
      .setMaster("local")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    import spark._
    import sqlContext.implicits._

    val data = spark.range(0, 5)
    data.write.format("delta").save("D:\\data")
  }
}
