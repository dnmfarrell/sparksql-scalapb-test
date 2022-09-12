package myexample

import com.example.protos.demo._

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import scalapb.spark.Implicits._
import scalapb.spark.ProtoSQL

object RunDemo {

  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()
    val xs: Seq[Large] = Seq[Large](Large(Some(1L)))
    val df: DataFrame = ProtoSQL.createDataFrame(spark, xs)
    df.show
    val ds: Dataset[Small] = df.as[Small]
    ds.head
  }
}

