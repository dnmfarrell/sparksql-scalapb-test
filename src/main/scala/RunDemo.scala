package myexample

import com.example.protos.demo._
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object RunDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()
    args.size match {
      case 1 if args(0) == "--native" => tryNativeEncoder(spark)
      case _ => tryScalapbEncoder(spark)
    }
  }

  def tryScalapbEncoder(spark: SparkSession) {
    println("Trying scalapb-sparksql encoder")
    import scalapb.spark.Implicits._
    val xs: Seq[Large] = Seq[Large](Large(Some(1L)))
    val ys: Dataset[Large] = spark.createDataset[Large](xs)
    ys.show

    // This should fail - (Long) -> (Int)
    val zs: Dataset[Small] = ys.as[Small]
    zs.show

    // This should fail - (Int) -> (Int, Int)
    val as: Dataset[Point] = ys.as[Point]
    as.show

    // This will fail - query execution
    as.head

  }

  def tryNativeEncoder(spark: SparkSession) {
    println("Trying native encoder")
    import spark.implicits._
    val xs: Seq[LargeNative] = Seq[LargeNative](LargeNative(Some(1L)))
    val ys: Dataset[LargeNative] = spark.createDataset[LargeNative](xs)
    ys.show

    // This fails - (Long) -> (Int) "cannot up cast x from bigint to int"
    val zs: Dataset[SmallNative] = ys.as[SmallNative]
    zs.show

    // This should fail - (Long) -> (Int, Int)
    val as: Dataset[PointNative] = ys.as[PointNative]
    as.show

    // This will fail - query execution
    as.head

  }
}

final case class LargeNative (x: Option[Long])
final case class SmallNative (x: Option[Int])
final case class PointNative (x: Option[Int], y: Option[Int])
