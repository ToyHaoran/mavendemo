package com.atguigu.sparktuning.aqe

import com.atguigu.sparktuning.utils.InitUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AqeOptimizingSkewJoin {
  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("AqeOptimizingSkewJoin")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")  //为了演示效果，禁用广播join
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true") // 为了演示效果，关闭自动缩小分区
      .set("spark.sql.adaptive.enabled", "true")  // 打开AQE总开关
      .set("spark.sql.adaptive.skewJoin.enable","true")  // 开启倾斜join检测
      // 判断分区数据量是否数据倾斜：
      // 当任务中最大数据量分区对应的数据量max > 分区数据量中位数 * Factor 并且 max > ThresholdInBytes
      .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2")  // 默认5
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","20mb")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8mb")  // 打散后的推荐分区大小
    val sparkSession: SparkSession = InitUtil.initSparkSession(sparkConf)
    useJoin(sparkSession)
  }

  def useJoin( sparkSession: SparkSession ) = {
    val saleCourse = sparkSession.sql("select *from sparktuning.sale_course")
    val coursePay = sparkSession.sql("select * from sparktuning.course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    val courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    saleCourse.join(courseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(coursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("sparktuning.salecourse_detail_1")
  }


}
