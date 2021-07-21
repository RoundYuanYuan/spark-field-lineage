package com.roundyuan.sparkagent

import org.apache.spark.sql.SparkSession

object TestHive {
  def main(args: Array[String]): Unit = {
    // 连接hive数据仓库
    val sparkSession = SparkSession.builder()
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .appName("HiveCaseJob")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val listenV3 = new FlQueryExecutionListener()
    sparkSession.listenerManager.register(listenV3)
    //sparkSession.sql("show databases").show()
    sparkSession.sql("insert into test.test_orc select id,count(name) as num from test.test02 group by id").show()
    //val user_log = sparkSession.sql("select * from dbtaobao.user_log").collect()
    //val test = user_log.map(row => "user_id"+row(0))
    //test.map(row => println(row))

  }
}
