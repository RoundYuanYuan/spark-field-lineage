# spark 字段血缘 （spark field lineage）

## 参考代码
https://github.com/RHobart/spark-lineage-parent

https://github.com/AbsaOSS/spline-spark-agent
## getting start 
com.roundyuan.sparkagent.TestHive
## Build project
mvn install

## use spark-sql-cli
1. 先打包 mvn install
2. 将jar包放到 spark的jar下
3. 在spark-default.conf 配置
   spark.sql.execution.arrow.enabled  true
   spark.sql.queryExecutionListeners com.roundyuan.sparkagent.FlQueryExecutionListener
   
4. 执行sql会打印出 字段的依赖关系

### 下版本更新内容
1. 将字段血缘推送到 kafka和日志打印出来

## issue 规范
版本：spark2.4.7
问题：with 语法不支持
日志详情：
