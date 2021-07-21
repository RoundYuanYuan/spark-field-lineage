package com.roundyuan.sparkagent

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, DeserializeToObject, Distinct, Filter, Generate, GlobalLimit, InsertIntoTable, Join, LocalLimit, LocalRelation, LogicalPlan, MapElements, MapPartitions, Project, Repartition, RepartitionByExpression, SerializeFromObject, SubqueryAlias, TypedFilter, Union, Window}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.control.NonFatal

/**
 * auth:zhangmengyuan
 * 手动获取解析计划为 explain extended
 * 该方法为通过解析逻辑执行计划获取字段间的血缘关系
 *
 */

class FlQueryExecutionListener extends QueryExecutionListener with Logging {
  // 目标表应该只有一张
  private val targetTable: Map[Long, String] = Map()
  // source表 可能有多个
  private val sourceTables: Map[Long, String] = Map()
  // 字段执行过程的关系
  private val fieldProcess: Map[Long, mutable.Set[Long]] = Map()
  // 压缩后的血缘关系 只记录source表到 target表
  private val fieldLineage: Map[String, mutable.Set[String]] = mutable.Map();
  // SQL类型 考虑、insert select、create as
  private var processType: String = ""

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = withErrorHandling(qe) {
    // scuess exec logic plan exec
    lineageParser(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = withErrorHandling(qe) {

  }

  private def withErrorHandling(qe: QueryExecution)(body: => Unit): Unit = {
    try
      body
    catch {
      case NonFatal(e) =>
        val ctx = qe.sparkSession.sparkContext
        logError(s"Unexpected error occurred during lineage processing for application: ${ctx.appName} #${ctx.applicationId}", e)
    }
  }

  def lineageParser(qe: QueryExecution): Unit = {
    logInfo("----------- field lineage parse start --------")
    // 针对做解析且将 source表及结果表记录
    val analyzedLogicPlan = qe.analyzed
    resolveLogicV2(analyzedLogicPlan)
    // 关系连接
    connectSourceFieldAndTargetField()
    println(fieldLineage)
  }

  /**
   *
   * @param plan
   */
  def resolveLogicV2(plan: LogicalPlan): Unit = {
    // 获取原始表从 LogicalRelation 或 HiveTableRelation 目标表从 InsertIntoHiveTable 和 CreateHiveTableAsSelectCommand
    // 获取转换过程从Aggregate 和 Project
    plan.collect {
      case plan: LogicalRelation => {
        val calalogTable = plan.catalogTable.get
        val tableName = calalogTable.database + "." + calalogTable.identifier.table
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTables += (columnAttribute.exprId.id -> columnFullName)
        })
      }
      case plan: HiveTableRelation => {
        val tableName = plan.tableMeta.database + "." + plan.tableMeta.identifier.table
        plan.output.foreach(columnAttribute => {
          val columnFullName = tableName + "." + columnAttribute.name
          sourceTables += (columnAttribute.exprId.id -> columnFullName)
        })
      }
      case plan: InsertIntoHiveTable => {
        val tableName = plan.table.database + "." + plan.table.identifier.table
        extTargetTable(tableName, plan.query)
      }
      case plan: InsertIntoHadoopFsRelationCommand=>{
        val catalogTable: CatalogTable = plan.catalogTable.get
        val tableName=catalogTable.database+"."+catalogTable.identifier.table
        extTargetTable(tableName, plan.query)
      }
      case plan: CreateHiveTableAsSelectCommand => {
        val tableName = plan.tableDesc.database + "." + plan.tableDesc.identifier.table
        extTargetTable(tableName, plan.query)
      }
      case plan: Aggregate => {
        plan.aggregateExpressions.foreach(aggItem => {
          extFieldProcess(aggItem)
        }
        )
      }
      case plan: Project => {
        plan.projectList.toList.foreach {
          pojoItem => {
            extFieldProcess(pojoItem)
          }
        }
      }
      //      case `plan` => logInfo("******child plan******:\n" + plan)
    }
  }

  def extFieldProcess(namedExpression: NamedExpression): Unit = {
    //alias 存在转换关系 不然就是原本的值
    if ("alias".equals(namedExpression.prettyName)) {
      val sourceFieldId = namedExpression.exprId.id
      val targetFieldIdSet: mutable.Set[Long] = fieldProcess.getOrElse(sourceFieldId, mutable.Set.empty)
      namedExpression.references.foreach(attribute => {
        targetFieldIdSet += attribute.exprId.id
      })
      fieldProcess += (sourceFieldId -> targetFieldIdSet)
    }
  }

  def extTargetTable(tableName: String, plan: LogicalPlan): Unit = {
    logInfo("start ext target table")
    plan.output.foreach(columnAttribute => {
      val columnFullName = tableName + "." + columnAttribute.name
      targetTable += (columnAttribute.exprId.id -> columnFullName)
    })
  }

  /**
   * 从过程中提取血缘：目标表 字段循环是否存在于 source表中，不存在的话从过程中寻找直到遇见目标表
   */
  def connectSourceFieldAndTargetField(): Unit = {
    val fieldIds = targetTable.keySet
    fieldIds.foreach(fieldId => {
      val resTargetFieldName = targetTable(fieldId)
      val resSourceFieldSet: mutable.Set[String] = mutable.Set.empty[String]
      if (sourceTables.contains(fieldId)) {
        val sourceFieldId = sourceTables.getOrElse(fieldId, "")
        resSourceFieldSet += sourceFieldId
      } else {
        val targetIdsTmp = findSourceField(fieldId)
        resSourceFieldSet ++= targetIdsTmp
      }
      fieldLineage += (resTargetFieldName -> resSourceFieldSet)
    })
  }

  def findSourceField(fieldId: Long): mutable.Set[String] = {
    val resSourceFieldSet: mutable.Set[String] = mutable.Set.empty[String]
    if (fieldProcess.contains(fieldId)) {
      val fieldIds: mutable.Set[Long] = fieldProcess.getOrElse(fieldId, mutable.Set.empty)
      fieldIds.foreach(fieldId => {
        if (sourceTables.contains(fieldId)) {
          resSourceFieldSet += sourceTables(fieldId)
        } else {
          val sourceFieldSet = findSourceField(fieldId)
          resSourceFieldSet ++= sourceFieldSet
        }
      })
    }
    resSourceFieldSet
  }
}
