/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.producer.service.repo


import java.util.UUID.randomUUID
import java.{lang => jl}

import com.arangodb.ArangoDatabaseAsync
import org.apache.commons.lang3.StringUtils.wrap
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.spline.common.json.SimpleJsonSerDe
import za.co.absa.spline.common.logging.Logging
import za.co.absa.spline.persistence.model._
import za.co.absa.spline.persistence.tx.{InsertQuery, TxBuilder}
import za.co.absa.spline.persistence.{ArangoImplicits, Persister, model => dbModel}
import za.co.absa.spline.producer.model._
import za.co.absa.spline.producer.service.repo.ExecutionProducerRepositoryImpl._
import za.co.absa.spline.producer.{model => apiModel}

import scala.compat.java8.FutureConverters._
import scala.compat.java8.StreamConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

@Repository
class ExecutionProducerRepositoryImpl @Autowired()(db: ArangoDatabaseAsync) extends ExecutionProducerRepository
  with Logging {

  import ArangoImplicits._

  import scala.concurrent.ExecutionContext.Implicits._

  override def insertExecutionPlan(executionPlan: apiModel.ExecutionPlan)(implicit ec: ExecutionContext): Future[Unit] = Persister.execute({
    val eventuallyExists = db.queryOne[Boolean](
      s"""
         |FOR ex IN ${NodeDef.ExecutionPlan.name}
         |    FILTER ex._key == @key
         |    COLLECT WITH COUNT INTO cnt
         |    RETURN TO_BOOL(cnt)
         |    """.stripMargin,
      Map("key" -> executionPlan.id))

    val referencedDSURIs = {
      val readSources = executionPlan.operations.reads.flatMap(_.inputSources).toSet
      val writeSource = executionPlan.operations.write.outputSource
      readSources + writeSource
    }

    val eventualPersistedDSes = db.queryAs[DataSource](
      s"""
         |FOR ds IN ${NodeDef.DataSource.name}
         |    FILTER ds.uri IN [${referencedDSURIs.map(wrap(_, '"')).mkString(", ")}]
         |    RETURN ds
         |    """.stripMargin
    ).map(_.streamRemaining.toScala.map(ds => ds.uri -> ds._key).toMap)

    for {
      persistedDSes: Map[String, String] <- eventualPersistedDSes
      alreadyExists: Boolean <- eventuallyExists
      _ <-
        if (alreadyExists) Future.successful(Unit)
        else createInsertTransaction(executionPlan, referencedDSURIs, persistedDSes).execute(db).map(_ => true)
    } yield Unit
  })

  override def insertExecutionEvents(events: Array[ExecutionEvent])(implicit ec: ExecutionContext): Future[Unit] = Persister.execute({
    val allReferencesConsistentFuture = db.queryOne[Boolean](
      """
        |LET cnt = FIRST(
        |    FOR ep IN executionPlan
        |        FILTER ep._key IN @keys
        |        COLLECT WITH COUNT INTO cnt
        |        RETURN cnt
        |    )
        |RETURN cnt == LENGTH(@keys)
        |""".stripMargin,
      Map("keys" -> events.map(_.planId))
    )

    val progressNodes = events.map(e => Progress(
      e.timestamp,
      e.error,
      e.extra,
      createEventKey(e)))

    val progressEdges = progressNodes
      .zip(events)
      .map({ case (p, e) => EdgeDef.ProgressOf.edge(p._key, e.planId) })

    val tx = new TxBuilder()
      .addQuery(InsertQuery(NodeDef.Progress, progressNodes: _*).copy(ignoreExisting = true))
      .addQuery(InsertQuery(EdgeDef.ProgressOf, progressEdges: _*).copy(ignoreExisting = true))
      .buildTx

    for {
      refConsistent <- allReferencesConsistentFuture
      if refConsistent
      res <- tx.execute(db)
    } yield res
  })

  private def createInsertTransaction(
    executionPlan: apiModel.ExecutionPlan,
    referencedDSURIs: Set[String],
    persistedDSes: Map[String, String]
  ) = {
    val transientDSes: Map[String, String] = (referencedDSURIs -- persistedDSes.keys).map(_ -> randomUUID.toString).toMap
    val referencedDSes = transientDSes ++ persistedDSes
    new TxBuilder()
      .addQuery(InsertQuery(NodeDef.Operation, createOperations(executionPlan): _*))
      .addQuery(InsertQuery(EdgeDef.Follows, createFollows(executionPlan): _*))
      .addQuery(InsertQuery(NodeDef.DataSource, createDataSources(transientDSes): _*))
      .addQuery(InsertQuery(EdgeDef.WritesTo, createWriteTo(executionPlan, referencedDSes)))
      .addQuery(InsertQuery(EdgeDef.ReadsFrom, createReadsFrom(executionPlan, referencedDSes): _*))
      .addQuery(InsertQuery(EdgeDef.Executes, createExecutes(executionPlan)))
      .addQuery(InsertQuery(NodeDef.ExecutionPlan, createExecution(executionPlan)))
      .addQuery(InsertQuery(EdgeDef.Depends, createExecutionDepends(executionPlan, referencedDSes): _*))
      .addQuery(InsertQuery(EdgeDef.Affects, createExecutionAffects(executionPlan, referencedDSes)))
      .buildTx
  }

  override def isDatabaseOk: Future[Boolean] = {
    try {
      val anySplineCollectionName = NodeDef.ExecutionPlan.name
      val futureIsDbOk = db.collection(anySplineCollectionName).exists.toScala.mapTo[Boolean]
      futureIsDbOk.onSuccess {
        case false =>
          log.error(s"Collection '$anySplineCollectionName' does not exist. Spline database is not initialized properly!")
      }
      futureIsDbOk.recover { case _ => false }
    } catch {
      case NonFatal(_) => Future.successful(false)
    }
  }
}

object ExecutionProducerRepositoryImpl {

  import SimpleJsonSerDe._

  private[repo] def createEventKey(e: ExecutionEvent) =
    s"${e.planId}:${jl.Long.toString(e.timestamp, 36)}"

  private def createExecutes(executionPlan: apiModel.ExecutionPlan) = EdgeDef.Executes.edge(
    executionPlan.id,
    s"${executionPlan.id}:${executionPlan.operations.write.id}")

  private def createExecution(executionPlan: apiModel.ExecutionPlan): dbModel.ExecutionPlan =
    dbModel.ExecutionPlan(
      systemInfo = executionPlan.systemInfo.toJsonAs[Map[String, Any]],
      agentInfo = executionPlan.agentInfo.map(_.toJsonAs[Map[String, Any]]).orNull,
      extra = executionPlan.extraInfo,
      _key = executionPlan.id.toString)

  private def createReadsFrom(plan: apiModel.ExecutionPlan, dsUriToKey: String => String): Seq[Edge] = for {
    ro <- plan.operations.reads
    ds <- ro.inputSources
  } yield EdgeDef.ReadsFrom.edge(
    s"${plan.id}:${ro.id}",
    dsUriToKey(ds))

  private def createWriteTo(executionPlan: apiModel.ExecutionPlan, dsUriToKey: String => String) = EdgeDef.WritesTo.edge(
    s"${executionPlan.id}:${executionPlan.operations.write.id}",
    dsUriToKey(executionPlan.operations.write.outputSource))

  private def createExecutionDepends(plan: apiModel.ExecutionPlan, dsUriToKey: String => String): Seq[Edge] = for {
    ro <- plan.operations.reads
    ds <- ro.inputSources
  } yield EdgeDef.Depends.edge(
    plan.id,
    dsUriToKey(ds))

  private def createExecutionAffects(executionPlan: apiModel.ExecutionPlan, dsUriToKey: String => String) = EdgeDef.Affects.edge(
    executionPlan.id,
    dsUriToKey(executionPlan.operations.write.outputSource))

  private def createDataSources(dsUriToKey: Map[String, String]): Seq[DataSource] = dsUriToKey
    .map({ case (uri, key) => DataSource(uri, key) })
    .toVector

  private def createOperations(executionPlan: apiModel.ExecutionPlan): Seq[dbModel.Operation] = {
    val allOperations = executionPlan.operations.all
    val schemaFinder = new RecursiveSchemaFinder(allOperations)
    allOperations.map {
      case r: ReadOperation =>
        dbModel.Read(
          inputSources = r.inputSources,
          properties = r.params,
          outputSchema = r.schema,
          _key = s"${executionPlan.id}:${r.id.toString}"
        )
      case w: WriteOperation =>
        dbModel.Write(
          outputSource = w.outputSource,
          append = w.append,
          properties = w.params,
          outputSchema = schemaFinder.findSchemaOf(w),
          _key = s"${executionPlan.id}:${w.id.toString}"
        )
      case t: DataOperation =>
        dbModel.Transformation(
          properties = t.params,
          outputSchema = schemaFinder.findSchemaOf(t),
          _key = s"${executionPlan.id}:${t.id.toString}"
        )
    }
  }

  private def createFollows(executionPlan: apiModel.ExecutionPlan): Seq[Edge] =
    for {
      operation <- executionPlan.operations.all
      childId <- operation.childIds
    } yield EdgeDef.Follows.edge(
      s"${executionPlan.id}:${operation.id}",
      s"${executionPlan.id}:$childId")
}
