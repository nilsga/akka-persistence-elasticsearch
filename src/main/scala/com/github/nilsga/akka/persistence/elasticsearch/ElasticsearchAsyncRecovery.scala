package com.github.nilsga.akka.persistence.elasticsearch

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import akka.util.Timeout
import com.github.nilsga.akka.persistence.elasticsearch.ScrollActor.Execute
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.search.aggregations.metrics.max.Max

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait ElasticsearchAsyncRecovery extends AsyncRecovery with ActorLogging {
  this : ElasticsearchAsyncWriteJournal =>

  import context._
  implicit def scrollTimeout = Timeout(60 seconds)

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    esClient.execute(refresh index journalIndex).flatMap(_ => {
      val maxSearch = esClient.execute(search in journalIndex / journalType query {
        filteredQuery filter {
          termFilter("persistenceId", persistenceId)
        }
      } aggregations {
        aggregation max "maxSeqNr" field "sequenceNumber"
      })

      maxSearch.map(response => {
        val max = response.aggregations.asMap().get("maxSeqNr").asInstanceOf[Max].getValue
        max.isInfinite || max.isNaN match {
          case true => 0
          case false => max.toLong
        }
      })
    })
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    val end = toSequenceNr - fromSequenceNr match {
      case num if num < max =>
        toSequenceNr
      case _ =>
        fromSequenceNr + max - 1
    }
    esClient.execute(refresh index journalIndex).flatMap(_ => {
      val query = search in journalIndex / journalType query {
        filteredQuery filter {
          and(
            termFilter("persistenceId", persistenceId),
            termFilter("deleted", false),
            rangeFilter("sequenceNumber") gte fromSequenceNr.toString lte end.toString
          )
        }
      } sourceInclude "message" scroll "1m"

      val scroll = system.actorOf(ScrollActor.mkProps(esClient))
      val promise = Promise[Unit]
      (scroll ? Execute(query)).mapTo[List[PersistentRepr]].onComplete {
        case Success(result) =>
          result.sortWith((r1, r2) => r1.sequenceNr < r2.sequenceNr).foreach(replayCallback)
          promise.success()
        case Failure(ex) =>
          promise.failure(ex)
      }
      promise.future
    })
  }
}
