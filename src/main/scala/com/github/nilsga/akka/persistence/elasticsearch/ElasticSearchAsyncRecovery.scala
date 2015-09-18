package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.Base64
import org.elasticsearch.search.aggregations.metrics.max.Max

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait ElasticSearchAsyncRecovery extends AsyncRecovery {
  this : ElasticSearchAsyncWriteJournal =>

  import context._

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
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
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    val normalizedEnd = toSequenceNr - fromSequenceNr >= max match {
      case true => fromSequenceNr + max
      case false => toSequenceNr + 1
    }
    val ids = (fromSequenceNr until normalizedEnd).map(seqNr => s"$persistenceId-$seqNr")
    if(ids.isEmpty) Future.successful()
    else {
      val replays = esClient execute multiget(ids.map(get id _ from journalIndex / journalType))
      val promise = Promise[Unit]

      replays.onComplete({
        case Failure(ex) => promise.failure(ex)
        case Success(multiGetResponse) =>
          multiGetResponse.getResponses.filter(_.getResponse.isExists).foreach(hit => {
            val source = hit.getResponse.getSourceAsMap
            val messageBase64 = source.get("message").asInstanceOf[String]
            val msg = serializer.deserialize[PersistentRepr](Base64.decode(messageBase64), classOf[PersistentRepr]).get
            replayCallback(msg)
          })
          promise.success()
      })

      promise.future
    }
  }
}
