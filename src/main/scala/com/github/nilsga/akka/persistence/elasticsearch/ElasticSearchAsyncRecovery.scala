package com.github.nilsga.akka.persistence.elasticsearch

import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.Base64
import org.elasticsearch.search.aggregations.metrics.max.Max

import scala.concurrent.{Promise, Future}
import scala.util.{Success, Failure}

trait ElasticSearchAsyncRecovery extends AsyncRecovery {
  this : ElasticSearchAsyncWriteJournal =>

  import context._

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("Reading highest seqnr for {} from {}", persistenceId, fromSequenceNr)
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
    log.debug("Replaying messages for {} from {} to {} count {}", persistenceId, fromSequenceNr, toSequenceNr, max)
    val normalizedEnd = toSequenceNr - fromSequenceNr >= max match {
      case true => fromSequenceNr + max
      case false => toSequenceNr + 1
    }
    log.debug("Params {} {} {} {}", fromSequenceNr, toSequenceNr, max, normalizedEnd)
    val ids = (fromSequenceNr until normalizedEnd).map(seqNr => s"$persistenceId-$seqNr")
    if(ids.isEmpty) Future.successful()
    else {
      log.debug("Multigetting {}", ids)

      val replays = esClient execute multiget(ids.map(get id _ from journalIndex / journalType))
      val promise = Promise[Unit]

      replays.onComplete({
        case Failure(ex) => promise.failure(ex)
        case Success(multiGetResponse) =>
          log.debug("Starting replay of {} messages...", multiGetResponse.getResponses.length)
          multiGetResponse.getResponses.filter(_.getResponse.isExists).foreach(hit => {
            val source = hit.getResponse.getSourceAsMap
            val messageBase64 = source.get("message").asInstanceOf[String]
            val msg = serializer.deserialize[PersistentRepr](Base64.decode(messageBase64), classOf[PersistentRepr]).get
            log.debug("Replaying {}", msg)
            replayCallback(msg)
          })
          log.debug("Completing future")
          promise.success()
      })

      promise.future
    }
  }
}
