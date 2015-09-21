package com.github.nilsga.akka.persistence.elasticsearch

import java.util.concurrent.TimeoutException

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.serialization.SerializationExtension
import com.github.nilsga.akka.persistence.elasticsearch.ScrollActor.{Execute, Finished, Scroll, Timeout}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, SearchDefinition}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.Base64

import scala.concurrent.duration._


object ScrollActor {
  def mkProps(esClient: ElasticClient) = Props(new ScrollActor(esClient))

  case class Execute(query: SearchDefinition)
  case class Finished(originalSender: ActorRef, result: List[PersistentRepr])
  case class Scroll(search: SearchResponse)
  case class Timeout()
}

class ScrollActor(esClient: ElasticClient) extends Actor {

  import context._

  val serializer = SerializationExtension(context.system)
  val timeout = context.system.scheduler.scheduleOnce(10 seconds, self, Timeout())


  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = timeout.cancel()

  override def receive: Actor.Receive = {
    case Execute(query) =>
      esClient.execute(query).map(Scroll) pipeTo self
      context.become(scroll(sender(), List()))
    case Finished(originalSender, result) =>
      originalSender ! result
      context.stop(self)
  }

  def scroll(originalSender: ActorRef, result: List[PersistentRepr]) : Receive = {
    case Scroll(searchResponse) =>
      searchResponse.isEmpty match {
        case false =>
          val representations = searchResponse.hits.map(hit => {
            val source = hit.sourceAsMap
            val messageBase64 = source("message").asInstanceOf[String]
            serializer.deserialize[PersistentRepr](Base64.decode(messageBase64), classOf[PersistentRepr]).get
          })
          esClient.execute(search scroll searchResponse.getScrollId keepAlive "1m").map(Scroll) pipeTo self
          context.become(scroll(originalSender, result ++ representations))
        case true =>
          self ! Finished(originalSender, result)
          context.become(receive)
      }
    case msg @ Failure => {
      originalSender ! msg
      context.stop(self)
    }
    case Timeout() =>
      originalSender ! Failure(new TimeoutException("Scroll operation timed out"))
      context.stop(self)
  }
}
