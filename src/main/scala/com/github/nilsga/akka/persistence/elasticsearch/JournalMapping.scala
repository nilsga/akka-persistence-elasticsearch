package com.github.nilsga.akka.persistence.elasticsearch

import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

object JournalMapping {

  val mapping = XContentFactory.jsonBuilder()
    .startObject()
      .startObject("representation")
        .field("dynamic", "strict")
        .startObject("properties")
          .startObject("persistenceId")
            .field("type", "string")
            .field("index", "not_analyzed")
          .endObject()
          .startObject("sequenceNumber")
            .field("type", "long")
          .endObject()
          .startObject("message")
            .field("type", "string")
            .field("index", "not_analyzed")
          .endObject()
        .endObject()
      .endObject()
    .endObject()

  def apply() = new JournalMapping(mapping)
}

class JournalMapping(val mapping: XContentBuilder)

