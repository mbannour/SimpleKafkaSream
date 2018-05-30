package com.dali.akkastreams

import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore

import scala.collection.JavaConverters.asJavaIterableConverter


object StreamsKafkaStart extends App {

  val config = new Properties()
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-starter-project")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass())
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass())

  val stringSerde = Serdes.String()
  val longSerde = Serdes.Long()

  val builder = new StreamsBuilder()

  val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val textLines: KStream[String, String] = builder.stream("streams-plaintext-input", Consumed.`with`(stringSerde, stringSerde))

  val worldCounts: KStream[String, Long] = textLines
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+").toIterable.asJava)
    .groupBy((_, word) => word)
    .count(Materialized.as("counts-store").asInstanceOf[Materialized[String, Long, KeyValueStore[Bytes, Array[Byte]]]])
    .toStream()

    worldCounts.to("streams-wordcount-output", Produced.`with`(Serdes.String(), Serdes.Long()))

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(10, TimeUnit.SECONDS)
  }))


}
