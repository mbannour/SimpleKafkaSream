package com.dali.akkastreams

import java.util.Properties
import java.util.regex.Pattern
import java.{lang, util}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Produced}

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

  val worldCounts: KStream[String, lang.Long] = textLines.flatMapValues(value => util.Arrays.asList(pattern.split(value.toLowerCase())))
     .groupByKey()
     .count()
     .toStream()


  worldCounts.to("streams-wordcount-output", Produced.`with`(stringSerde, longSerde))


  val streams =  new KafkaStreams(builder.build(), config);
  streams.start()

}
