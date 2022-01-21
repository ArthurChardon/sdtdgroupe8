/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import org.apache.spark.sql.SparkSession

import com.datastax.spark.connector._

/**
 * Consumes messages from one or more topics in Kafka.
 * Usage: SparkTwitter <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.SparkTwitter broker1-host:port,broker2-host:port \
 *    consumer-group topic1,topic2
 */
object SparkTwitter {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: SparkTwitter <brokers> <groupId> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <groupId> is a consumer group name to consume from topics
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val cassandraHost = "127.0.0.1"
    val keyspace = "tweet"
    val table = "hashtag"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)

    val Array(brokers, groupId, topics) = args
    
    // Create context with 1 second batch interval
    val sparkConf = new SparkConf().setAppName("Spark-Twitter")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    // val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    
    // format des values : {"ID": "1481260459391320065", "Date": "2022-01-12T13:43:08.000Z", "Hashtags": ["#CoronavirusUpdates", "#NaToCorona"]}
    val DStweets = stream.map(_.value)

    // On garde que les hashtags 
    val DStweetwords = DStweets.flatMap(tweetText => tweetText.split("\""))

    // on garde que les hashtags
    val DShashtags = DStweetwords.filter(word => word.startsWith("#"))

    // On Map chaque hashtag en une paire de key/value (hashtag, 1) pour faire la somme de values
    val DShashtagKeyValues = DShashtags.map(hashtag => (hashtag, 1))

    val DShashtagCount = DShashtagKeyValues.reduceByKeyAndWindow((x:Int, y:Int) => x + y, Seconds(60), Seconds(1))

    // Trier les hashtags par nombre
    val DShashtagsSorted = DShashtagCount.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    // print le top 10 en attendant de faire qqc avec cassandra 
    DShashtagsSorted.print()

    // save to cassandra
    DShashtagsSorted.foreachRDD{tweetRDD =>
      tweetRDD.saveToCassandra(keyspace,table)
    }

    ssc.checkpoint("./checkpoint/")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println

