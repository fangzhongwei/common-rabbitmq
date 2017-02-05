package com.jxjxgo.common.rabbitmq

import java.util.concurrent.Executors
import javax.inject.{Inject, Named}

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, MessageProperties}
import org.slf4j.LoggerFactory

/**
  * Created by fangzhongwei on 2016/10/22.
  */

trait RabbitmqProducerTemplate {
  def connect(): Unit

  def send(exchange: String, exchangeType: String, queue: String, routingKey: String, dataByte: Array[Byte]): Unit

  def send2Multiple(exchange: String, exchangeType: String, queues: Array[String], routingKeys: Array[String], dataByte: Array[Byte]): Unit

  def close: Unit
}

class RabbitmqProducerTemplateImpl @Inject()(@Named("rabbitmq.host") host: String,
                                             @Named("rabbitmq.port") port: Int,
                                             @Named("rabbitmq.username") username: String,
                                             @Named("rabbitmq.password") password: String,
                                             @Named("rabbitmq.virtualHost") virtualHost: String,
                                             @Named("rabbitmq.threadPollSize") threadPollSize: Int) extends RabbitmqProducerTemplate {
  val logger = LoggerFactory.getLogger(this.getClass)

  var conn: Connection = _

  def apply(host: String, port: Int, username: String, password: String, virtualHost: String, threadPollSize: Int): RabbitmqProducerTemplateImpl = new RabbitmqProducerTemplateImpl(host, port, username, password, virtualHost, threadPollSize)

  override def connect(): Unit = {
    val factory: ConnectionFactory = new ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    factory.setUsername(username)
    factory.setPassword(password)
    factory.setVirtualHost(virtualHost)
    factory.setSharedExecutor(Executors.newFixedThreadPool(threadPollSize))
    conn = factory.newConnection()
  }

  override def send(exchange: String, exchangeType: String, queue: String, routingKey: String, dataByte: Array[Byte]): Unit = {
    var channel: Channel = null
    try {
      channel = conn.createChannel()
      channel.exchangeDeclare(exchange, exchangeType) //direct fanout topic
      channel.queueDeclare(queue, false, false, false, null)
      channel.queueBind(queue, exchange, routingKey)
      channel.basicPublish(exchange, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, dataByte)
    } finally {
      if (channel != null) channel.close()
    }
  }

  override def send2Multiple(exchange: String, exchangeType: String, queues: Array[String], routingKeys: Array[String], dataByte: Array[Byte]): Unit = {
    var channel: Channel = null
    try {
      channel = conn.createChannel()
      channel.exchangeDeclare(exchange, exchangeType) //direct fanout topic
      for (i <- 0 to (queues.length - 1)) {
        channel.queueDeclare(queues(i), false, false, false, null)
        channel.queueBind(queues(i), exchange, routingKeys(i))
        channel.basicPublish(exchange, routingKeys(i), MessageProperties.PERSISTENT_TEXT_PLAIN, dataByte)
      }
    } finally {
      if (channel != null) channel.close()
    }
  }

  override def close: Unit = {
    conn.close()
  }
}