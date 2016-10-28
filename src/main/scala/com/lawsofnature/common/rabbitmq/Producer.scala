package com.lawsofnature.common.rabbitmq

import java.util.concurrent.Executors
import javax.inject.Inject

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, MessageProperties}

/**
  * Created by fangzhongwei on 2016/10/22.
  */
class RabbitmqProducerTemplate @Inject()(host: String) {
  var conn: Connection = _

  def connect(host: String, port: Int, username: String, password: String, virtualHost: String, threadPollSize: Int): Unit = {
    val factory: ConnectionFactory = new ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    factory.setUsername(username)
    factory.setPassword(password)
    factory.setVirtualHost(virtualHost)
    factory.setSharedExecutor(Executors.newFixedThreadPool(threadPollSize))
    conn = factory.newConnection()
  }

  def send(exchange: String, exchangeType: String, queue: String, routingKey: String, dataByte: Array[Byte]): Unit = {
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

  def send2Multiple(exchange: String, exchangeType: String, queues: Array[String], routingKeys: Array[String], dataByte: Array[Byte]): Unit = {
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

  def close: Unit = {
    conn.close()
  }
}