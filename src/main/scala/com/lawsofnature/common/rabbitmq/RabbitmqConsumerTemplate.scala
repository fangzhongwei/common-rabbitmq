package com.lawsofnature.common.rabbitmq

import java.util.concurrent.Executors
import javax.inject.{Inject, Named}

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, QueueingConsumer}
import org.slf4j.LoggerFactory

/**
  * Created by fangzhongwei on 2016/10/22.
  */

trait RabbitmqConsumerTemplate {
  def connect(): Unit

  def startConsume(queue: String, handle: String => Boolean): Unit

  def close: Unit
}

class RabbitmqConsumerTemplateImpl @Inject()(@Named("rabbitmq.host") host: String,
                                             @Named("rabbitmq.port") port: Int,
                                             @Named("rabbitmq.username") username: String,
                                             @Named("rabbitmq.password") password: String,
                                             @Named("rabbitmq.virtualHost") virtualHost: String,
                                             @Named("rabbitmq.threadPollSize") threadPollSize: Int) extends RabbitmqConsumerTemplate {
  val logger = LoggerFactory.getLogger(this.getClass)

  var conn: Connection = _

  def apply(host: String, port: Int, username: String, password: String, virtualHost: String, threadPollSize: Int): RabbitmqConsumerTemplateImpl = new RabbitmqConsumerTemplateImpl(host, port, username, password, virtualHost, threadPollSize)

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

  override def startConsume(queue: String, handle: String => Boolean): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        val channel: Channel = conn.createChannel()
        channel.queueDeclare(queue, false, false, false, null)
        val consumer: QueueingConsumer = new QueueingConsumer(channel)
        channel.basicConsume(queue, true, consumer)
        logger.info(" [*] Waiting for messages.")
        while (true) {
          val delivery: QueueingConsumer.Delivery = consumer.nextDelivery()
          val message: String = new String(delivery.getBody())
          logger.info(new StringBuilder("receive message:").append(message).toString())
          handle(message)
        }
      }
    }).start()
  }

  override def close: Unit = {
    if (conn != null) conn.close()
  }
}
