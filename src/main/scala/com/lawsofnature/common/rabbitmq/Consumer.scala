package com.lawsofnature.common.rabbitmq

import java.util.concurrent.{ExecutorService, Executors}

import com.rabbitmq.client.{Channel, Connection, ConnectionFactory, QueueingConsumer}

/**
  * Created by fangzhongwei on 2016/10/22.
  */
object Consumer extends App {
  var factory: ConnectionFactory = new ConnectionFactory()
  factory.setUsername("fangzhongwei")
  factory.setPassword("fzw@2016")
  factory.setVirtualHost("/")
  factory.setHost("192.168.181.130")
  factory.setPort(5672)
  var service: ExecutorService = Executors.newFixedThreadPool(500)
  factory.setSharedExecutor(service)
  var connection: Connection = factory.newConnection()
  //共用connection
  var channel: Channel = connection.createChannel()

  //  channel.queueDeclare("firstQueue", false, false, false, null)
  channel.queueDeclare("secondQueue", false, false, false, null)
  System.out.println(" [*] Waiting for messages. To exit press CTRL+C")

  var consumer: QueueingConsumer = new QueueingConsumer(channel)
  channel.basicConsume("firstQueue", true, consumer)

  while (true) {
    val delivery: QueueingConsumer.Delivery = consumer.nextDelivery()
    val message: String = new String(delivery.getBody())
    System.out.println(" [x] Received '" + message + "'")
  }
}
