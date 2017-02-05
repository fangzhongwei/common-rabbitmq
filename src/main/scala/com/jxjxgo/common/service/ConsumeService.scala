package com.jxjxgo.common.service

import scala.concurrent.Future

/**
  * Created by fangzhongwei on 2016/11/21.
  */
trait ConsumeService {
  def consume(data:Array[Byte]):Future[Unit]
}
