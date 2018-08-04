package com.vkd.elasticsearch.client


import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try

import org.slf4j.LoggerFactory


object SensexData {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("SensexData")

    val file = Source.fromFile("src/main/data/SENSEX.csv")

    var lst = new ListBuffer[String]()
    val lines = file.getLines()
    lines.next() //skip the first line(headers)
    lines.foreach(line => {
      //sample line
      //31-July-2003,3796.53,3835.75,3785.12,3792.61,
      val arr = line.split(",")
      val date = dateToTimestamp(arr(0))
      lst +=
        s"""{"timestamp":"$date",
           |"Open":${stringToDouble(arr(1))},
           |"High":${stringToDouble(arr(2))},
           |"Low":${stringToDouble(arr(3))},
           |"Close":${stringToDouble(arr(4))}}""".stripMargin

      if (lst.length % 400 == 0) {
        IndexClient.transportClient("stock", "bse", null, lst.toList)
        lst = new ListBuffer[String]()
      }
    })
    if (lst.nonEmpty) {
      IndexClient.transportClient("stock", "bse", null, lst.toList)
    }

    file.close()

  }

  def dateToTimestamp(str: String): String = {
    val simpleDateFormat = new SimpleDateFormat("d-MMMM-yyyy", Locale.ENGLISH)
    val date = simpleDateFormat.parse(str)
    new SimpleDateFormat("yyyy-MM-dd'T'mm:ss:SS").format(date)
  }

  def stringToDouble(str: String): Double = {
    Try {
      str.trim.toDouble
    }.getOrElse(0.0)
  }
}
