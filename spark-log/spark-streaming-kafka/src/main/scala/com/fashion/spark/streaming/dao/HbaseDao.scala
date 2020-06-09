package com.fashion.spark.streaming.dao

import com.fashion.hbase.util.HBaseUtil
import com.fashion.spark.streaming.domain.ClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object HbaseDao {
  val tableName = "course_clickcount_1"
  val cf = "info"
  val qulifer = "click_count"

  /*
   保存信息
   */
  def save(list:ListBuffer[ClickCount]) ={
//     val table = HbaseU

    val table = HBaseUtil.getInstance().getTable(tableName);

    for (clickCount <- list){

      // incrementColumnValue是将之前的值跟当前的值相加
      table.incrementColumnValue(Bytes.toBytes(clickCount.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qulifer),
        clickCount.click_count)
    }
  }

  /**
    * 根据rowKey查询值
    */
  def count(day_courseId:String):Long = {
    val table = HBaseUtil.getInstance.getTable(tableName)

    val get = new Get(Bytes.toBytes(day_courseId))

    val value = table.get(get).getValue(cf.getBytes(),qulifer.getBytes())

    if(value ==null){
      0l
    }else{
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {
//    val list = new ListBuffer[ClickCount]
//
//    list.append(ClickCount("20201111_1",8l))
//    list.append(ClickCount("20201111_9",9l))
//    list.append(ClickCount("20201111_8",100l))
//
//    save(list)

    println(count("20200607_112"))
  }


}
