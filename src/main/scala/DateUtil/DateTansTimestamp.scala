package DateUtil

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

/**
  * 日期转换为时间戳
  */

class DateTansTimestamp extends Serializable {
  def tranTimeToLong(tm:String)={
    var dt:Date=new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
       dt = fm.parse(tm)
      val aa = fm.format(dt)
    } catch {
      case e1:ParseException => println("错误的日期格式，已经换为本时刻")
      case _ =>  println("日期转时间戳时发生未知异常")
    }
    val tim: Long = dt.getTime()
    tim.toString.substring(0,10)
  }
}
