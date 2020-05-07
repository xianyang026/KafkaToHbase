package DateUtil

import java.text.SimpleDateFormat
import java.util.Date

class TimeTrans extends Serializable {
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
