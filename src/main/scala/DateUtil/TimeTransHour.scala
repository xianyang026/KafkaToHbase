package DateUtil

import java.text.SimpleDateFormat
import java.util.Date

class TimeTransHour extends Serializable {
  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd-HH")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }
}
