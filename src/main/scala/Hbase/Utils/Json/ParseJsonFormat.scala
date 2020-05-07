package Hbase.Utils.Json

import com.alibaba.fastjson.JSON

class ParseJsonFormat extends Serializable {

  def parseJson1(str:String,key:String): String = {
    val json = JSON.parseObject(str)
    json.getString(key)
  }

  def test(str:String,key:String): String = {
    val json = JSON.parseObject(str)
    json.getString(key)
  }


  def parseJson2(str:String,key1:String,key2:String): String = {
    val json = JSON.parseObject(str)
    var str1=""
    try {
      str1 = json.getJSONObject(key1).getString(key2)
    } catch {
      case e1:NullPointerException => println("生成json对象时，空指针异常")
        str1="null"
      case _ => println("生成json解析对象遇到未知异常")
        str1="null"
    }
    str1
  }

  def parseJson3(str:String,key1:String,key2:String,key3:String): String = {
    val json = JSON.parseObject(str)
    var str1=""
    try {
      str1=json.getJSONObject(key1).getJSONObject(key2).getString(key3)
    } catch {
      case e1:NullPointerException => println("生成json对象时，空指针异常")
        str1="null"
      case _ => println("生成json解析对象遇到未知异常")
        str1="null"
    }
    str1
  }

  def parseJson4(str:String,key1:String,key2:String,key3:String,key4:String): String = {
    val json = JSON.parseObject(str)
    var str1=""
    try {
      str1=json.getJSONObject(key1).getJSONObject(key2).getJSONObject(key3).getString(key4)
    } catch {
      case e1:NullPointerException => println("生成json对象时，空指针异常")
        str1="null"
      case _ => println("生成json解析对象遇到未知异常")
        str1="null"
    }
    str1
  }
}
