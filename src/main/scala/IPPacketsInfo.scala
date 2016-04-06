import org.apache.spark.streaming.Time

/**
  * Created by Andrii_Krasnolob on 3/4/2016.
  */
class IPPacketsInfo(newWireLen:Int, newCount:Long)
  extends Serializable {

  var wirelen:Int = newWireLen
  var count:Long = newCount
  var timeStamp:Time = Time(0)

  def setWireLen(newWireLen:Int): Unit ={
    wirelen=newWireLen
  }
  def setCount(newCount:Long): Unit = {
    count = newCount
  }
  def setTimeStamp(time:Time): Unit ={
    timeStamp = time
  }
  override def toString: String = {
    s"$wirelen : $count : $timeStamp"
  }

}

