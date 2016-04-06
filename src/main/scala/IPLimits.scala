import scala.collection.immutable.HashMap

/**
  * Created by Andrii_Krasnolob on 3/11/2016.
  */
class IPLimits
  extends Serializable{
  var limitsMap:Map [Int, (Double, Long)] = new HashMap[Int, (Double, Long)]

  def add(key:Int, value:Double, period:Long): Unit ={
    limitsMap += (key -> (value, period))
  }

  def get(key:Int): Option[(Double, Long)] ={
    limitsMap.get(key)
  }
  def getValue(key:Int): Double ={
    limitsMap.get(key).get._1
  }

  def getPeriod(key:Int): Long ={
    limitsMap.get(key).get._2
  }

  override def toString: String = {
    var res = ""
    limitsMap.foreach(r => {
        res += r._1 + " -> " + s"(${r._2._1}, ${r._2._2})\n"
      })
    res
  }
}
