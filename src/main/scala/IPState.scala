import scala.collection.mutable
import scala.collection.mutable.Queue
/**
  * Created by Andrii_Krasnolob on 3/7/2016.
  */
class IPState
  extends Serializable {
  var q:Queue[IPPacketsInfo] = new mutable.Queue[IPPacketsInfo]
  var totalCount = 0L
  var totalWireLen = 0
  var breached:Boolean = false
  var isReadyForALert:Boolean = false
  var limitType = 0
  var period = 0l
  var value = 0.
  def addValue(iPPacketsInfo: IPPacketsInfo, limitTypeNew:Int, periodNew:Long, valueNew:Double): Boolean ={
    limitType = limitTypeNew
    period = periodNew
    value = valueNew

    if (q.nonEmpty) {

      while (q.nonEmpty &&
        (iPPacketsInfo.timeStamp -  q.front.timeStamp).milliseconds > period * 1000 ) {
        totalCount -=  q.front.count
        totalWireLen -=  q.front.wirelen
        q.dequeue()

      }
    }
    totalCount += iPPacketsInfo.count
    totalWireLen += iPPacketsInfo.wirelen
    q.enqueue(iPPacketsInfo)
    isReadyForALert = checkBreach(limitType, value)
    isReadyForALert
  }

  def checkBreach(limitType:Int, value: Double): Boolean = {
    var oldBreached = breached

    if (( limitType == 1 && (totalWireLen / totalCount > value) )
       || ( limitType == 2 &&( totalWireLen > value ) ) )
      breached = true
    else
      breached = false
    oldBreached != breached
  }

  override def toString: String = {
    s"$totalCount : $totalWireLen : $breached"
  }

}
