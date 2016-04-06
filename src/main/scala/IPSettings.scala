import scala.collection.immutable.HashMap

/**
  * Created by Andrii_Krasnolob on 3/4/2016.
  */
class IPSettings()
  extends Serializable{
  var settings:Map[ String, IPLimits] = new HashMap[String, IPLimits]

  def add(newHostIp: String, newLimitType: Int, newValue: Double, newPeriod: Long):Unit={
    var limits = settings.getOrElse(newHostIp, new IPLimits())

    if (newHostIp == null && limits.get(newLimitType).isEmpty){
      limits.add( newLimitType, newValue, newPeriod )
    }

    if (newHostIp != null) {
      limits.add( newLimitType, newValue, newPeriod )
    }
    settings += (newHostIp -> limits )
  }

  def get(ip:String):IPLimits={
    settings.getOrElse(ip, settings.get(null).get)
  }

  override def toString: String = {
    var res = ""
    settings.foreach(s => {
      res += s"\n${s._1} limits:\n${s._2.toString()}"
    })
    res
  }
}
