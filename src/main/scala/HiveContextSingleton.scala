import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Andrii_Krasnolob on 3/17/2016.
  */


object HiveContextSingleton {
  lazy val hql:HiveContext = createHiveContext(sc.get)
  var sc: Option[SparkContext] = None

  def  createHiveContext(sc:SparkContext): HiveContext ={
    new HiveContext(sc)
  }
  def setSparkCOntext(sparkContext: SparkContext): Unit ={
    sc =  Option(sparkContext)
  }

}