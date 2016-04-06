import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.joda.time.Hours

import scala.collection.mutable

/**
  * Created by Andrii_Krasnolob on 3/3/2016.
  */
object SparkStreamSandbox {
  val networkDeviceName: String = "\\Device\\NPF_{6AE37950-B19A-4A4F-B3AC-2425236BE527}"
  val checkpointDirectory: String = "checkpoint"
  private val windowWidth: Duration = Seconds(10)
  val numberOfThreads = 3
  val batchInterval: Long = 1

  def main(args:Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\BigData\\Hadoop")
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDirectory, createContext _)
    LogManager.getRootLogger.setLevel(Level.ERROR)
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def createContext(): StreamingContext = {
    val conf = new SparkConf().setMaster(s"local[$numberOfThreads]").setAppName(SparkStreamSandbox.getClass.toString)
    val ssc = new StreamingContext(conf, Seconds(batchInterval))
    ssc.checkpoint(checkpointDirectory)
    processData(ssc)
    ssc.remember(Seconds(10))
    ssc
  }

  def processData(ssc: StreamingContext): Unit = {
    val lines = ssc.receiverStream(new CustomReceiverJnetPcap(networkDeviceName))
    val ipSettings = new IPSettings
    HiveContextSingleton.setSparkCOntext(ssc.sparkContext)
    var hql = HiveContextSingleton.hql
    createHourlyStatisticTable(hql)

    val settings = loadSettings(hql).collect.
      foreach(r => ipSettings.add(
        r.getAs[String]("hostIp")
        , r.getAs[Long]("limitType").toInt
        , r.getAs[Double]("value")
        , r.getAs[Long]("period")))

    val reducedStream = lines
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    val schema =
      StructType(
        (Array(StructField("HostIP", StringType, true)
          , StructField("TrafficConsumed", IntegerType, true)
          , StructField("AverageSpeed", DoubleType, true))
          )
      )

    val hourlySnaphot = reducedStream.window(windowWidth, windowWidth)
      .reduceByKeyAndWindow( (a, b) => (a._1 + b._1, a._2 + b._2), windowWidth )
      .map(a => Row(a._1, a._2._1,  a._2._1.toDouble/a._2._2))

    hourlySnaphot.foreachRDD(
        (rdd,time) => {
          HiveContextSingleton.setSparkCOntext(rdd.sparkContext)
          var hql = HiveContextSingleton.hql
          hql
            .createDataFrame(rdd, schema)
            .registerTempTable("temp")
          hql.sql("INSERT INTO table hourlySatistic select  '"+time.milliseconds.toString+"', *  from temp")
        }
      )

    val packetsWithSettings=reducedStream
      .map(r => (r._1, (new IPPacketsInfo(r._2._1, r._2._2), ipSettings.get(r._1))))

    val props: Properties = initKafkaProperties()
    val kafkaSink = KafkaSink(props)

    val stateSpec = StateSpec.function(trackStateFunc _)
    val wordCountStateStream = packetsWithSettings.mapWithState(stateSpec)
      .foreachRDD((rdd, time) => {
        println("-" * 40 + time)

        rdd.foreachPartition(
          p => p.foreach(x => {
            if (x._2._1.isReadyForALert  && x._2._1.breached) {
              println(getAlertMessage(x._1, x._2._1))
              kafkaSink.send("alerts", getAlertMessage(x._1, x._2._1))
            }
            if (x._2._2.isReadyForALert && x._2._2.breached) {
              println(getAlertMessage(x._1, x._2._2))
              kafkaSink.send("alerts", getAlertMessage(x._1, x._2._2))
            }
          }
          )
        )
      }
      )
  }

  def loadSettings(hql: HiveContext): DataFrame = {

    populateTestingData(hql)
    val settings: DataFrame = hql.sql("Select " +
      "s1.hostIp, " +
      "s1.limitType,  " +
      "nvl(s1.value, s2.value) as value, " +
      "nvl(s1.period, s2.period) as period " +
      "from settings s1 left join (select * from settings where hostIp is null) s2 on s1.limitType = s2.limitType").toDF()
    settings.show()
    validateSettings(settings)
    settings
  }

  def populateTestingData(hql: HiveContext): Unit = {
    hql.read.json("D:\\Share\\Spark_Basics_p2\\hive_settings.json").registerTempTable("settings")
  }

  def validateSettings(settings: DataFrame): Unit = {
    val defaultRecord = settings.filter("hostIp is null")
   // defaultRecord.show()
    if (defaultRecord.count() != 2) {
      LogManager.getRootLogger.error("Table should always has two DEFAULT parameters")
      throw new scala.RuntimeException("Table should always has two DEFAULT parameters")
    }
    val tmpDF = defaultRecord.filter("value is not null and period is not null and (limitType = 1  or limitType = 2)")
    if (tmpDF.count() != 2) {
      LogManager.getRootLogger.error("Invalid default settings")
      throw new scala.RuntimeException("Invalid default settings")
    }
  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[(IPPacketsInfo, IPLimits)], state: State[(IPState, IPState)]): Option[(String, (IPState, IPState))] = {

    val emptyValue = (new IPPacketsInfo(0,0), null)
    val emptyState = (new IPState(), new IPState())

    var (currentState_1, currentState_2) = state.getOption().getOrElse(emptyState)
    var (currentValue, currentSettings) = value.getOrElse(emptyValue)
    currentValue.setTimeStamp(batchTime)

    currentState_1.addValue(currentValue, 1, currentSettings.getPeriod(1),currentSettings.getValue(1) )
    currentState_2.addValue(currentValue, 2, currentSettings.getPeriod(2),currentSettings.getValue(2) )

    val output = (key, (currentState_1, currentState_2))
    state.update((currentState_1, currentState_2))
    Some(output)
  }

  def getAlertMessage(key:String, state:IPState ): String ={
    var (limitType, thresholdValue) = if (state.limitType == 1) ("Threshold", state.totalWireLen/state.totalCount) else ("Limit",  state.totalWireLen)
    var isBreached = if (state.breached) "Exceeded" else "Norm"

    s"${java.util.UUID.randomUUID()} ${limitType}${isBreached} for $key ${state.q.last.timeStamp} FactValue=${thresholdValue}, ThresholdValue=${state.value}, Period = ${state.period}"
  }

  def createHourlyStatisticTable(hql: HiveContext): DataFrame = {
    hql.sql("create table if not exists hourlySatistic ( " +
      "timeStamp STRING, " +
      "HostIP STRING, " +
      "TrafficConsumed INT, " +
      "AverageSpeed DOUBLE) " +
      "STORED AS ORC")
  }

  def initKafkaProperties(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}

/*
    val setTmp = new IPSettings()
    setTmp.add("255.255.255.255", 1, 10.0, 500L)
    setTmp.add("255.255.255.255", 2, 20.0, 500L)
    setTmp.add(null, 2, 30.0, 500L)
    setTmp.add(null, 1, 40.0, 400L)
    println(setTmp)

    println (setTmp.get("255.255.285.255"))
*/
