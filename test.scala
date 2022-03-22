package org.varcities


import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.fiware.cosmos.orion.spark.connector._

/**
  * cleanTemperature001LD Example Orion Connector
  * @author @sonsoleslp
  */
object cleanTemperature001LD {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Feedback")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create Orion Receiver. Receive notifications on port 9001
    val eventStream = ssc.receiverStream(new NGSILDReceiver(9001))
    // eventStream.print() // NgsiEventLD
    // NgsiEventLD(1647328131458,application/json,372,List(EntityLD(urn:ngsi-ld:Device:temperature001,TemperatureSensor,
    //  Map(temperature -> Map(type -> Property, value -> 1002, unitCode -> CEL, observedAt -> 2022-03-15T07:08:51.000Z)),[Ljava.lang.String;@1cbd4a36)))


  val averageTemperature = eventStream
      .flatMap(event => event.entities)
      .map(ent => ent.id -> {(ent.attrs("temperature")("value").asInstanceOf[Number].floatValue(),1L)} ) //(urn:ngsi-ld:Device:temperature001,(12.0,1))
      .reduceByKeyAndWindow((acc:(Float,Long),value:(Float,Long))=>{(acc._1 + value._1, acc._2 + value._2)}, Seconds(10)) // (urn:ngsi-ld:Device:temperature001,(40.0,3))
      .map((agg :  (String,(Float,Long))) =>  (agg._1, agg._2._1 / agg._2._2))
    averageTemperature.print()
// need to calculate the standard deviation


    ssc.start()
    ssc.awaitTermination()
  }


}