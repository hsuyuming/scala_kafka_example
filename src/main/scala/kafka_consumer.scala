
import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object kafka_consumer extends App{

  val props = new Properties()
  props.put("bootstrap.servers","192.168.2.125:19092,192.168.2.125:29092,192.168.2.125:39092")
  props.put("group.id","student")
  props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offser.reset","earliest")
  props.put("consumer.timeout.ms","10")
//  props.put("enable.auto.commit", "true")
//  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
//  val consumer = new KafkaConsumer[String,String](props)
  val topics = List("demo3")
  try{
    consumer.subscribe(topics.asJava)
    while(true){
      val records = consumer.poll(Duration.ofMillis(10))
      for(record <- records.asScala){
        println(record.value())
      }
    }
  }
  catch{
    case e : Exception => e.printStackTrace()
  }
  finally{
    consumer.close()
  }

}
