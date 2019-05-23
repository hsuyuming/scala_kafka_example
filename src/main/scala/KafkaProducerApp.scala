
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}



object KafkaProducerApp extends App{

  val props = new Properties()

  props.put("bootstrap.servers","192.168.2.125:19092,192.168.2.125:29092,192.168.2.125:39092")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val producer = new KafkaProducer[String,String](props)
  val topic = "quick-start"
  try {
    for( i <- 0 to 100000){
      val record = new ProducerRecord[String,String](topic,i.toString,"Abe testing " + i)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset()
      )
    }
  }catch {
    case e:Exception => e.printStackTrace()
  }
  finally {
    producer.close()
  }
}
