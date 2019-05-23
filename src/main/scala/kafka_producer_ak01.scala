
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafka_producer_ak01 extends App{
  val props = new Properties()
  props.put("bootstrap.servers","1192.168.2.125:19092,192.168.2.125:29092,192.168.2.125:39092")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String,String](props)
  val topic = "demo3"
  try {
    for (i <- 0 to 200) {
      val record = new ProducerRecord[String, String](topic, i.toString, "Abe test " + i)
      val metadata = producer.send(record)
      printf("sent record (key=%s,value=%s) meta(partition=%d, offset=%d)", record.key(), record.value(), metadata.get().partition(), metadata.get().offset())
    }
  }
  catch{
    case e : Exception => e.printStackTrace()
  }
  finally {
    println("finally")
    producer.close()
  }
}
