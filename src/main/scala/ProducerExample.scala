/**
 * Created by macor on 15-11-28.
 */
import java.util.Properties
//import kafka.producer.ProducerConfig

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.util.Random
import java.util.Date

object ProducerExample extends App{
	/*
	def main(args:Array[String]): Unit ={

	}
	*/
	val events = 20   //args(0).toInt
	val topic = "page_visits"  //   args(1)
	val brokers = "slave9:9092,slave10:9092,slave11:9092"
	val rnd = new Random()
	val props = new Properties()
	props.put("metadata.broker.list", brokers)
	props.put("serializer.class", "kafka.serializer.StringEncoder")
	//props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
	props.put("producer.type", "async")
	//props.put("request.required.acks", "1")
	val config = new ProducerConfig(props)
	val producer = new Producer[String,String](config)
	val t = System.currentTimeMillis()
	for (nEvents <- Range(0, events)) {
		val runtime = new Date().getTime()
		val ip = "192.168.2." + rnd.nextInt(255)
		val msg = runtime + "," + nEvents + ",www.example2.com," + ip + " " +nEvents
		val data = new KeyedMessage[String, String](topic, ip, msg)
		println(msg)
		producer.send(data);
	}
	producer.close
	System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
}
