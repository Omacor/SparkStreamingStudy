/**
 * Created by macor on 15-11-28.
 */
import java.util.Properties
import scala.collection.JavaConversions._
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.utils._
import kafka.utils.Logging
import kafka.consumer.KafkaStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class ScalaConsumerExample(val zookeeper: String= "slave9:2181,slave10:2181,slave11:2181",
                           val groupId: String="idea-consumer",
                           val topic: String="secure_log",
                           val delay: Long=100L) extends Logging {
	val config = createConsumerConfig(zookeeper, groupId)
	val consumer = Consumer.create(config)
	var executor: ExecutorService = null
	def shutdown() = {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}
	def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
		val props = new Properties()
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("auto.offset.reset", "largest");
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		val config = new ConsumerConfig(props)
		config
	}
	def run(numThreads: Int) = {
		val topicCountMap = Map(topic -> numThreads)
		val consumerMap = consumer.createMessageStreams(topicCountMap);
		val streams = consumerMap.get(topic).get;
		executor = Executors.newFixedThreadPool(numThreads);
		println("Begin consumer...")
		var threadNumber = 0;
		for (stream <- streams) {
			executor.submit(new ScalaConsumerTest(stream, threadNumber, delay))
			threadNumber += 1
		}
	}
}

class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val delay: Long) extends Logging with Runnable {
	def run {
		val it = stream.iterator()
		while (it.hasNext()) {
			val msg = new String(it.next().message());
			System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + msg);
		}
		System.out.println("Shutting down Thread: " + threadNumber);
	}
}

object ConsumerExample extends App{
	val example = new ScalaConsumerExample("slave9:2181,slave10:2181,slave11:2181","idea-consumer","secure_log",100L)
	example.run(2)
}
