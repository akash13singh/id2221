import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import scala.util.Random

object SGen {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("metadata.broker.list", "127.0.0.1:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val topic = "avg"
    var i =0
    //to test change while(true) to soething like while(i<20)
    while (true) {
      Thread.sleep(10)
      val r = getRandomVal
      producer.send(new KeyedMessage[String, String](topic, null, r))
      println("generated: " + r)
      i=i+1
    }

    producer.close
  }

  def getRandomVal: String = {
    val i = Random.nextInt(alphabet.size)
    val key = alphabet(i)
    val value = Random.nextInt(alphabet.size)
    key + "," + value
  }

  val alphabet = 'a' to 'z'
}

