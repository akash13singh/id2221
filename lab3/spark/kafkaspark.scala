import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
    def main(args: Array[String]) {

        val kafkaConf = Map(
                "metadata.broker.list" -> "localhost:9092",
                "zookeeper.connect" -> "localhost:2181",
                "group.id" -> "kafka-spark-streaming",
                "zookeeper.connection.timeout.ms" -> "1000")

       val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
       val ssc = new StreamingContext(sparkConf,Seconds(2))
       ssc.checkpoint("checkpoint")

       // if you want to try the receiver-less approach, comment the below line and uncomment the next one
       //val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder]()
       val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaConf,Set("avg"))

       // the messages are of format: <key,"a,24">
       // ignore the key, use the value to extract the letter and value and form pais (a,24)
       val pairs = messages.map(s=>(s._2.split(",")(0),s._2.split(",")(1).toInt))
       
       // key:String letter
       // value: the current value of the letter
       // state: State[Tuple2[Int,Double]] . Stores the count:Int (cumulative frequency of the letter so far) and avg:Double( the average so far of the letter)
       def mappingFunc(key: String, value: Option[Int], state: State[Tuple2[Int,Double]]): Option[(String, Double)] = {
           val oldState:Tuple2[Int,Double]  =  state.getOption.getOrElse((0,0))
           val count:Int = oldState._1
           val oldAvg:Double  = oldState._2
           val numer = count*oldAvg + value.getOrElse(0)
           val denom = count+1
           val avg = numer/denom
           state.update(count+1,avg)
           Some((key,avg))
       }  

       // on pairs use map with state to calculate the running average.
       val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

       stateDstream.print()
       ssc.start()
       ssc.awaitTermination()
    }
}
