import java.util.Properties
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.{longSerde, stringSerde}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object StreamsJoinApplication extends App {

  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.kstream._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsJoin-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    p
  }

  val builder = new StreamsBuilder
  val amounts = builder.stream[String, String]("amounts")
  val rates = builder.table[String, String]("rates")
  val out = amounts.join(rates)((amounts: String, rates: String) => "amt is " + (amounts.toDouble * rates.toDouble).toString)

  out.to("out")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close()

  }
}
