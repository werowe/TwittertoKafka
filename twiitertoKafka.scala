import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.Status;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
import twitter4j.StatusDeletionNotice;
import twitter4j.FilterQuery;
import java.util.Properties
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


val props =new Properties
props.put("bootstrap.servers", "0.0.0.0:2182")
props.put("acks", "all")
props.put("retries",new Integer( 0))
props.put("batch.size",new Integer( 16384))
props.put("linger.ms",new Integer( 1))
props.put("buffer.memory", new Integer(33554432))
props.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer")

 
val producer = new KafkaProducer[String, String](props)


val cb5 = new ConfigurationBuilder();

cb5.setDebugEnabled(true).setOAuthConsumerKey("get your own").setOAuthConsumerSecret("x").setOAuthAccessToken("x").setOAuthAccessTokenSecret("x");

def listener = new StatusListener() {
  def onStatus(status: Status) { val message =  new ProducerRecord[String, String]("trump", null, status.getText)
                                 producer.send(message)
                                 println(status.getText)
                                 } 
                                 
 
                                 
  def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
  def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
  def onException(ex: Exception) { ex.printStackTrace }
  def onScrubGeo(arg0: Long, arg1: Long) {}
  def onStallWarning(warning: StallWarning) {}
}



val twitterStream = new TwitterStreamFactory(cb5.build()).getInstance
twitterStream.addListener(listener)
twitterStream.filter(new FilterQuery().track("trump"))

Thread.sleep(5000)
twitterStream.cleanUp
twitterStream.shutdown 

