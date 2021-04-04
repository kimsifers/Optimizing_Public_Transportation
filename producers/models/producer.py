#"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)




class Producer:
    """Defines and provides common functionality amongst Producers"""
    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,            
        
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        
        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
        # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
        # configuration for avro producer and adminclient are different
   

        self.broker_properties = AdminClient({
            "bootstrap.servers": "PLAINTEXT://localhost:9092"})
        
        self.avro_config_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081"}
          
                                   
    
        
        

        # If the topic does not already exist, try to create it
        
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        else: pass  #print(f"topic already exists,{self.topic_name}") use print for testing
            


        # TODO: Configure the AvroProducer
        #https://docs.confluent.io/clients-confluent-kafka-python/current/index.html#ak-producer 
       

   
        
        
        
        #while True:
        #
        # TODO: Replace with an AvroProducer produce. Make sure to specify the schema!
        #       Tip: Make sure to serialize the ClickEvent with `asdict(ClickEvent())`
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=loads#confluent_kafka.avro.AvroProducer
        #https://medium.com/better-programming/avro-producer-with-python-and-confluent-kafka-library-4a1a2ed91a24 
        # # good practice domain naming convention
        # domain,model,event type 
        
    
    
        self.producer = AvroProducer(self.avro_config_properties,
        default_key_schema = self.key_schema,                                                       default_value_schema =  self.value_schema)
            
            
   
    
    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #

        """Creates the topic with the given topic name"""
    # TODO: Create the topic. Make sure to set the topic name, the number of partitions, the
    # replication factor.
    #
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
    #https://stackoverflow.com/questions/26021541/how-to-programmatically-create-a-topic-in-apache-kafka-using-python
    #futures = #20201205 
    #configured clean up policy, comppression tupes, retention, and delete dalay 
    # to hadle topic closing  
        
        print(f"topic,{self.topic_name}") 
        print(f"num_partitions,{self.num_partitions}")
        print(f"replication_factor,{self.num_replicas}")
    
        futures = self.broker_properties.create_topics([NewTopic( 
        
        topic = self.topic_name,
      
        num_partitions = self.num_partitions,
        replication_factor = self.num_replicas, 
        #config = {
        #    "cleanup.policy": "delete",
        #    "compression.type": "gzip",
        #    "delete.retention.ms": "2000",
        #    "file.delete.delay.ms": "2000"}
            ) ] ) 
    
        
        for topic, future in futures.items():
            try:
                future.result()
                #print("topic created")
            except Exception as e:
                print(f"failed to create topic {topic_name}: {e}")
                logger.info("topic creation kafka integration incompleted - skipping")
                raise


        
        

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):

#https://medium.com/@sunny_81705/kafka-log-retention-and-cleanup-policies-c8d9cb7e09f8#:~:text=In%20Kafka%2C%20unlike%20other%20messaging,Its%20a%20Topic%20level#%20configuration 

        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
         #config={
         #   "cleanup.policy": self.cleanup.policy,
         #   "compression.type": self.compression.type,
         #   "delete.retention.ms": self.delete.retention.ms ,
         #   "file.delete.delay.ms": self.file.delete.delay.ms 
         #   }
	
        if self.producer is not None:
            logger.debug("flushing producer...")
            self.producer.flush()
        else: 
            logger.info("producer close incomplete - skipping")


        

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

        
