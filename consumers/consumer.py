"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        
        
        #Determine correct Consumer offset 
               
        if self.offset_earliest is True: 
            self.offset_earliest = "earliest" 
        else: 
            self.offset_earliest = "latest"
        
        
        #print(f"consumer line 37,{self.offset_earliest}")

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #https://medium.com/better-programming/consume-messages-from-kafka-topic-using-python-and-avro-consumer-eda5aad64230
        #https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
        #print("consumer line 40")
        self.broker_properties_avro = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081",
            "group.id": "CTAConsumerAvro",
            "auto.offset.reset": self.offset_earliest
        }

        self.broker_properties_notavro = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "group.id": "CTAConsumerNotAvro",
            "auto.offset.reset": self.offset_earliest
        }  
    
    
    
        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.consumer = AvroConsumer(self.broker_properties_avro)
     
            
            
        else:
            self.consumer = Consumer(self.broker_properties_notavro)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        # self.consumer.subscribe( TODO )
        
    
        # I can subscribe to all the topics and correct messages are processed
        subscribed_topics =['CTAProducersWeather','org.chicago.cta.stations.table.v1','org.chicago.cta.station.arrivals','TURNSTILE_SUMMARY'] 
        # I can see these tpics in the web page  
        #subscribed_topics =['CTAProducersWeather','org.chicago.cta.stations.table.v1','TURNSTILE_SUMMARY'] 
        #subscribed_topics = ['org.chicago.cta.station.arrivals']
        
       
        self.consumer.subscribe(subscribed_topics,on_assign=self.on_assign)
      
        
        

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
    
      
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        logger.info("on_assign is incomplete - skipping")
        #print ("consumer line 89 onassign partiontion,{partitions}")
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING    
            partitionnumber = consumer.assign(partitions)
         
            
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)
        

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
       
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)
    
    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
                
        try:
            message = self.consumer.poll(self.consume_timeout)
            if message is None:
                logger.debug("no message")
                #print("consumer line 189 no message")
                return 0
            elif message.error() is not None:
                logger.info("_consume is incomplete")
                return 0
            else:
                #check here for correct consuming
                #print("consumer line 152")
                #print(f"consumer line 153 Successfully poll a record from "f"Kafka topic:{message.topic()}")
                #print(f"consumer line 154 Successfully poll a record from "f"Kafka value:{message.value()}")
                self.message_handler(message)
                return 1
        except Exception as e:
            logger.error(str(e))
            print(f"consumer line 199,{e}")
            return 0

                               
                      
    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
