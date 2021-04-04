"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from producers.models.turnstile import Turnstile
from producers.models.producer import Producer


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    #
    # TODO: Define this value schema in `schemas/station_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

        def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #https://dattell.com/data-architecture-blog/kafka-optimization-how-many-partitions-are-needed/
        #I liked the calculation here. I will start with these values because I do not know the 
        #the technical limitations.  It seem like with 8 stations and 12 arrivals an hour 
        #we should need to process 10 mb per topic X 8 Stations # 12 arrivals X 60 seconds 
        #x 60 minutes = 34 560 000 mb per hour 
        #topic_name = topic name principals, Business Name Chicago Transport Authority (CTA), Name of the Python program, Name of the class # TODO: Come up with a better topic name
        #But for station.py I used the topic name that was used in Consumer
        #lines.py and server.py in order to resolve consuming problems 
        topic_name = "org.chicago.cta.station.arrivals"
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema, # TODO: Uncomment once schema is defined
            num_partitions=2,
            num_replicas=1,  
        )

        
        #self.topic.name = topic_name 
        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)
        self.line = color.name
        #self.train_status = train_status.name
        
   

    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        

    
        #
        #
        # TODO: Complete this function by producing an arrival message to Kafka
        #
        # call the producer.produce class, try to find errors, 
        # for testing purposes printed successful processing 
        
        
        
        
    
         
        try: 
            self.producer.produce(  
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station_id,
                    "train_id": train.train_id,
                    "direction": direction,
                    "line": self.line, 
                    "train_status": train.status.name,
                    "prev_station_id": prev_station_id,
                    "prev_direction": prev_direction,}
                    )
            
        except Exception as e:
            logger.info("arrival kafka integration incomplete - skipping")
            print(f"Exception while producing record value to topic -, {self.topic_name}: {e}")
        #else:
            #print(f"Station line 105 Successfully producing record value to topic -, {self.topic_name}")
            
        
        
            

            

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()

