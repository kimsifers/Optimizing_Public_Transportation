"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from producers.models.producer import Producer
from producers.models.turnstile_hardware import TurnstileHardware

# to work had to change the from, need to take into consideration 
# intit_py 




logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")
    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
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
        #In real life more partions and replicas would be needed

        
        
        super().__init__(
            # TODO: Come up with a better topic name
            topic_name = "CTAProducersTurnstile",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema, 
            num_partitions= 1,
            num_replicas= 1,
        )
    
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        # good practice is to verify errors 
        # for testing printed that records were being created
        try: 
            for i in range (num_entries):
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name
                    }
                )
                
        except Exception as e:
            logger.info("turnstile kafka integration incomplete - skipping")
            print(f"Exception while producing record value to topic - {self.topic_name}: {e}")
        #else:
            #print(f"Successfully producing record value to topic - {self.topic_name}")
                
                
