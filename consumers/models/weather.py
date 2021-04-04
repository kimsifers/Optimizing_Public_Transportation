"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"
        print("weather line 15")

    def process_message(self, message):
        """Handles incoming weather data"""
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
    
        
        
        try:
            #value = json.loads(message.value())
            value = message.value()
            self.temperature = value.get('temperature')
            self.status = value.get('status')
            print(f"weather line 31,{self.status}")
        except Exception as e:
            logger.error(e)
            logger.info("weather process_message is incomplete - skipping")
