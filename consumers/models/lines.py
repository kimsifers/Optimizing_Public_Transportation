"""Contains functionality related to Lines"""
import json
import logging

#from line import Line
from consumers.models.line import Line
# from models import Line 
#from line import Line

logger = logging.getLogger(__name__)

print("Lines line 11")

class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")
        print("lines line 21")

    def process_message(self, message):
        """Processes a station message"""
        # decided to use original names both for producing and consuming
        # to fixe problems 
        if "org.chicago.cta.station" in message.topic():
            value = message.value()
            #print(f"lines line 28 value,{value}")
            if message.topic() == "org.chicago.cta.stations.table.v1":
                #print("Lines line 32")
                value = json.loads(value)
                #print(f"lines line 35,{value}")
            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif "TURNSTILE_SUMMARY" == message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())
