"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

from confluent_kafka.admin import AdminClient, NewTopic

import topic_check



logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON
# https://markhneedham.com/blog/2019/05/20/kql-create-stream-extraneous-input/ how to fix if your field 

# names are KSQL keywords e.g. order 
#create stream CTAFaustStreamTransformedKSQL (station_id #INTEGER, station_name VARCHAR, `or
#der` INTEGER,  line VARCHAR) WITH (KAFKA_TOPIC='CTAFaustStreamTransformed',  #VALUE_FORMAT='JSON'
#https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/scalar-functions/#substring
# https://www.confluent.io/blog/troubleshooting-ksql-part-1/ 

KSQL_STATEMENT = """
create table CTAProducersTurnstileTableAVRO (timestamp BIGINT, station_id INTEGER, station_name VARCHAR, line VARCHAR) WITH (KAFKA_TOPIC = 'CTAProducersTurnstile',  VALUE_FORMAT='avro', KEY='station_id');

create table turnstile_summary with (value_format = 'JSON') as  
select station_id, count(station_id) as count  from CTAPRODUCERSTURNSTILETABLEAVRO group by station_id;
"""

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""    
    
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")
    
  
    
    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )
    

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
