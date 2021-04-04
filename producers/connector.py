"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests



logger = logging.getLogger(__name__)

#	* Please refer to the [Kafka Connect JDBC Source Connector Configuration Options](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-#connector/source_config_options.html) for documentation on the options you must complete.
#	* You can run this file directly to test your connector, rather than running the entire simulation.
#	* Make sure to use the [Landoop Kafka Connect UI](http://localhost:8084) and [Landoop Kafka Topics UI](http://localhost:8085) to check the status and output of #the Connector.
#	* To delete a misconfigured connector: `CURL -X DELETE localhost:8083/connectors/stations`

#First, use the REST API to check the connector status. curl http:<connect_url>/connectors/<your_connector>/status to see what the status of your connector is
#Next, use the REST API to check the task status for the connector. curl http:<connect_url>/connectors/<your_connector>/tasks/<task_id>/status to see what the status #of your task is

#If you can’t deduce the failure from these two options, the next best bet is to examine the logs of Kafka Connect. Typically, a tool like tail or less is useful in #examining the logs for Kafka Connect. On Linux systems, Kafka Connect logs are often available in /var/log/kafka/. Kafka Connect is often verbose and will indicate #what the issue is that it is experiencing.



KAFKA_CONNECT_URL = "http://localhost:8083/connectors"

CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")
    print("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return



    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    # I have put "poll.interval.ms": "3600000" - once an hour should be 
    # enough, stattion datat would be updated on a daily basis 
    # I had put "poll.interval.ms": "5000" and got no data 
    # seem like KAFKA does not like that defaults are used 
    logger.info("connector code not completed skipping connector creation")
    print("make request.post")
            
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "tasks.max": 1, 
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "CTAConnector", 
                "poll.interval.ms": "3600000"
            },
          }
        ),
    )
    
    


    ## Ensure a healthy response was given
    
    
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")
    print("Use kafka-console-consumer and kafka-topics to see data!")

    
    

if __name__ == "__main__":
    configure_connector()
