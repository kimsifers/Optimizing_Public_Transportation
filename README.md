# Optimizing_Public_Transportation
Inspired by Chicago Transport Authority Data 

# I can subscribe to all the topics and correct messages are processed
        subscribed_topics =['CTAProducersWeather','org.chicago.cta.stations.table.v1','org.chicago.cta.station.arrivals','TURNSTILE_SUMMARY'] 
        
        
I see that 'org.chicago.cta.station.arrivals'   gets to consumer.line but the station messages in not compeltely processed.  Although correctly produced and consumed: # print(f"line 37 arrival messages,{value}")

This is how I run the software: 
1 python producers/simulation.py

2 python consumers/ksql.py

3 python consumers/faust_stream.py worker

4 python consumers/server.py

w3m http://localhost:8889

Use sudo apt install w3m on install web browser 

20210207 - changes made to project based on 
Reviewer instructions: 

added enum to weather_value json 

to consumer.py self.broker_properties_avro and
self.broker_properties_notavro - added 
"auto.offset.reset": "earliest"

20210207 1340 Eastern European Standard Time 
Added to consumer.py: 
#Determine correct Consumer offset 
        
        if self.offset_earliest is True: 
            self.offset_earliest = "earliest" 
        else: 
            self.offset_earliest = "latest"

