import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

import time
import os
import uuid
import datetime
import random
import json
import time
from azure.eventhub import EventHubProducerClient, EventData


connectionStr = "Endpoint=sb://eventhubnamespace.servicebus.windows.net/;SharedAccessKeyName=************"


# This script simulates the production of events for 3 vehicles.
vehicles = []
for x in range(0, 3):
    vehicles.append(x)

# Create a producer client to produce and publish events to the event hub.
producer = EventHubProducerClient.from_connection_string(conn_str=connectionStr, eventhub_name="test-hub")
currentTime = datetime.datetime.utcnow()
for y in range(0,1000000):    # For each vehicle, produce 1M events every 3 second. 
    event_data_batch = producer.create_batch() # Create a batch. You will add events to the batch later. 
    for veh in vehicles:
        # Create a dummy reading.
        speed = random.randint(70, 100)
        tripProgress = (speed*3)/3600        
#         reading = {'vehicleID': veh, 'timestamp': str(currentTime),'battery-charge-levels': random.randint(50,100),'speed': speed,'tripProgress': tripProgress,'lat': 37.773972+random.randint(1,100)/1000000, 'lng': -122.431297+random.randint(1,9)/1000000,'source':'EventHubMessage'}
        reading = {'vehicleID': veh,'timestamp': str(currentTime),'battery-charge-levels': random.randint(50,100),'speed': speed,'tripProgress': tripProgress,'lat': 37.773972+random.randint(1,100)/1000000, 'lng': -122.431297+random.randint(1,9)/1000000,'source':'EventHubMessage'}

#     ,,'source':'EventHubMessage'}
        s = json.dumps(reading) # Convert the reading into a JSON string.
        event_data = EventData(s)
        event_data.properties = {'Table' : 'BDTable' ,'IngestionMappingReference':'Mapping','Format':'json' }
        event_data_batch.add(event_data) # Add event data to the batch.
    producer.send_batch(event_data_batch) # Send the batch of events to the event hub.
    currentTime =currentTime + datetime.timedelta(0,3)
#     time.sleep(3) this is simulated in the above line not necessary to kill the service bus for simulation
# Close the producer.    
producer.close()