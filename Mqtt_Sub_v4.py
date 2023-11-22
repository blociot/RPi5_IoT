import random
import json
from paho.mqtt import client as mqtt_client
from influxdb import InfluxDBClient

DBClient = InfluxDBClient(host='192.168.0.104', port=8086)
DBClient.create_database('BlocIoT_DB')
print(DBClient.get_list_database())
DBClient.switch_database('BlocIoT_DB')
broker = '192.168.0.104'
port = 1883
topic1 = "/lab/Sensor/Temperature1"
topic2 = "/lab/Sensor/Humidity1"
client_id = f'subscribe-{random.randint(0, 100)}'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Successfully Connected to MQTT Broker!")
        else:
            print("Failed to connect to MQTT Broker, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        TempVal = msg.payload.decode().split(":")
        if (msg.topic == "/lab/Sensor/Temperature1") and (TempVal[0] == "T") :
            print(f"Temperature1 Value is: {TempVal[1]}")
            json_body = [
                {
                    "measurement":"lab1",
                    "tags":{
                        "location":"lab",
                        "category":"sensor"
                        },
                    "fields": {
                        "deviceID":1,
                        "value":int(TempVal[1]),

                        }
                    }
                ]
            DBClient.write_points(json_body)
        elif (msg.topic == "/lab/Sensor/Humidity1") and (TempVal[0] == "H") :
            print(f"Humidity Value is: {TempVal[1]} %")
            json_body = [
                {
                    "measurement": "lab2",
                    "tags":{
                        "location":"lab",
                        "category":"sensor"
                    },
                    "fields": {
                        "deviceID":2,
                        "value":int(TempVal[1]),
                    }
                }]
            DBClient.write_points(json_body)
    client.subscribe(topic1)
    client.subscribe(topic2)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()