import json, os
from paho.mqtt.client import Client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv
from datetime import timedelta, timezone, datetime
from queue import Queue
import threading

# .env 로드
load_dotenv()

# 큐 생성
data_queue = Queue()

# MQTT 설정
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = os.getenv("MQTT_TOPIC")

# InfluxDB 설정
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# InfluxDB 클라이언트 생성
influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = influx_client.write_api()

# MQTT 메시지 수신 → 큐에 저장
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic
        data_queue.put((topic, payload))  #큐에 저장
    except Exception as e:
        print("수신 에러:", e)

# 큐에서 꺼내 InfluxDB에 쓰는 워커 스레드
def influx_worker() :
    kst = timezone(timedelta(hours=9))
    while True:
        topic, payload = data_queue.get()
        try:
            now_kst = datetime.now(kst)
            point = None

            if topic == "sensor/aircondition":
                point = (
                    Point("aircondition_sensor")
                    .field("temperature", float(payload['temperature']))
                    .field("humidity", float(payload['humidity']))
                    .field("pan", float(payload['pan']))
                    .field("peltier", float(payload['peltier']))
                    .time(now_kst, WritePrecision.MS)
                )

            elif topic == "sensor/air_purifier":
                point = (
                    Point("air_purifier_sensor")
                    .field("air_status", float(payload['air_status']))
                    .field("pan", float(payload['pan']))
                    .time(now_kst, WritePrecision.MS)
                )

            elif topic == "sensor/smart_curtain":
                point = (
                    Point("smart_curtain")
                    .field("switch", int(payload['switch']))
                    .field("pan", float(payload['pan']))
                    .time(now_kst, WritePrecision.MS)
                )

            elif topic == "sensor/smart_light":
                point = (
                    Point("smart_light")
                    .field("illuminance", int(payload['illuminance']))
                    .field("light", float(payload['light']))
                    .time(now_kst, WritePrecision.MS)
                )

            if point:
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                print(f"[{topic}] 저장 완료 ")

        except Exception as e:
            print("DB 저장 중 에러:", e)
        finally:
            data_queue.task_done()

# 워커 스레드 실행
threading.Thread(target=influx_worker, daemon=True).start()

# MQTT 클라이언트 설정 및 시작
mqtt_client = Client()
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
mqtt_client.subscribe(MQTT_TOPIC)

print("MQTT 수신 대기 중... Ctrl+C로 종료")
mqtt_client.loop_forever()
