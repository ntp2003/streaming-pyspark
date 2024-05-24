from kafka import KafkaConsumer
from PIL import Image
from io import BytesIO
import cv2
import numpy as np

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=['172.26.41.149:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

# Subscribe to a specific topic
consumer.subscribe(topics=['image1'])

for message in consumer:
    data = message.value
    stream = BytesIO(data)
    image = Image.open(stream).convert("RGB")
    
    cv2.imshow("live", np.asarray(image))
    cv2.waitKey(90)
    
cv2.destroyAllWindows()
consumer.close()
    