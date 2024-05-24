import time, cv2
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["172.26.41.149:9092"]
)

if __name__=="__main__":
    cam = cv2.VideoCapture(0)
    
    while True:
        ret, frame = cam.read()
        frame = cv2.resize(frame, (600, 480), \
               interpolation = cv2.INTER_LINEAR)
        ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        
        # Serialize the frame to bytes
        producer.send("test",buffer.tobytes())
        time.sleep(0.05)