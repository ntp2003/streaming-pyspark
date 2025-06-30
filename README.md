# streaming-pyspark

## Overview

The main idea of this project is to build a **Streaming Image DataSource** using **Apache Kafka** and process it in real time with **PySpark**. The processed results are then sent back to Kafka under a different topic.

Kafka is chosen as the central data storage and message broker because it is a powerful and flexible platform for streaming data processing. Its advantages include high scalability, low latency, reliability, ease of use, and rich integration capabilities. Moreover, since both Kafka and Spark are popular Apache projects, they offer excellent compatibility and integration for building reliable real-time systems.

![System Architecture](https://github.com/ntp2003/streaming-pyspark/assets/95475230/3e4d8665-f919-419b-a51e-f91760072249)

## Pipeline Description

1. **Image Stream Source**: Images captured from a camera are pushed into Kafka under **Topic A**.
2. **Spark Streaming Job**: PySpark consumes messages from **Topic A**, processes the image data, and publishes results to another Kafka topic.

### Image Processing Flow

![Spark Processing Flow](https://github.com/ntp2003/streaming-pyspark/assets/95475230/a28daec8-a2a8-4ff4-bdc5-3081b9c866aa)

* The structured streaming data from Kafka looks like this:

  ![Kafka Raw Data Format](https://github.com/ntp2003/streaming-pyspark/assets/95475230/cf586709-1b66-464a-a8a0-6e10f660e8fe)

* The `value` field contains the image in binary format. We decode this into a 3D array in RGB format.

* Next, we perform **face detection** and store the bounding box information for detected faces.

* For **face recognition**, we encode the detected faces using **128-dimensional face encodings**.

* Using a predefined identifier (see methods below), we attempt to **recognize and label** the faces. If a face is not recognized, it is labeled as `"Unknown"`.

* Finally, the labeled faces are drawn on the image, the image is re-encoded to binary, and sent to a Kafka output topic.

---

## Face Recognition Methods

We support **two recognition approaches**:

### 1. Distance-Based Matching (Euclidean Distance)

* Prepare a folder containing labeled face images (one image per person).
* Spark reads these reference images and assigns labels based on file names.
* Each reference image is encoded using 128-dimensional encodings.
* During streaming, the encoded face from the stream is compared with each reference encoding using **Euclidean distance**.
* If the distance is below a specified threshold, the face is labeled with the corresponding identity.
* **Lower thresholds increase accuracy** but may reduce recognition coverage.

### 2. Classifier-Based Recognition (Logistic Regression)

* Collect 100–200 face images for each person to be recognized.
* Encode all images using 128-dimensional encodings.
* Train a classification model (e.g., **Logistic Regression**) using these encodings and labels.
* During streaming, Spark applies the trained model to predict the identity of the encoded face.
* If the prediction confidence exceeds a defined threshold, the face is labeled with the predicted name.
* **Higher thresholds result in more accurate predictions**.

---

## Conclusion

This project showcases a real-time image processing pipeline that leverages Kafka and PySpark to detect and recognize faces in a streaming environment. By combining structured streaming, deep learning-based face recognition, and distributed processing, the system can handle large-scale, real-time image analysis efficiently.
