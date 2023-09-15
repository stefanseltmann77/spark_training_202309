README
======

Setup Environment
-----------------
If you want to create a working environment for all the training examples, use the setup.sh script as an
orientation. It runs without interrupt on a EC2 instance.

Python Interpreter
------------------
/usr/local/miniconda/envs/spark_training_spark3/bin/python

Spark Version
-------------
All training examples were developed using Spark 2.4.
They where thoroughly tested with Spark 3.0, without major adjustments and all worked fine.
Therefore this complete code base is regarded as Spark 3.0 now.

Streaming
=========
If you want to test streaming applications, you have to generate streaming data first.


File-Streaming
--------------
The script py56_fake_stream.py generates a CSV based script in a tmp-folder.
You just need to run it in order to get data. But don't forget to stop it after you are done.

Kafka-Streaming
---------------
For the Kafka stream you have to start the stream first.
Best use nohup or separate terminals, because the commands need to be running constantly:

/usr/local/kafka/kafka_2.12-2.5.0/bin/zookeeper-server-start.sh /usr/local/kafka/kafka_2.12-2.5.0/config/zookeeper.properties
/usr/local/kafka/kafka_2.12-2.5.0/bin/kafka-server-start.sh /usr/local/kafka/kafka_2.12-2.5.0/config/server.properties

As soon as the Kafka-Server startet you can trigger the py57_fake_stream_kafka.py in order to generate data.



