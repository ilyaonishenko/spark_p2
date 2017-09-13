# Spark part 2

So there are 3 applications now inside this repo.
* ```ScalaSniffer ``` -- local application for sniffering tcp packets from the machine
* ```PacketAnalyzer``` -- remote spark application. receives case class which contains information
about packets and make some operationf on it. I mean save reduced information in hive table and sends kafka alerts if
 threshold is bitted
* ```KafkaCons ``` -- local application for consuming alerts sended by ```PacketAnalyzer``` using kafka

Pointer to the library: ```-Djava.library.path=/home/ilia/native/jpcap-0.01.16/lib```
The device for listening: ```wlp2s0```

Also make sure you are using java 32 jdk

To get access to devices we need sudo run program. I use command
```sh
sudo bin/java -cp ~/IdeaProjects/spark-p2/target/scala-2.11/spark-p2-assembly-0.1.jar -Djava.library.path=/home/ilia/native/jpcap-0.01.16/lib com.example.local.ScalaSniffer wlp2s0
```
As I use virtual machine, I need to use correct IP address on PacketAnalyzer.scala: ```10.0.2.2```
Spark-submit command to run PacketAnalyzer on spark:
```sh
spark-submit --class com.example.remote.PacketAnalyzer --master local[*] --deploy-mode client spark-p2-assembly-0.1.jar
```
Very important to use parameter ```--master local[*]``` because if you do not use it there will single-thread application
and you will not use features of spark streaming.
