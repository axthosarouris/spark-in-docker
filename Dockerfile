FROM ubuntu:18.04


RUN apt-get update && apt-get install -y openjdk-8-jdk wget python3
RUN mkdir /spark

RUN wget -q http://apache.uib.no/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar zxf spark-2.4.0-bin-hadoop2.7.tgz -C /spark/ --strip-components=1

ENV PATH=$PATH:/spark/bin

RUN mkdir conf

COPY conf/sparkConf.conf  conf/sparkConf.conf
COPY spark/conf/hadoop /spark/conf/hadoop
COPY spark/conf/log4j.xml /spark/conf/log4j.xml
COPY spark/conf/spark-env.sh /spark/conf/spark-env.sh

EXPOSE 4040
EXPOSE 4041
EXPOSE 18080
EXPOSE 18081


ENTRYPOINT ["python3", "run.py"]






