FROM ubuntu:18.04


RUN apt-get update && apt-get install -y openjdk-8-jdk wget python3
RUN mkdir /spark

RUN wget -q http://apache.uib.no/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar zxf spark-2.4.0-bin-hadoop2.7.tgz -C /spark/ --strip-components=1

ENV PATH=$PATH:/spark/bin

RUN mkdir conf

COPY java_driver_options.py java_driver_options.py
COPY java_executor_options.py java_executor_options.py
COPY log4j.py log4j.py
COPY splittuple.py splittuple.py
COPY run.py run.py


EXPOSE 4040
EXPOSE 4041
EXPOSE 18080
EXPOSE 18081


ENTRYPOINT ["python3", "run.py"]






