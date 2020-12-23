FROM centos:latest

ADD ./bin /himage

ENV JAVA_HOME=/himage/jdk1.8.0_271
ENV HADOOP_HOME=/himage/hadoop-3.3.0
ENV HIVE_HOME=/himage/apache-hive-3.1.2-bin
ENV SQOOP_HOME=/himage/sqoop-1.99.7-bin-hadoop200
ENV ACCEPT_EULA=Y

RUN chmod +x /himage/install.sh && /himage/install.sh