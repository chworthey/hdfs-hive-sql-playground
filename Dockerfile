FROM centos:latest

ADD ./bin /himage

ENV JAVA_HOME=/himage/jdk1.8.0_271
ENV HADOOP_HOME=/himage/hadoop-3.3.0
ENV HIVE_HOME=/himage/apache-hive-3.1.2-bin
ENV SQOOP_HOME=/himage/sqoop-1.4.7.bin__hadoop-2.6.0
ENV ACCEPT_EULA=Y
ENV SQOOP_SERVER_EXTRA_LIB=/himage/sqoop-extras/
ENV HCAT_HOME=/himage/apache-hive-3.1.2-bin/hcatalog

RUN chmod +x /himage/install.sh && /himage/install.sh