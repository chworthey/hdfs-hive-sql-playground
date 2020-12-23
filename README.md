# HDFS & HIVE & SQL Playground

This project uses Docker to spin up a virtual cluster of nodes which simulate an enterprise big data ELT infrastructure on a single Windows machine. This will enable the user to play around with Hadoop, Hive, and SQL Servers to facilitate learning of those technologies. This could also be slightly modfied to serve the purposes of any infrastructure-specific testing in a CI/CD workflow.

## First thing is first...


### Install Docker

Please install [Docker for Windows](https://docs.docker.com/docker-for-windows/install/)

### Download JDK

The Apache projects all run with Java. Each virtual node needs a version of the licensed Oracle JDK8 deployed to it (this follows the recommendation of the Hadoop documentation and we *are* pretending we are an enterprise solution after all).

Please download and extract the [Linux x64 Compressed Archive version 8u271](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to ./bin/jdk1.8.0_271. This will require you to sign up with Oracle and accept the license terms, but it is free for non-business purposes.

## Set up your playground project

Please create some folders wherever you please:

```
mkdir src && mkdir data
```

### ./src/...

In your src, add any hive query files (.hql) or sql (.sql) query files you may wish to execute in your playground at a later time.

### ./data/...

Place any data files (in subdirectories too if you wish) that should get ingested into HDFS during setup. This directory will be mounted as a readonly volume to one of the nodes. (Note: by ingesting into HDFS, you acknowledge that a complete copy of these files will exist within HDFS PERSISTANTLY, so don't go filling up your disk space with this!)

## Initialize Nodes for first time

The setup command only needs to be run ONCE per project.

Make sure Docker for Windows is running ([with Linux containers](https://docs.docker.com/docker-for-windows/#switch-between-windows-and-linux-containers)).

Run the setup process with:
```
python playground.py setup
```

The python script will interactively prompt you for your project name and your src + data folder locations and set up the `./config.json` file respectively if it hasn't already been set up.

Then, it will attempt to provision the cluster (format hdfs, ingest data, etc).

## Boot Cluster for Playground Use

When you want to play around with the cluster, run:
```
python playground.py start
```
And when you want to tear down the cluster:
```
python playground.py stop
```

## Playing in the Playground

Your `src` directory gets copied into (TODO: Finish this section)

## Destroying the Volumes

If you want to start fresh (delete all the volumes), go ahead and run:

```
python playground.py destroy-vol
```
And then if you want to reprovision the cluster for more use, run:
```
python playground.py setup
```
