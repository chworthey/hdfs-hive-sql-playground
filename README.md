# HDFS & HIVE & SQL Playground

Copyright 2021 Patrick S. Worthey

Think of this like a homemade Apache Ambari for testing purposes.

This project uses Docker to spin up a virtual cluster of nodes which simulate an enterprise big data ELT infrastructure on a single host. This will enable the user to play around with Hadoop, Hive, and SQL Servers to facilitate learning of those technologies. This repository could also be used to serve the purposes of any infrastructure-specific testing in a CI/CD workflow (for example performance benchmarking analytics in a cluster that you administrate, but would need more attention to the hardware-aspect than is currently present in this repository).

This repository does:
- Faciliate learning of Hadoop HDFS, Hive, SQL, etc.
- Facilitate testing of self-administered big data clusters

This repository does NOT:
- Implement any sort of security or compliance standards
- Integrate with 3p cloud solutions

This repository has only been tested with Windows 10.

## Architecture

The data starts as files on your local machine and undergoes the following processes:

Local file => HDFS ingestion => Hive transformations => SQL Server

This project achieves this by hosting a cluster of nodes with Docker with the needed components installed. The nodes are as follows:

Node Name | AKA | Description
--- | --- | ---
nn1 | Name Node 1 | The master node for the HDFS file system. (secondary name node / high availability is not implemented currently)
dn1 | Data Node 1 | The slave node for the HDFS file system. (currently it's the only one but there could be many)
rman | Resource Manager | The master node for YARN jobs. Manages cluster-wide resources for distributed work.
nm1 | Node Manager 1 | Slave node for YARN jobs. Hosts application containers. There could be any number of node managers although right now there is just one. Furthermore, the node manager could be on the data nodes so the data does not need to travel but it doesn't really matter for this example.
mrhist | Map Reduce History Server | Persists the map reduce job history.
hs | Hive Server | Enables hive execution.
client | Client Node | Represents a workspace that you work from using various cli's to interact with the cluster (like beeline for example).
sql | SQL Server | A Microsoft SQL Server instance running on its own node.

To help orchestrate these nodes, `playground.py` comes to the rescue. It provides a commandline interface to configure, run, and interact with the node cluster.

## Install Docker

Please install [Docker](https://docs.docker.com/get-docker/)

## Download JDK

The Apache projects all run with Java. Each virtual node needs a version of the licensed Oracle JDK8 deployed to it (this follows the recommendation of the Hadoop documentation and we *are* pretending we are an enterprise solution after all).

Please download and extract the [Linux x64 Compressed Archive version 8u271](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to `./bin/jdk1.8.0_271`. *Even if your main host is Windows, please get the Linux distrubtion because the nodes are linux distributions*. This process will require you to sign up with Oracle and accept the license terms, but it is free for personal use.

NOTE: I don't currently extract the distributions using the Docker image build in general because I've made some modifications to the hive distribution. See `./NOTICE.txt` for more details on those modifications. I also currently place the configuration files in the distributions largely because I haven't needed to modify them very often in my projects so far.

If on Windows, one option is to use [7-Zip](https://www.7-zip.org/) to complete the task of extracting.

If you want to check the binary into a **PRIVATE** repostitory, modify `./gitignore` and remove the lines:

```sh
# Cannot check this in publically due to licensing (sorry!)
/bin/jdk1.8.0_271
```

I also recommend that you install [git lfs](https://git-lfs.github.com/) and run prior to checking in the JDK to possibly save some disk space in the repository down the road:

```sh
git lfs install
git lfs track "bin/jdk1.8.0_271/**"
git add .gitattributes
```

## Python Setup

This project uses [pipenv](https://pypi.org/project/pipenv/) (optional)

Alternatively, you can simply use [Python 3.9](https://www.python.org/downloads/release/python-390/). There are no PyPi packages needed other than `requests`.

## Run Examples

Make sure Docker for Windows is running ([with Linux containers](https://docs.docker.com/docker-for-windows/#switch-between-windows-and-linux-containers)).

The `./examples` directory is the best place to start. Check the contents and then run:

```sh
pipenv shell
cd ./examples
python runall.py
```
Alternatively, you can use the `runall.ps1` script which is basically identical to `runall.py` in functionality.

The 'runall' scripts will do the following:
- Build the docker image (if first time)
- Set up the cluster and ingest data from local files into HDFS
- Start the cluster and wait for the nodes to reach full health
- Execute a series of hive and sql scripts/inline queries
- Sqoop export from HDFS to SQL
- Spin down the cluster

An example output of all the following can be found in `./examples/example-output.log`.

If you want to play around with the cluster as it runs, the `config.json` file has been provided for convenience. You only need to `cd` into the examples directory to use it and then execute commands. For example:

```sh
cd ./examples
python ../playground.py start
python ../playground.py bash-cli -n client
```

## About the Example data

The data found in `./examples/data` comes from the WSU astrophysics department (from my Dad) and contains the first couple entries for chemically peculiar (cp) and non-chemically peculiar (no-cp) stars. Hidden in the data might be a way to measure the age of stars based on the difference between cp and no-cp. I'll attempt to explain this better at a later time...

## Incorporating into other projects

Please create some folders wherever you please:

```sh
mkdir src && mkdir data
```

### src/...

In your src, add any hive query files (.hql) or sql (.sql) query files you may wish to execute in your playground at a later time.

### data/...

Place any data files (in subdirectories too if you wish) that should get ingested into HDFS during setup. This directory will be mounted as a readonly volume to one of the nodes. (Note: by ingesting into HDFS, you acknowledge that a complete copy of these files will exist within HDFS PERSISTANTLY, so don't go filling up your disk space with this!)

Any run of playground.py outside of `./examples` and without configuration variables will prompt you to interactively input the configuration variables where you should place the src and data paths when prompted.

## Configuration Variables

Configuration can take the form of command-line arguments, config files, or direct Python manipulation. The project variables are as follows:
- project_name: the base name of your docker cluster group for example `example`
- source_dir: the relative or absolute path to the directory containing the hive/sql/etc. scripts you want copied to the cluster (on the client node) during the setup phase
- data_dir: the relative or absolute path to the directory containing data files you want ingested into HDFS during the setup phase
- volumes_dir: the relative or absolute path to a directory that may or may not exist which will contain persisted data from the cluster such that the data will remain even after the cluster has been torn down

## Normal Project Lifecycle

This section details the order in which you normally execute the playground commands.

### Setup
The setup command generally only needs to be run ONCE per project, and it will destroy persistant volumes you have, if any.

Make sure Docker for Windows is running ([with Linux containers](https://docs.docker.com/docker-for-windows/#switch-between-windows-and-linux-containers)).

Run the setup process with:
```
python playground.py setup
```

The python script will interactively prompt you for your project name and your src + data folder locations and set up the `./config.json` file respectively if it hasn't already been set up.

Then, it will attempt to provision the cluster (format hdfs, ingest data, etc).

### Boot Cluster for Playground Use

When you want to play around with the cluster, run:
```
python playground.py start
```
And when you want to tear down the cluster:
```
python playground.py stop
```

### Destroying the Volumes

If you want to start fresh (delete all the volumes), go ahead and run:

```
python playground.py destroy-vol
```
And then if you want to reprovision the cluster for more use, run:
```
python playground.py setup
```

## Things I wish I had more time to address

### Tez

The execution engine for this project is the old map reduce system, but tez would the more modern approach.

### More nodes

Currently I only have one of each, but it would be interesting to compare performance in several distributed scenarios.

### Hive SQL-based Metastore

Currently I only use Derby for the metastore database, but it would be better to use some sql-based server.

### Sqoop --hcatalog

In order to use sqoop right now, the files have to be delimited text. I meant to use the --hcatalog argument to handle the ORC deserialization for the SQL tables and easier Hive integration, but it seems there's an issue with the derby metastore, so I haven't gotten around to it.