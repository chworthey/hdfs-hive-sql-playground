# HDFS & HIVE & SQL Playground

This project uses Docker to spin up a virtual cluster of nodes which simulate an enterprise big data ELT infrastructure on a single host. This will enable the user to play around with Hadoop, Hive, and SQL Servers to facilitate learning of those technologies. This repository could also be used to serve the purposes of any infrastructure-specific testing in a CI/CD workflow (for example performance benchmarking analytics in a cluster that you administrate).

This repository does:
- Faciliate learning of Hadoop HDFS, Hive, SQL, etc.
- Facilitate testing of self-administered big data clusters

This repository does NOT:
- Implement any sort of security or compliance standards
- Integrate with 3p cloud solutions like Azure HDInsight or Azure Storage

## Disclaimers

This repository is for testing purposes only and there have been no cluster security measures taken.

This repository has only been tested on Windows 10. However it was built with other host platforms in mind.

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

Alternatively, you can simply use [Python 3.9](https://www.python.org/downloads/release/python-390/). There are no PyPi packages needed.

## Set up your playground project

Please create some folders wherever you please:

```sh
mkdir src && mkdir data
```

### src/...

In your src, add any hive query files (.hql) or sql (.sql) query files you may wish to execute in your playground at a later time.

### data/...

Place any data files (in subdirectories too if you wish) that should get ingested into HDFS during setup. This directory will be mounted as a readonly volume to one of the nodes. (Note: by ingesting into HDFS, you acknowledge that a complete copy of these files will exist within HDFS PERSISTANTLY, so don't go filling up your disk space with this!)

### Examples

Want to see an example of data and src? See: `./examples/tutorial/`

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
