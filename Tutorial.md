# Docker-Spark-mongoDB-Tutorial
The purpose of this tutorial is to provide a step-by-step guide on setting up and using a Spark cluster that runs inside Docker containers on different machines.
Below is a list of the steps we will be following, as indicated in the table of contents.

## Table of Contents
- [Introduction](#introduction)
  - [Project and Data](#ProjectandData)
  - [Requirement](#Requirement)
- [Create Docker images](#create-docker-images)
  - [Pulling images from repository](#Pulling-images-from-repository)
  - [Building images](#Building-images)
- [Docker networking](#docker-networking)
- [Apache Spark](#Apache-Spark)
- [MongoDB](#Mongodb)
- [Assembling](#assembling)

### Introduction
<h2 id="ProjectandData">Project and Data</h2>
<p>The purpose of this project is to create a scalable Spark cluster that can handle big data. The data used in the project is sourced from Reddit comments which can be obtained from the following link: http://files.pushshift.io/reddit/comments/. The data is compressed in zstd format on a monthly basis, resulting in significant compression. A compressed file of 20GB size can expand to 250-300GB upon decompression, posing significant challenges in managing such huge data. This project primarily focuses on analyzing the body (i.e., comment text), score, and author of the comments.</p>

<h2 id="Requirement">Requirement</h2>
<p>The cluster will be built across different hosts, with each container node residing on a single host. As the cluster comprises 6 nodes, 6 VMs should be available, each with Docker already installed. For MongoDB and workers, VMs with 4GB RAM or more are recommended. For MongoDB, it is necessary to have a volume of at least 300GB. Additionally, it is advisable for the master host to have a volume of 40-50 GB to prevent data overload during data loading.</p>

### Create Docker images
<h2 id="Pulling-images-from-repository">Pulling images from repository</h2>
<p>In each host you need to create it's own designated image. You can pull the pre-built images from my repository. Simply run the following commands based on which image you want to create</p>

```docker
docker pull minabadri/spark-cluster-master:latest
docker pull minabadri/spark-cluster-worker:latest
docker pull minabadri/spark-cluster-jupyter:latest
```
<h2 id="Building-images">Building images</h2>
<p>There is also a way to build the Docker images from scratch, for that clone this repo and run following commands</p>

```docker
docker build -f Dockerfile_base -t {YOUR_TAG} .
docker build -f Dockerfile_master -t {YOUR_TAG} .
docker build -f Dockerfile_wroker -t {YOUR_TAG} .
docker build -f Dockerfile_jupyter -t {YOUR_TAG} .
```
For mongoDB container we use the official image of mongoDB which will be explained in next steps.

### Docker networking

<p>To establish connectivity between containers, we must enclose them within a user-defined network known as 'overlay.' To achieve this, we must first create a Docker swarm, which links the daemons together. Then, we can add all containers to the overlay network. In our setup, the master node serves as a manager. Now set up a Docker daemon to be the swarm manager by running the following command on master's host</p>

```docker
docker swarm init
```
<p>The outcome of the command is like following command:</p>

```docker
docker swarm join --token <YOUR_TOKEN> <YOUR_PRIVATE_IP>:2377
```
<p>Run it on all remaining hosts. Now your docker daemon is ready to use</p>
<p>It's time to create the user-defined overlay network which should be done on master's host.</p>

```docker 
docker network create --driver overlay --attachable spark-project-net 
```
<p>You can choose any name you wish for the network. In the master node the network can be seen and inspected but not in any other hosts. the following commands are for this purpose</p>

```docker
docker network ls
docker network inspect spark-project-net
```
### Apache Spark
### MongoDB
### Assembling
