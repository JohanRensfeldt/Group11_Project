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
- [Apache Spark](#apache-spark)
- [MongoDB](#Mongodb)
- [Architecture](#architecture)

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
<p>In this section, we will create our Apache Spark cluster without MongoDB. The setup of the MongoDB container will be described in the next section. To begin, we need to create a master node. To do this, run the following command on the host that will act as the master node:</p>

```docker
docker run -dit --name spark-master --network spark-project-net -p 8080:8080 minabadri/spark-cluster-master:latest
```
<p>To ensure that the master has been set up correctly, you can check by navigating to the <PUBLIC_IP_ADDRESS_OF_INSTANCE>:8080</p>
<p>If you have built the image from scratch and changed the tag, you will need to use your own tag. To confirm the name and version of your image, you can run the following command before starting the master container.</p>

```docker
docker images
```
<p>Then run the following command on workers' host. just remember to change the name of container respectively</p>

```docker
docker run -dit --name spark-worker1 --network spark-project-net -p 8081:8081 minabadri/spark-cluster-worker:latest
```
<p>To confirm that the workers have been added successfully, you can access the master web user interface by visiting the URL <PUBLIC_IP_ADDRESS_OF_INSTANCE>:8080</p>
<img src="https://github.com/JohanRensfeldt/Group11_Project/blob/main/images/master-url.png" alt="image">

<p>In this step we are going to create the driver node by runnig it's container:</p>
```docker
docker run -dit --name spark-jupyter --network spark-project-net -p 8888:8888 -p 4040:4040 -p4041:4041 minabadri/spark-cluster-jupyter:latest
```
<p>Let's check if the application is working properly by navigating to the URL <PUBLIC_IP_ADDRESS_OF_DRIVER>:8888. If you are using the image from my Docker repository, the password is 1989. However, if you want to build your own image, you can change the hash for the password to whatever you prefer.</p>
<img src="https://github.com/JohanRensfeldt/Group11_Project/blob/main/images/Jupyter.png" alt="image">
<p>To check if your cluster is functioning properly, you can use the code provided in the example folder named "spark-connection-test.ipynb". This notebook contains code that will verify whether a Spark application is running on the cluster.

Once you run the code, you should be able to see the status of the Spark application in the "master URL". If the cluster is working correctly, the result of the test should be displayed in the notebook.</p>

### MongoDB
In the final step, we will add the MongoDB node to the Docker daemon. To do this, please run the following commands on the host of the MongoDB:
```docker
mkdir ~/mongo/data
docker run -dit -p27017:27017 --name mongoDb -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=1989  --network spark-project-net -v ~/mongo/data:/data/db mongo 
```
We now need to enter the container and prepare our MongoDB instance for data. To enter the container, use the following command:
```docker
docker exec -it mongoDB bash
``` 
```docker
#connect to mongoDB
mongosh -u admin -p 1989 --authenticationDatabase=admin
#inside the mongoDB
use reddit #to create reddit database
db.createCollection("your_collection_name")
exit
```
Now while inside the container, let's import a small dataset inside the database to test the connection.
```docker
curl -s http://files.pushshift.io/reddit/comments/RC_2006-02.zst | zstd --memory=2048MB -d | mongoimport --uri mongodb://admin:1989@77aaa25c0bc3:27017/reddit?authSource=admin --collection smallTest
```
After completing the previous step, you can run the "mongoConnectionTest.ipynb" file located in the examples folder to verify that the connection to the MongoDB server has been established successfully. However, please ensure that you modify the name of the MongoDB container in the code if you have used a container name different from "mongoDb".
### Assembling
What we have set up in the previous sections is the following architecture:
<img src="https://github.com/JohanRensfeldt/Group11_Project/blob/main/images/Swarm%20Network.jpg" alt="image">
