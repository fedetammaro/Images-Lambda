
# Images-Lambda
## Overview
![Overview of the entire architecture](https://i.imgur.com/aOXSKAU.png)
Final assignment for the Parallel Computing course at Università degli Studi di Firenze, developed together with [Federico Palai](http://github.com/palai103).

This Lambda Architecture retrieves images from Google and Bing, based on a given keyword, and processes them in order to show the user the most relevant images for the given query. Image features are extracted using the CEDD descriptor.

A more complete analysis of the project can be found in the [paper](https://github.com/Sfullez/Images-Lambda/blob/master/PC_2018_19_Final_Project_Report.pdf).
## Usage
In order to use this architecture, Hadoop must be started and a new namenode must be created. Example, from the Hadoop installation directory:

    bin/hdfs namenode -format
	sbin/start-all.sh

Then, the Spring Boot web application and the Cassandra database must be started: as soon as a query is sent by a client from the front-end, batch and speed layer will be started automatically by the driver. Example:

    sudo service cassandra start
    jar springboot-artifact-name.jar

## Technologies used
 - Java 8
 - [LIRE framework](http://lire-project.net)
 - [Apache Hadoop 3.1.2](https://hadoop.apache.org/)
 - [Apache Storm 2.0.0](https://storm.apache.org/)
 - [Apache Cassandra 3.11.4](https://cassandra.apache.org/)
 - [Datastax Java Driver for Cassandra 3.7.1](https://github.com/datastax/java-driver)
 - [Spring Boot 2.1.6](https://spring.io/projects/spring-boot)
 - Python 3
 - HTML5 and Javascript
