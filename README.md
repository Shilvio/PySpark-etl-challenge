# Quick start

1. **Congiguration**
	rename the `example.env` file to `.env`

2. **Build & Run**
	 build the container with
	`docker-compose up -d --build`
	 or with `docker-compose up --build` to check the building logs

3. **Monitoring** 
	you can check the log of a specific container with `docker logs -f <container name>` 

4. **Stop & Cleanup**
	use `docker-compose down`  to stop the services maintaining the data 
	or use `docker-compose down -v` to cleanup the volumes

# Introduction

Due to the complexity of the challenge and the time limitations I decided to focus on **Structural Clarity** and **Architectural Robustness**. Since this was a relatively new **topic** for me, I had to prioritize searching and analyzing various documentations and architectural patterns, to create a simple, but fully working project.

# Project architecture and structure

Following my research I decided to use this structure for my project following the **Producer-Consumer** pattern:
```.
├── Dockerfile
├── Dockerfile.spark
├── README.md
├── docker-compose.yml
├── requirements.txt
└── src
    ├── __init__.py
    ├── config.py
    ├── cdc
    │   └── poller.py
    ├── database
    │   ├── data_generator.py
    │   └── init.sql
    └── etl
        ├── __init__.py
        ├── main.py
        ├── schemas.py
        ├── settings.py
        └── transformations.py
```

This structure emphasize **Modularity**, which is essential for improving code **Readability** and permitting a seamless project **Scalability**.

Studying the requirements for building an effective pipeline I needed to define this components:

**Database Source**:
- A database structure (Postgres).

 **Data Generator**:
- A custom Python **user traffic simulation** to insert data to the database within random time intervals.

**CDC**:
- A custom Python module to identify, collect and manage data coming from the source database.

**Message Broker**: 
- Using **Kafka** as a persistent buffer enabling data decoupling

**ETL**:
- Using Apache Spark to automatically handle data coming from the **message broker**, applying transformations (**business logic**) and loading them into a **Data Warehouse Sink**.

**Data Warehouse Sink / Database di destinazione**:
- Another Postgres instance for receiving the data managed by the **ETL** process and ready for analysis.

**N.B.**:
- The `config.py` file is located directly in the source folder of the project, this respect the **the flatter the better** rule of python, but can be nested and reorganized following the project scalability needs, to better handle the config files.

# Architecture diagram

![Untitled diagram-2026-01-29-160821](https://github.com/user-attachments/assets/d5e154b7-e502-4bb2-82a2-00727df60fcd)

# Database

For the database structures I choose to use **Postgres**, being highly **reliable** and easy to deploy.
Postgres is more than capable to handle the amount of data required for this challenge.

Scaling the project might result in a database overload, in a professional scenario I would suggest to use a **hybrid database solution**, both with a relational and non relational database reducing the load to the single database instance.

# Data generator

I used Faker, a python library capable of generating and manipulating data in a database, effectively simulating user traffic for testing out the following **CDC** and **ETL** logic.
# CDC

While researching CDC'S I evaluated various strategies:

- **Log-based CDC:** It has minimal impact on latency for managing streaming data, captures and manages every change on the database by reading the transaction logs, is minimally invasive, often a company standard praised for its efficiency, but difficult to implement quickly as it also requires specific configurations at the database level.

- **Trigger-based CDC:** It uses database triggers to handle data changes and writes them to a separate audit table, which is also difficult to implement quickly and significantly slows down database write and modification operations, requiring double writes.

- **Query-based/Snapshot CDC**: It performs periodic queries to databases based on time intervals often on columns containing "modified_at", "updated_at" timestamps, incremental ids, very easy to implement and does not require access to internal logs, reliable and with medium latency, but does not handle DELETE cases, if a column is deleted it overloads the DB with continuous polling.

- **Application Level CDC**: The application itself, when writing to the DB, sends an event to the pipeline. This is easy to implement at a basic level, but would require the development of an event system and a separate application. Furthermore, there is a risk of generating inconsistencies at the data level in the event of errors (message not sent, insertion failed). To overcome this, complex patterns are required.
  
To reduce structural complexity and meet delivery deadlines, I decided to implement a **query-based** system with timestamp-based polling. Once the data is retrieved, it will pass it to **Kafka** for queuing and message handling.

With more development time I could have created a log-based system with **Debezium** to improve the scalability of the project by reducing the load at the database level and handle **DELETE** cases as previously explained.

In fact, one of the bottlenecks when scaling the project to 10x is the polling system itself, which would exponentially increase the latency of operations.

# Queueing

 used **Kafka** as a buffering layer, promoting **decoupling** between the source database and the warehouse sink. It also provides data protection (**similar to caching**) in the event of a destination database failure. Furthermore, if the ETL layer slows down, Kafka buffers messages without slowing down the source database (**backpressure**).

Kafka also requires an **orchestrator** service, **ZooKeeper**, which tracks and manages the nodes created by Kafka.
# ETL

I used **Spark** as requested by the assignment, it is a java wrapped python library capable of managing Structured Streaming, the ETL script takes data from Kafka and processes it by applying a business logic function for adding taxes to the product.

The various components of **ETL** are **modular**, ensuring easy code maintenance as the project scales up.

Spark also allowed me to create the table and insert the data into the target database using a **Code First** approach.

# Dockerization

Various docker containers have been created:

- postgres_source 
- postgres_dest 
- zookeeper & kafka 
- data_generator 
- cdc_poller 
- spark-etl (custom build to fix permissions and dependencies)

Containerization makes code accessible and easy to deploy and run, increasing maintainability.
