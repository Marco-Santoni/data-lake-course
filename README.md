# Setup

```bash
docker-compose up
```

# Minimal runtime

The runtime provided by the docker compose file is far from a large scale production-grade warehouse, but it does let you demo Iceberg’s wide range of features. Let’s quickly cover this minimal runtime.

- Spark 3.1.2 in local mode (the Engine)
- A JDBC catalog backed by a Postgres container (the Catalog)
- A docker-compose setup to wire everything together
- A %%sql magic command to easily run SQL in a notebook cell

# Demo

The container is now running. We can launch SparkSQL commands via

```bash
docker exec -it spark-iceberg spark-sql
```

The interactive shell is now there.

```sql
CREATE TABLE demo.nyc.taxis
(
  vendor_id bigint,
  trip_id bigint,
  trip_distance float,
  fare_amount double,
  store_and_fwd_flag string
)
PARTITIONED BY (vendor_id);


SELECT * FROM demo.nyc.taxis;

INSERT INTO demo.nyc.taxis
VALUES (1, 1000371, 1.8, 15.32, 'N'), (2, 1000372, 2.5, 22.15, 'N'), (2, 1000373, 0.9, 9.01, 'N'), (1, 1000374, 8.4, 42.13, 'Y');

SELECT * FROM demo.nyc.taxis;   
```

Let's look at the data behind these tables by opening an interactive bash shell in the container.

```bash
docker exec -it minio /bin/bash

cd data
```

We can also launch an Iceberg-Enabled Spark notebook server.

```bash
docker exec -it spark-iceberg pyspark-notebook
```