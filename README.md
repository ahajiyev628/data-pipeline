# Setting up Airflow, Minio, Spark

## Pre-installation Steps for Airflow (including database setup and volumes creation)

```bash
docker-compose up -d --build airflow-init
```

This command initializes the Airflow environment, including database setup and volume creation.

### Troubleshooting: "ValueError: Unable to configure handler 'processor'" in Airflow

If you encounter an error in the Airflow service related to configuring the 'processor' handler, follow these steps:

```bash
cd airflow/
sudo chown 50000:0 dags logs plugins
cd ..
docker-compose up -d
```
- Wait till web-server is healthy.
