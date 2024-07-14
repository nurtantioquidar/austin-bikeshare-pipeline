#!/bin/bash

# Initialize the database
airflow db init

# Create the admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start the webserver
airflow webserver &

# Start the scheduler in the foreground (blocking)
exec airflow scheduler