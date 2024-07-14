# Austin Bikeshare Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for the Austin Bikeshare dataset using Apache Airflow, Google Cloud Platform (GCP), and Docker.

## Prerequisites

- Docker and Docker Compose
- Google Cloud Platform account with BigQuery and Cloud Storage enabled
- Google Cloud SDK (gcloud CLI)

## Project Structure
```
austin-bikeshare-pipeline/
│
├── dags/
│   ├── bikeshare_etl.py
│   └── scripts/
│       ├── extract.py
│       ├── manage_biglake_table.py
├── logs/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

## Setup

1. Clone the repository:
```
git clone https://github.com/your-username/austin-bikeshare-pipeline.git
cd austin-bikeshare-pipeline
```
2. Set up your Google Cloud credentials:
- Create a service account in your GCP project with necessary permissions for BigQuery and Cloud Storage.
- Download the JSON key file for this service account.
- Place the JSON key file in the project root and name it `google-credentials.json`.

3. Create a `.env` file in the project root with the following content (replace with your actual values):
```
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com
GCP_PROJECT_ID=your-GCP_PROJECT_ID
GCP_GCS_BUCKET=your-GCP_GCS_BUCKET
GCP_DATASET_ID=temp_dataset
GCP_TABLE_ID=bikeshare_trips
GCP_GCS_PATH=bikeshare
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/google-credentials.json
```
4. Build the Docker images and start the services:
```
docker-compose build
docker-compose up -d
```
5. Access the Airflow web interface at `http://localhost:8080`. Use the admin username and password specified in your `.env` file.

## Running the Pipeline

1. In the Airflow UI, enable the `bikeshare_etl_pipeline` DAG.
2. The DAG is scheduled to run daily, but you can also trigger it manually from the UI.

## Pipeline Steps

1. Extract data from the BigQuery public dataset `bigquery-public-data.austin_bikeshare.bikeshare_trips`.
    - By default we decrease `today - 200 day` to ensure we get enough data since latest data is on Jan 2024
2. Store the extracted data in Google Cloud Storage.
3. Create or update a BigLake table referencing the data in GCS.
4. Perform data analysis on the BigLake table.

## Data Analysis Task
### Task 5: Data Analysis
Write SQL queries to answer the following questions using the `new BigLake table`:
1. **Find the total number of trips for each day.**
```
SELECT
  DATE(start_time) AS `date`,
  COUNT(DISTINCT trip_id) AS daily_total_count_trip,
FROM
  `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
GROUP BY
  1
```
2. **Calculate the average trip duration for each day.**
```
SELECT
  DATE(start_time) AS `date`,
  AVG(duration_minutes) AS daily_average_duration_minutes,
FROM
  `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
GROUP BY
  1
```
3. **Identify the top 5 stations with the highest number of trip starts.**
```
SELECT
  start_station_id,
  COUNT(1)
FROM
  `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  5
```
4. **Find the average number of trips per hour of the day.**
```
WITH
  base AS (
  SELECT
    DATE(start_time) AS `date`,
    EXTRACT(hour
    FROM
      start_time) AS hour,
    COUNT(1) AS hourly_total_count_trip
  FROM
    `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
  GROUP BY
    1,
    2)
SELECT
  `date`,
  AVG(hourly_total_count_trip) AS daily_hourly_average_trip
FROM
  base
GROUP BY
  1
```
5. **Determine the most common trip route (start station to end station).**
```
WITH
  base AS (
  SELECT
    start_station_id,
    end_station_id,
    COUNT(1) AS total_trip_route_count
  FROM
    `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
  GROUP BY
    1,
    2
  ORDER BY
    3 DESC)
SELECT
  *
FROM
  base
QUALIFY
  ROW_NUMBER() OVER(ORDER BY total_trip_route_count DESC) = 1
```
6. **Calculate the number of trips each month.**
```
SELECT
  DATE_TRUNC(DATE(start_time), month) AS month_id,
  COUNT(DISTINCT trip_id) AS monthly_total_count_trip,
FROM
  `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
GROUP BY
  1
```
7. **Find the station with the longest average trip duration.**
```
WITH
  base AS (
  SELECT
    end_station_id,
    AVG(duration_minutes) AS average_duration_minutes
  FROM
    `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
  GROUP BY
    1 )
SELECT
  *
FROM
  base
QUALIFY
  ROW_NUMBER() OVER(ORDER BY average_duration_minutes DESC) = 1
```
8. **Find the busiest hour of the day (most trips started).**
```
WITH
  base AS (
  SELECT
    DATE(start_time) AS `date`,
    TIMESTAMP_TRUNC(start_time, hour),
    COUNT(DISTINCT trip_id) AS hourly_total_trip
  FROM
    `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
  GROUP BY
    1,
    2)
SELECT
  *
FROM
  base
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY `date` ORDER BY hourly_total_trip DESC) = 1
```
9. **Identify the day with the highest number of trips.**
```
WITH
  base AS (
  SELECT
    DATE(start_time) AS `date`,
    COUNT(DISTINCT trip_id) AS daily_total_trip
  FROM
    `{your-gcp-project-id}.temp_dataset.bikeshare_trips`
  GROUP BY
    1 )
SELECT
  *
FROM
  base
QUALIFY
  ROW_NUMBER() OVER(ORDER BY daily_total_trip) = 1
```

## Customization
- Adjust the DAG schedule in `dags/bikeshare_etl.py` to run at different intervals.

## Troubleshooting

- If you encounter permission issues, ensure your Google Cloud service account has the necessary roles and permissions.
- Check the Airflow logs for detailed error messages if a task fails.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.