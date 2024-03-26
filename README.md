### Workflow

Extract data from https://date.nager.at/Api API, transform the data in a readable format and load into a Postgresql database for accessibility, scheduled the flow to run and update table at 09:00, Sunday through Saturday using Airflow.

### Environment Setup
1. Install Docker from https://docs.docker.com/compose/install/
2. Deploy Airflow on Docker Compose https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
3. Setup PgAdmin, Postgres, and Airflow credentials for logins

### Initialize Database and Start Airflow
```
$ docker compose up airflow-init
```

```
$ docker compose up
```

### PgAdmin Server
1. Register server
2. Setup connection and input credentials.

### Airflow Server
1. Connect to postgres db in airflow. Admin -> Connections

<img width="718" alt="Screenshot 2024-02-22 at 17 09 09" src="https://github.com/toludoyin/public-holiday-pipeline/assets/76572085/dc710a18-82d9-4629-8201-90cf7b356c30">
