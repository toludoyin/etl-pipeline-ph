### Workflow

Extract data from https://date.nager.at/Api API, transform the data in a readable format and load into a Postgresql database for accessibility, scheduled the flow to run and update table at 09:00, Sunday through Saturday using Airflow.

### Environment Setup
1. Install Docker from https://docs.docker.com/compose/install/
2. Setup Postgres, PgAdmin and Airflow credentials

### Initialize Database

$ docker compose up airflow-init


### Run Airflow
```
$ docker compose up
```

<img width="718" alt="Screenshot 2024-02-22 at 17 09 09" src="https://github.com/toludoyin/public-holiday-pipeline/assets/76572085/dc710a18-82d9-4629-8201-90cf7b356c30">
