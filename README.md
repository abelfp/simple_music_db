# Simple Music Database using PostgreSQL

## Sparkify's Database
The purpose of this database is to allow scientists, business intelligence
engineers, and data analysts to extract insights on the usage of our app. With
it, they will be able to get metrics on usage by song, artists, time spent by
user and many other things. These will help create long-term goals for a more
business focused objective, and all improvements will delight our customers.

## Database schema design and ETL pipelines
I chose the STAR schema for this project because we want to be able to extract
insights in an easy way, with not to many joins. We have a fact table
`songplays`, and four dimension tables. This enables power users to extract
important metrics as well as allowing for some ad-hoc requests.

The ETL pipelines read from the raw data in our `data` directory and outputs
dumps of formatted data to be ingested into Database in Postgres. This is less
prone to failing since COPY statements inserts data in bulk. The ETL can also
adapt very easily to backfills and for scheduling.

## How to run
In order to run the scripts you must have PostgreSQL install in your machine.
Then you can run the scripts as follows:

```sh
python create_tables.py
python etl.py
```

To look at the data in the tables, you can open `notebooks/test.ipynb` to
connect to the database and query the tables.
