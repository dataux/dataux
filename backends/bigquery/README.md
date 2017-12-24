
Google BigQuery Data source
--------------------------------------

Provides MySQL acess to [Google BigQuery](https://cloud.google.com/bigquery/)
which opens up the usage of bigquery via standard sql allowing tools that don't have native
bigquery clients.


![dataux_bigquery](https://cloud.githubusercontent.com/assets/7269/26564686/1d82180e-4499-11e7-90f5-57ee7f87310a.png)


```sh
# assuming you are running local, if you are instead in Google Cloud, or Google Container Engine
# you don't need the credentials or volume mount
docker run -e "GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json" \
  -e "LOGGING=debug" \
  --rm -it \
  -p 4000:4000 \
  -v ~/.config/gcloud:/.config/gcloud \
  gcr.io/dataux-io/dataux:latest
```

```sql

# connect to dataux
mysql -h 127.0.0.1 -P4000


-- Create a new schema = "bq" with one source
-- a bigquery public dataset is the only source/tables
-- replace BIGQUERY_PROJECT with your billing account project

CREATE source `BIGQUERY_PROJECT` WITH {
    "type":"bigquery",
    "schema":"bq",
    "table_aliases" : {
       "bikeshare_stations" : "bigquery-public-data.san_francisco.bikeshare_stations"
    },
    "settings" {
      "billing_project" : "BIGQUERY_PROJECT",
      "data_project" : "bigquery-public-data",
      "dataset" : "san_francisco"
    }
};

-- WITH Properties:
-- "schema":  Name of schema to attach this source to
-- "type":  Source type, most be datasource registered in registry (mongo, bigtable, etc)

CREATE source BIGQUERY_PROJECT WITH json_properties


use bq;

show tables;

select title, release_year AS year, locations from film_locations limit 10;

select count(*) as ct, landmark from bikeshare_stations GROUP BY landmark;

SELECT landmark from bikeshare_stations WHERE landmark like "Palo%"

select count(*) AS ct, landmark FROM bikeshare_stations GROUP BY landmark ORDER BY ct DESC LIMIT 1;


# Drop it when your are done if you want

drop schema bq;

```


