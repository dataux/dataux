
File Data Source
---------------------------------

Turn files into a SQL Queryable data source.

Allows Cloud storage (Google Storage, S3, etc) files (csv, json, custom-protobuf)
to be queried with traditional sql.  Also allows these files to have custom 
serializations, compressions, encryptions.

**Design Goal**
* *Hackable Stores* easy to add to Google Storage, local files, s3, etc.
* *Hackable File Formats* Protbuf files, WAL files, mysql-bin-log's etc.


**Developing new stores or file formats**

* *FileStore* defines file storage (s3, google-storage, local files, sftp, etc)
  * *StoreReader* defines file storage reader(writer) for finding lists of files, and
    opening files.
* *FileHandler* Defines Registry to create handler for converting a file from `StoreReader`
    into a `FileScanner` that iterates rows of this file. Also extracts info from 
    filepath, ie often folders serve as "columns" or "tables" in the virtual table.
  * *FileScanner* File Row Reading, how to transform contents of
    file into *qlbridge.Message* for use in query engine.
    Currently CSV, Json types.  


**Similar To**

Similar to the [federated BigQuery engine](https://cloud.google.com/bigquery/external-data-sources) 
that can query avro, csv files (or BigTable) or [prestodb](http://prestodb.io/).  
But with focus on hacking in your own sources or file formats, encryption, etc.

Example
----------------------------

The default example dataux docker container
includes a single table `appearances` in database `baseball`
that is made from a single csv file stored in the container.

```sh

docker pull gcr.io/dataux-io/dataux:latest
docker run --rm -e "LOGGING=debug" -p 4000:4000 --name dataux gcr.io/dataux-io/dataux:latest

```

![dataux_file_source](https://cloud.githubusercontent.com/assets/7269/23976158/12a378be-09a3-11e7-971e-8a05d7002aaf.png)

```sql
mysql -h 127.0.0.1 -P4000

use baseball;

show tables;

describe appearances

select count(*) from appearances;

select * from appearances limit 10;

```



Query CSV Files
--------------------
We are going to create a CSV `database` of Baseball data from 
http://seanlahman.com/baseball-archive/statistics/

```sh
# download files to local /tmp
mkdir -p /tmp/baseball
cd /tmp/baseball
curl -Ls http://seanlahman.com/files/database/baseballdatabank-2017.1.zip > bball.zip
unzip bball.zip

mv baseball*/core/*.csv .
rm bball.zip
rm -rf baseballdatabank-*

# run a docker container locally
docker run -e "LOGGING=debug" --rm -it -p 4000:4000 \
  -v /tmp/baseball:/tmp/baseball \
  gcr.io/dataux-io/dataux:latest


```
In another Console open Mysql:
```sql
# connect to the docker container you just started
mysql -h 127.0.0.1 -P4000


-- Now create a new Source
CREATE source baseball WITH {
  "type":"cloudstore", 
  "schema":"baseball", 
  "settings" : {
     "type": "localfs",
     "format": "csv",
     "path": "baseball/",
     "localpath": "/tmp"
  }
};

show databases;

use baseball;

show tables;

describe appearances

select count(*) from appearances;

select * from appearances limit 10;


```
Query CSV Files on Google Cloud Storage
----------------------------------------------

This is similar to above, but will use Google Cloud Storage instead
of local file drives.  If you are running inside of Google Cloud the 
performance off of Cloud Storage is amazing, much, much faster than
you would expect.

```
# create a google-cloud-storage bucket

gsutil mb gs://my-baseball-bucket

# sync files to cloud
gsutil rsync -d -r /tmp/baseball  gs://my-baseball-bucket/


# run a docker container locally, using your local google cloud credentials
docker run -e "GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json" \
  -e "LOGGING=debug" \
  --rm -it \
  -p 4000:4000 \
  -v ~/.config/gcloud:/.config/gcloud \
  gcr.io/dataux-io/dataux:latest

# connect to the docker container you just started
mysql -h 127.0.0.1 -P4000

```

```sql
-- Now create a new Source
CREATE source gcsbball2 WITH {
  "type":"cloudstore", 
  "schema":"gcsbball", 
  "settings" : {
     "type": "gcs",
     "project": "your-google-project",
     "bucket": "my-baseball-bucket",
     "format": "csv"
  }
};

show databases;

use baseball;

show tables;

describe appearances

select count(*) from appearances;

select * from appearances limit 10;

```