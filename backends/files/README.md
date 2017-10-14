
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

Similar to the federated BigQuery engine that can query avro, csv files 
or [prestodb](http://prestodb.io/).  But with focus on hacking in your own sources
or file formats, encryption, etc.

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



Adding A Source
--------------------


```sh
# from baseball http://seanlahman.com/baseball-archive/statistics/
mkdir -p /tmp/baseball2
cd /tmp/baseball2
curl -Ls http://seanlahman.com/files/database/baseballdatabank-2017.1.zip > bball2.zip
unzip bball2.zip

mv baseball*/core/*.csv .

rm bball2.zip
rm -rf baseballdatabank-*

# create a google-cloud-storage bucket

gsutil mb gs://my-dataux2-bucket

# sync files to cloud
gsutil rsync -d -r /tmp/baseball2/  gs://my-dataux2-bucket/


# connect
mysql -h 127.0.0.1 -P4000


docker run -e "GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json" \
  -e "LOGGING=debug" \
  --rm -it \
  -p 4000:4000 \
  -v ~/.config/gcloud:/.config/gcloud \
  gcr.io/dataux-io/dataux:latest

```

```sql

CREATE source gcsbball2 WITH {
  "type":"cloudstore", 
  "schema":"gcsbball", 
  "settings" : {
     "type": "gcs",
     "bucket": "my-dataux-bucket",
     "format": "csv",
     "jwt": "/.config/gcloud/application_default_credentials.json"
  }
};

```