

Lytics Source
---------------------------
Query [Lytics Api](https://www.getlytics.com/developers/rest-api#segment-scan) 
Or more specifically the Segment Scan api.

Use SQL To query the api and via the mysql protocol, uses Secure HTTPS
for transport to Lytics Api.


Example
----------------------------

The default example dataux docker container
includes a single table `appearances` in database `baseball`
that is made from a single csv file stored in the container.

We are going to add an additional source (Lytics) via a `SQL CREATE` statement.

```sh

docker pull gcr.io/dataux-io/dataux:latest
docker run --rm -e "LOGGING=debug" -p 4000:4000 --name dataux gcr.io/dataux-io/dataux:latest

```

![dataux_lytics](https://cloud.githubusercontent.com/assets/7269/23976456/0c6bc878-09a5-11e7-9cec-207c300ed0ab.png)


```sql
mysql -h 127.0.0.1 -P4000


-- Get api key from Account in Lytics

CREATE source lytics WITH {
  "type":"lytics", 
  "schema":"lytics", 
  "settings" : {
     "apikey":"YOUR_API_KEY"
  }
};

use lytics;

show tables;

describe user;

select * from user WHERE EXISTS email limit 10;

```


TODO
------------------------
* handle ending paging/limits (ie, don't open cursors)
* paging segment scans efficiently
* Select INTO (mytable)
