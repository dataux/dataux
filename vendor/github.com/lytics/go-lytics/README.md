#Lytics SDK for Go
The Lytics SDK for go offers easy integration with our public REST API endpoints. 
This library is actively being managed and every effort will be made to ensure 
that all handling reflects the best methods available. 
Overview of supported methods outlined below.

## Full REST API Documentation
https://www.getlytics.com/developers/rest-api

## Command Line Tool Doc


The [Lytics CLI](https://github.com/lytics/lytics) utilizes this library.


## Getting Started
1. Import the library. `go get github.com/lytics/go-lytics`
2. Create a new client from api key.
3. Run one of the many methods to access account info.

```
package main

import (
	"fmt"
	lytics "github.com/lytics/go-lytics"
)

func main() {
	// set your api key
	key := "<YOUR API KEY>"

	// create the client
	client := lytics.NewLytics(key, nil, nil)

	// create a scanner for All Users in a Segment 
	scan := client.PageSegment(`
		FILTER AND (
		    lastvisit_ts > "now-2d"
		    EXISTS email
		)
		FROM user
	`)

	// handle processing the users
	for {
		e := scan.Next()
		if e == nil {
			break
		}

		fmt.Println(e.PrettyJson())
	}
}
```

## Examples
* [Get All Accounts](examples/get_accounts.md)
* [Get All Segments](examples/get_segments.md)
* [Get Segments A User Belongs To](examples/get_segments_for_user.md)
* [Page Through Users in Segment](examples/page_through_segment.md)

## Supported Methods
* **Account**
	* Single `GET`
	* All `GET`
* **Admin User**
	* Single `GET`
	* All `GET`
* **Segment**
	* Single `GET`
	* All `GET` 
* **Entity (end users) API** `GET`
* **Catalog**
	* Schema `GET`
* **Query**
	* All `GET`
	* Test Evaluation `POST`

## Contributing
Want to add something? Go for it, just fork the repo and 
send us a PR. Please make sure all tests run `go test -v` 
and that all new functionality comes with well documented and thorough testing.

## License
[MIT](LICENSE.md)
Copyright (c) 2017, 2016, 2015 Lytics