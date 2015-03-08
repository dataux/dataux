package testutil

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	es "github.com/mattbaird/elastigo/lib"
)

var (
	loaded sync.Once
)

func LoadOnce(host string) {
	loaded.Do(func() {
		LoadTestData(host, 2014, 12, 2, 24)
	})
}

type GithubEvent struct {
	Url     string
	Created time.Time `json:"created_at"`
	TypeVal string    `json:"type"`
}

func (g *GithubEvent) Type() string {
	return strings.Replace(strings.ToLower(g.TypeVal), "event", "", -1)
}

func PutGithubMappings(host string) {

	for _, evType := range []string{"release", "watch", "fork"} {
		esUrl := fmt.Sprintf("http://%s:9200/github_%s", host, evType)
		u.Debugf("create index/mapping at %v", esUrl)
		u.PostJson(esUrl, `
		{
		    "settings" : {
		        "number_of_shards" : 1
		    },
		    "mappings" : {
		        "event" : {
		            "properties" : {
		                "actor_attributes" : {
		                    "type" : "object",
		                    "properties" : {
		                       "login" : {"type" : "string", "index" : "not_analyzed" }
		                       , "name" : {"type" : "string", "index" : "not_analyzed" }
		                       , "company" : {"type" : "string", "index" : "not_analyzed" }
		                    }
		                },
		                "repository" : {
		                    "type" : "object",
		                    "properties" : {
		                       "language" : {"type" : "string", "index" : "not_analyzed" }
		                       , "name" : {"type" : "string", "index" : "not_analyzed" }
		                       , "organization" : {"type" : "string", "index" : "not_analyzed" }
		                    }
		                }
		            }
		        }
		    }
		}`)
	}

}

//    LoadTestData("localhost",2015,1,2,24)
func LoadTestData(host string, year, month, daysToImport, hoursToImport int) {
	defer func() {
		time.Sleep(time.Second * 3)
	}()

	PutGithubMappings(host)
	docCt := 0
	conn := es.NewConn()
	conn.Domain = host
	indexer := conn.NewBulkIndexer(5)
	indexer.Sender = func(buf *bytes.Buffer) error {
		//u.Debugf("send:  %v  buffersize: %v", docCt, buf.Len())
		return indexer.Send(buf)
	}
	indexer.Start()

	//http://data.githubarchive.org/2015-01-01-15.json.g
	for day := 1; day <= daysToImport; day++ {
		for hr := 0; hr < hoursToImport; hr++ {
			downUrl := fmt.Sprintf("http://data.githubarchive.org/%d-%02d-%02d-%d.json.gz", year, month, day, hr)

			u.Info("Starting Download ", downUrl)
			//return
			resp, err := http.Get(downUrl)
			if err != nil || resp == nil {
				panic("Could not download data")
			}
			defer resp.Body.Close()
			if err != nil {
				u.Error(err)
				return
			}
			gzReader, err := gzip.NewReader(resp.Body)
			defer gzReader.Close()
			if err != nil {
				panic(err)
			}
			r := bufio.NewReader(gzReader)
			var ge GithubEvent
			for {
				// New line delimited format
				line, err := r.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						indexer.Flush()
						break
					} else {
						u.Errorf("FATAL:  could not read line? %v", err)
						return
					}
				}
				if err := json.Unmarshal(line, &ge); err == nil {
					//u.Debugf("type: %v", ge.Type())
					// switch ge.Type() {
					// case "release", "watch", "fork", "push":
					// 	// ok
					// default:
					// 	continue
					//}
					// switch ge.Type() {
					// case "push", "fork", "watch", "release", "member":
					// }
					// create a Document ID that is consistent across imports
					id := fmt.Sprintf("%x", md5.Sum(line))
					//indexer.Index("github", ge.Type, id, "", &ge.Created, line)
					err = indexer.Index("github_"+ge.Type(), "event", id, "", &ge.Created, line, true)
					if err != nil {
						u.Errorf("error? %v", err)
					}
					docCt++

				} else {
					u.Errorf("ERROR? %v", string(line))
				}
			}

			u.Infof("finished importing %d json docs ", docCt)
		}
	}
}

func waitFor(check func() bool, timeoutSecs int) {
	timer := time.NewTicker(100 * time.Millisecond)
	tryct := 0
	for _ = range timer.C {
		if check() {
			timer.Stop()
			break
		}
		if tryct >= timeoutSecs*10 {
			timer.Stop()
			break
		}
		tryct++
	}
}
