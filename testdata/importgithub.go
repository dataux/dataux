package testdata

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
	esGithubLoaded sync.Once
)

func LoadGithubToEsOnce(address string) {
	esGithubLoaded.Do(func() {
		// Load 2014 December 01, 02 full 24 hours for those days
		LoadGithubToEs(address, 2014, 12, 1, 2)
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

// Create an Elasticsearch Put Mapping for customizing field mappings
func PutGithubMappings(address string) {

	for _, evType := range []string{"watch", "issues"} {
		esUrl := fmt.Sprintf("%s/github_%s", address, evType)
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

// Load a set of historical data from Github into Elasticsearch
//
// > LoadGithubToEs("localhost:9200",2015,1,2,24)
//
// @address = elasticsearch host:port address
// @year = 2015   ie which year of github history
// @month = 01    ie month of year
// @daysToImport = Number of days worth of data to import
// @hoursToImport = number of hours of data to import
func LoadGithubToEs(address string, year, month, daysToImport, hoursToImport int) {

	if !strings.HasPrefix(strings.ToLower(address), "http") {
		address = "http://" + address
	}
	PutGithubMappings(address)
	docCt := 0
	conn := es.NewConn()
	conn.SetFromUrl(address)
	u.Debugf("starting ES at host=%v port=%v", conn.Domain, conn.Port)
	indexer := conn.NewBulkIndexer(5)
	indexer.Sender = func(buf *bytes.Buffer) error {
		//u.Debugf("send:  %v  buffersize: %v", docCt, buf.Len())
		return indexer.Send(buf)
	}
	indexer.Start()

	defer func() {
		indexer.Flush()
	}()

	// curl -v -I http://data.githubarchive.org/2015-01-01-15.json.gz
	for day := 1; day <= daysToImport; day++ {
		for hr := 0; hr < hoursToImport; hr++ {
			downUrl := fmt.Sprintf("http://data.githubarchive.org/%d-%02d-%02d-%d.json.gz", year, month, day, hr)

			u.Info("Starting Download ", downUrl)
			resp, err := http.Get(downUrl)
			exitIfErr(err)
			defer resp.Body.Close()
			gzReader, err := gzip.NewReader(resp.Body)
			defer gzReader.Close()
			exitIfErr(err)
			r := bufio.NewReader(gzReader)
			var ge GithubEvent
			for {
				// New line delimited format
				line, err := r.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						indexer.Flush()
						break
					}
					u.Errorf("could not read line? %v", err)
					return
				}

				err = json.Unmarshal(line, &ge)
				if err != nil {
					u.Errorf("could not read line err= %v\n%v", err, string(line))
					continue
				}

				switch ge.Type() {
				case "watch", "issues":
					id := fmt.Sprintf("%x", md5.Sum(line))
					err = indexer.Index("github_"+ge.Type(), "event", id, "", "", &ge.Created, line)
					if err != nil {
						u.Errorf("error? %v", err)
					}
					docCt++
				case "push":
					// jh := u.NewJsonHelper(line)
					// delete(jh, "payload") // elasticsearch 2.1 hates that payload.shas alternates string/bool
					// line, _ = json.Marshal(&jh)
				}

			}

			u.Infof("finished importing %d json docs ", docCt)
		}
	}
}

func exitIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}
