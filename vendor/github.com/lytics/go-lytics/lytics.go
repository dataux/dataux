package lytics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Client defines the supported subset of the Lytics API. The Lytics API may contain other features
// that have been added or deprecated since the last update of this SDK.
//
// Lytics reserves the right to deprecate or modify endpoints at any time and does not guarantee
// backwards compatibility or SemVer versioning for early version of API.
//
// For more information see the Lytics API documentation at
// http://getlytics.com/developers/rest-api
//
//  Original Author: Mark Hayden
//  Contributions:   Mark Hayden
//  Version:         0.0.2
//
const (
	updated        = "2017-01-12"
	apiVersion     = "1.1.0"
	libraryVersion = "0.0.3"
)

var (
	apiBase = "https://api.lytics.io/api"
)

func init() {
	if apiEnv := os.Getenv("LIOAPI"); apiEnv != "" {
		apiBase = apiEnv + "/api"
	}
}

// Client bundles the data necessary to interact with the vast majority of Lytics REST endpoints.
type Client struct {
	baseURL    string
	apiKey     string
	dataApiKey string
	client     *http.Client
}

// ApiResp is the core api response for all Lytics endpoints. In some instances the "Status" is returned
// as a string rather than an int. This is a known but and will be addressed / updated.
type ApiResp struct {
	Status  interface{}     `json:"status"`
	Message string          `json:"message"`
	Meta    Meta            `json:"meta"`
	Next    string          `json:"_next"`
	Total   int             `json:"total"`
	Data    json.RawMessage `json:"data"`
}

type Meta struct {
	Format   string   `json:"name"`
	Name     []string `json:"by_fields"`
	ByFields []string `json:"by_fields"`
}

// NewLytics creates a new client instance. This contains the segment pager, segment details
// and maintains all core data used throughout this SDK
func NewLytics(apiKey, dataApiKey interface{}, httpclient *http.Client) *Client {
	l := Client{
		baseURL: apiBase,
	}

	if httpclient != nil {
		l.client = httpclient
	} else {
		l.client = http.DefaultClient
	}

	// set the apikey if not null
	if apiKey != nil {
		l.apiKey = apiKey.(string)
	}

	// set the dataapikey if not null
	if dataApiKey != nil {
		l.dataApiKey = dataApiKey.(string)
	}

	return &l
}

// BaseUrl returns the base url used in all calls for this client.
func (l *Client) BaseUrl() string {
	return l.baseURL
}

// ApiKey returns the API key configured for this client.
func (l *Client) ApiKey() string {
	return l.apiKey
}

// DataApiKey returns the public API key configured for this client.
func (l *Client) DataApiKey() string {
	return l.dataApiKey
}

// Client returns the HTTP client configured for this client.
func (l *Client) Client() *http.Client {
	return l.client
}

// SetClient updates the HTTP client for this client. Used to alter main client
// handling for instances such as AppEngine
func (l *Client) SetClient(c *http.Client) {
	l.client = c
}

// PrepUrl handles the parsing and setup for all api calls. The encoded url string is passed
// along with a set of param. Params are looped and injected into the master url
func (l *Client) PrepUrl(endpoint string, params url.Values, dataKey bool) (string, error) {
	// parse the url into native http.URL
	url, err := url.Parse(fmt.Sprintf("%s/%s", l.BaseUrl(), endpoint))
	if err != nil {
		return "", err
	}

	values := url.Query()

	// add the api key
	if params != nil {
		for key, val := range params {
			for _, v := range val {
				values.Add(key, v)
			}
		}
	}

	// if there is a data key use that by default, if not use the main api key
	// assumption here is that if its a data key there are specific reasons
	if dataKey {
		values.Add("key", l.dataApiKey)
	} else {
		values.Add("key", l.apiKey)
	}

	// encode the final url so we can return string and make call
	url.RawQuery = values.Encode()

	return url.String(), nil
}

// Do handles executing all http requests for the SDK. Takes a httpRequest and parses
// the response into the master api struct as well as a specific data type.
func (l *Client) Do(r *http.Request, response, data interface{}) error {
	// make the request

	res, err := l.client.Do(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// get the response
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	// if we have some struct to unmarshal body into, do that and return
	if response != nil {
		err = buildRespJSON(b, response, data)
		if err != nil {
			return err
		}
		switch rt := response.(type) {
		case *ApiResp:
			// if we have an invalid response code error out
			if res.StatusCode >= 301 {
				return fmt.Errorf(rt.Message)
			}
		}
	}

	// if we have an invalid response code error out
	if res.StatusCode >= 301 {
		return fmt.Errorf("Received non-successful response: %d", res.StatusCode)
	}

	return nil
}

// Get prepares a get request and then executes using the Do method
func (l *Client) Get(endpoint string, params url.Values, body interface{}, response, data interface{}) error {
	method := "GET"

	// get the formatted endpoint url
	path, err := l.PrepUrl(endpoint, params, false)
	if err != nil {
		return err
	}

	payload, err := prepRequestBody(body)
	if err != nil {
		return err
	}

	// build the request
	r, _ := http.NewRequest(method, path, payload)

	// execute the request
	err = l.Do(r, response, data)
	if err != nil {
		return err
	}

	return nil
}

// Get prepares a post request and then executes using the Do method
func (l *Client) Post(endpoint string, params url.Values, body interface{}, response, data interface{}) error {
	return l.PostType("application/json", endpoint, params, body, response, data)
}

// Get prepares a post request and then executes using the Do method
func (l *Client) PostType(contentType, endpoint string, params url.Values, body interface{}, response, data interface{}) error {
	method := "POST"

	// get the formatted endpoint url
	path, err := l.PrepUrl(endpoint, params, false)
	if err != nil {
		return err
	}

	//log.Printf("prep %s  %T \n", path, body)
	payload, err := prepRequestBody(body)
	if err != nil {
		return err
	}

	// build the request
	r, _ := http.NewRequest(method, path, payload)

	r.Header.Set("Content-Type", contentType)

	// execute the request
	err = l.Do(r, response, data)
	if err != nil {
		return err
	}

	return nil
}

// prepBodyRequest takes the payload and returns an io.Reader
func prepRequestBody(body interface{}) (io.Reader, error) {

	switch val := body.(type) {
	case string:
		return strings.NewReader(val), nil
	case nil:
		return nil, nil
	default:
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(b), nil
	}

	return nil, nil
}

// buildRespJSON handles the first round of unmarshaling into the master Api Response struct
func buildRespJSON(b []byte, response, data interface{}) error {
	var err error

	err = json.Unmarshal(b, response)
	if err != nil {
		return err
	}

	switch rt := response.(type) {
	case *ApiResp:
		if len(rt.Data) > 0 {
			err = json.Unmarshal(rt.Data, &data)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// parseLyticsTime translates a timestamp as returned by Lytics into a Go standard timestamp.
func parseLyticsTime(ts string) (time.Time, error) {
	var err error

	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Now(), err
	}

	tm := time.Unix((i / 1000), 0)

	return tm, err
}

// formatLyticsTime translates a timestamp into a human-readable form.
func formatLyticsTime(t *time.Time) string {
	return t.Format("Mon, 2 Jan 2006 15:04:05 -0700")
}

// parseLyticsURL joins params with a string using : notation
func parseLyticsURL(url string, params map[string]string) string {
	out := url

	for key, value := range params {
		out = strings.Replace(out, fmt.Sprintf(":%s", key), value, -1)
	}

	return out
}
