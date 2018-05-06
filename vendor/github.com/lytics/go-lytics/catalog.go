package lytics

import (
	"net/url"
	"strconv"
	"strings"
)

const (
	schemaEndpoint               = "schema"
	schemaTableEndpoint          = "schema/:table"           // limit
	schemaTableFieldinfoEndpoint = "schema/:table/fieldinfo" // limit, fields
	routeSchemaStreams           = "schema/_streams"
)

/*

{
  "data": [
    {
      "hidden": false,
      "ct": 24,
      "curct": 0,
      "stream": "cm_content",
      "last_msg_ts": "1482433744000",
      "last_update_ts": "1483486363322",
      "metrics": [
        {
          "ct": 0,
          "ts": "1483471963000"
        },
        {
          "ct": 0,
          "ts": "1483477363000"
        },
        {
          "ct": 0,
          "ts": "1483482763000"
        }
      ],
      "recent_events": [
        {
          "ContentUrl": [
            "http://createsend.com/t/t-2CD8ACA1878CC54C/t"
          ],
          "Html": [




curl -s -H "Authorization: $LIOKEY"   -XGET "$LIOAPI/api/schema/user/fieldinfo?fields=utm_campaigns" | jq '.'
{
  "data": {
    "table": "user",
    "fields": [
      {
        "field": "utm_campaigns",
        "terms_counts": {
          "mycampaign": 2956,
          "email_week_22": 2532
        },
        "more_terms": true,
        "ents_present": 318308,
        "ents_absent": 2324726,
        "approx_cardinality": 1215
      }
    ]
  },
  "message": "success",
  "status": 200
}


*/

type (
	Schema struct {
		Name     string   `json:"name"`
		ByFields []string `json:"by_fields"`
		Columns  Columns  `json:"columns"`
	}

	Columns []Column
	Column  struct {
		As         string   `json:"as"`
		IsBy       bool     `json:"is_by"`
		Type       string   `json:"type"`
		ShortDesc  string   `json:"shortdesc"`
		LongDesc   string   `json:"longdesc"`
		Froms      []string `json:"froms"`
		Identities []string `json:"identities"`
	}
	Stream struct {
		Name           string       `json:"stream"`
		Ct             int          `json:"ct"`
		LastMsgTime    JsonTime     `json:"last_msg_ts,omitempty"`
		LastUpdateTime JsonTime     `json:"last_update_ts,omitempty"`
		Recent         []url.Values `json:"recent_events,omitempty"`
	}
	SchemaFieldInfo struct {
		Table  string      `json:"table"`
		Fields []FieldInfo `json:"fields"`
	}
	FieldInfo struct {
		Field             string         `json:"field"`
		More              bool           `json:"more_terms"`
		EntsPresent       int64          `json:"ents_present"`
		EntsAbsent        int64          `json:"ents_absent"`
		ApproxCardinality int64          `json:"approx_cardinality"`
		TermCounts        map[string]int `json:"terms_counts"`
	}
)

// GetSchema returns the data schema for an account
// https://www.getlytics.com/developers/rest-api#schema
func (l *Client) GetSchema() (map[string]*Schema, error) {
	res := ApiResp{}
	data := make(map[string]*Schema)

	// make the request
	err := l.Get(parseLyticsURL(schemaEndpoint, nil), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSchema returns the data schema for an account
// https://www.getlytics.com/developers/rest-api#schema
func (l *Client) GetSchemaTable(table string) (Schema, error) {
	res := ApiResp{}
	data := Schema{}

	// make the request
	err := l.Get(parseLyticsURL(schemaTableEndpoint, map[string]string{"table": table}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetStreams returns the data stream field introspection for an account
// https://www.getlytics.com/developers/rest-api#streams
func (l *Client) GetStreams(stream string) ([]*Stream, error) {
	res := ApiResp{}
	data := []*Stream{}

	// make the request
	err := l.Get(parseLyticsURL(routeSchemaStreams, nil), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetFieldInfo returns the metadata about a single field
// https://www.getlytics.com/developers/rest-api#schema
func (l *Client) GetSchemaFieldInfo(table string, fields []string, limit int) (SchemaFieldInfo, error) {
	res := ApiResp{}
	data := SchemaFieldInfo{}

	qs := make(url.Values)
	qs.Set("fields", strings.Join(fields, ","))
	qs.Set("limit", strconv.Itoa(limit))

	// make the request
	err := l.Get(parseLyticsURL(schemaTableFieldinfoEndpoint, map[string]string{"table": table}), qs, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
