package lytics

import (
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	userRecommendEndpoint    = "content/recommend/user/:fieldName/:fieldVal"
	segmentRecommendEndpoint = "content/recommend/segment/:id"
	documentsEndpoint        = "content/doc"
	topicEndpoint            = "content/topic/:topicId"
	taxonomyEndpoint         = "content/taxonomy"
	rollupListEndpoint       = "content/topicrollup"
)

type Document struct {
	Url             string             `json:"url"`
	Title           string             `json:"title"`
	Description     string             `json:"description"`
	Topics          []string           `json:"topics"`
	TopicRelevances map[string]float64 `json:"topic_relevances"`
	PrimaryImage    string             `json:"primary_image"`
	Author          string             `json:"author"`
	Created         time.Time          `json:"created"`
	Hash            string             `json:"id"`
	Sitename        string             `json:"sitename,omitempty"`
	Stream          string             `json:"stream"`
	Path            []string           `json:"path,omitempty"`
	Aspects         []string           `json:"aspects,omitempty"`
	Language        string             `json:"language,omitempty"`
	Updated         time.Time          `json:"updated,omitempty"`
	Fetched         time.Time          `json:"fetched,omitempty"`
	Meta            []string           `json:"meta,omitempty"`
	Id              string             `json:"hashedurl,omitempty"`
}

type Recommendation struct {
	*Document
	Confidence float64 `json:"confidence"`
	Visited    bool    `json:"visited"`
	VisitRate  float64 `json:"visitrate,omitempty"`
}

// Filter params for recommendations:
// https://www.getlytics.com/developers/rest-api#content-recommendation
type RecommendationFilter struct {
	Limit   int         // limit on number of documents to suggest/return
	Ql      string      // raw FilterQL statement
	Shuffle interface{} // randomize the recommendation order (expected to be bool)
	Topics  []string    // allow recommendations on content with the specified topics
	Rollups []string    // allow recommendations on content that have relevance to a topic rollup
	From    string      // start of time range (publication date)
	To      string      // end of time range (publication date)
	Path    string      // url path to match
	Domain  string      // url domain to match
	Rank    string      // (popular | recent | affinity)
	Visited interface{} // show recommendations for content the user has already viewed (expected to be bool)
}

type Documents struct {
	Urls  []Document `json: "urls"`
	Total int        `json: "total"`
}

type TopicSummary struct {
	Topics struct {
		Total      int     `json:"total"`
		Missing    int     `json:"missing"`
		Present    int     `json:"present"`
		NoneBucket int     `json:"bucket_none"`
		LowBucket  int     `json:"bucket_low"`
		MidBucket  int     `json:"bucket_mid"`
		HighBucket int     `json:"bucket_high"`
		Avg        float64 `json:"avg"`
	} `json:"topics"`

	Docs struct {
		Total int        `json:"total"`
		Urls  []Document `json:urls`
	}
}

type TopicNode struct {
	Name  string `json:"name"`
	Count int    `json:"doc_count"`
}

type TopicLink struct {
	Source int     `json:"source"`
	Target int     `json:"target"`
	Value  float64 `json:"value"`
}

type TopicGraph struct {
	DocCount int          `json:"n"`
	Nodes    []*TopicNode `json:"nodes"`
	Links    []*TopicLink `json:"links"`
}

type Topic struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

type TopicRollup struct {
	Id     string   `json:"id"`
	AcctId string   `json:"acctid"`
	Label  string   `json:"label"`
	Topics []*Topic `json:"topics"`
}

// GetUserContentRecommendation returns a list of documents
// to recommend the user based on their content affinities
// https://www.getlytics.com/developers/rest-api#content-recommendation
func (l *Client) GetUserContentRecommendation(fieldName, fieldVal string, filter *RecommendationFilter) ([]Recommendation, error) {
	res := ApiResp{}
	data := []Recommendation{}
	params := url.Values{}

	if filter.Limit > 0 {
		params.Add("limit", strconv.Itoa(filter.Limit))
	}

	if shuffle, ok := filter.Shuffle.(bool); ok {
		params.Add("shuffle", strconv.FormatBool(shuffle))
	}

	if filter.Ql != "" {
		params.Add("ql", filter.Ql)
	}

	for _, topic := range filter.Topics {
		params.Add("topics[]", topic)
	}

	for _, rollup := range filter.Rollups {
		params.Add("rollups[]", rollup)
	}

	if filter.To != "" {
		params.Add("to", filter.To)
	}

	if filter.From != "" {
		params.Add("from", filter.From)
	}

	if filter.Path != "" {
		params.Add("path", filter.Path)
	}

	if filter.Domain != "" {
		params.Add("domain", filter.Domain)
	}

	if filter.Rank != "" {
		params.Add("rank", filter.Rank)
	}

	if visited, ok := filter.Visited.(bool); ok {
		params.Add("visited", strconv.FormatBool(visited))
	}

	// make the request
	err := l.Get(parseLyticsURL(userRecommendEndpoint, map[string]string{"fieldName": fieldName, "fieldVal": fieldVal}), params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentContentRecommendation returns a list of documents
// to recommend to users in a segment
func (l *Client) GetSegmentContentRecommendation(segId string, filter *RecommendationFilter) ([]Recommendation, error) {
	res := ApiResp{}
	data := []Recommendation{}
	params := url.Values{}

	if filter.Limit > 0 {
		params.Add("limit", strconv.Itoa(filter.Limit))
	}

	if shuffle, ok := filter.Shuffle.(bool); ok {
		params.Add("shuffle", strconv.FormatBool(shuffle))
	}

	if filter.Ql != "" {
		params.Add("ql", filter.Ql)
	}

	for _, topic := range filter.Topics {
		params.Add("topics[]", topic)
	}

	for _, rollup := range filter.Rollups {
		params.Add("rollups[]", rollup)
	}

	if filter.To != "" {
		params.Add("to", filter.To)
	}

	if filter.From != "" {
		params.Add("from", filter.From)
	}

	if filter.Path != "" {
		params.Add("path", filter.Path)
	}

	if filter.Domain != "" {
		params.Add("domain", filter.Domain)
	}

	if filter.Rank != "" {
		params.Add("rank", filter.Rank)
	}

	if visited, ok := filter.Visited.(bool); ok {
		params.Add("visited", strconv.FormatBool(visited))
	}

	// make the request
	err := l.Get(parseLyticsURL(segmentRecommendEndpoint, map[string]string{"id": segId}), params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetDocuments returns a summary for document(s).
// If no url is specified, then return the 10 most recent urls.
// https://www.getlytics.com/developers/rest-api#content-document
func (l *Client) GetDocuments(urls []string, limit int) (Documents, error) {
	res := ApiResp{}
	data := Documents{}
	params := url.Values{}

	if limit > 0 {
		params.Add("limit", strconv.Itoa(limit))
	}

	if len(urls) > 0 {
		params.Add("urls", strings.Join(urls, ","))
	}

	err := l.Get(documentsEndpoint, params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetTopicSummary returns a summary of user affinity for, and related
// documents to a topic.
// https://www.getlytics.com/developers/rest-api#content-topic-summary
func (l *Client) GetTopicSummary(topic string, limit int) (TopicSummary, error) {
	res := ApiResp{}
	data := TopicSummary{}
	params := url.Values{}

	if limit > 0 {
		params.Add("limit", strconv.Itoa(limit))
	}

	err := l.Get(parseLyticsURL(topicEndpoint, map[string]string{"topicId": topic}), params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetContentTaxonomy returns a graph relationship among topics.
// https://www.getlytics.com/developers/rest-api#content-taxonomy
func (l *Client) GetContentTaxonomy() (TopicGraph, error) {
	res := ApiResp{}
	data := TopicGraph{}

	err := l.Get(taxonomyEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetTopicRollups returns a list of topic rollups for an account.
func (l *Client) GetTopicRollups() ([]TopicRollup, error) {
	res := ApiResp{}
	data := []TopicRollup{}

	err := l.Get(rollupListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
