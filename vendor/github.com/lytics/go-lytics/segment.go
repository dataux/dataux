package lytics

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	segmentEndpoint               = "segment/:id"
	segmentListEndpoint           = "segment"
	segmentSizeEndpoint           = "segment/:id/sizes"
	segmentSizesEndpoint          = "segment/sizes"       // ids
	segmentAttributionEndpoint    = "segment/attribution" // ids
	segmentScanEndpoint           = "segment/:id/scan"
	adHocsegmentScanEndpoint      = "segment/scan"
	segmentCollectionListEndpoint = "segmentcollection"
	segmentCollectionEndpoint     = "segmentcollection/:id"
	segmentCreateEndpoint         = segmentListEndpoint
	segmentValidateEndpoint       = "segment/validate"
)

type (
	// Segment is a logical expression to filter entity
	//  The normal concept is logical filter to find users, but the
	//  table also allows logical filter on content, or other entity types.
	Segment struct {
		Id            string    `json:"id"`
		AccountId     string    `json:"account_id"`
		Name          string    `json:"name"`
		IsPublic      bool      `json:"is_public"`
		SlugName      string    `json:"slug_name"`
		Description   string    `json:"description,omitempty"`
		SegKind       string    `json:"kind,omitempty"`
		Table         string    `json:"table,omitempty"`
		AuthorId      string    `json:"author_id"`
		Updated       time.Time `json:"updated"`
		Created       time.Time `json:"created"`
		Tags          []string  `json:"tags"`
		Category      string    `json:category,omitempty`
		Invalid       bool      `json:"invalid"`
		InvalidReason string    `json:"invalid_reason"`
		FilterQL      string    `json:"segment_ql,omitempty"`
		AST           *Expr     `json:"ast,omitempty"`
	}
	// SegmentSize request is just name, slug, id, size
	// - also filters out any non Kind="Segment" segments
	SegmentSize struct {
		Id       string  `json:"id"`
		Name     string  `json:"name"`
		SlugName string  `json:"slug_name"`
		Size     float64 `json:"size"`
	}
	// SegmentAttribution is segment size history
	SegmentAttribution struct {
		Id      string                      `json:"id"`
		Metrics []SegmentAttributionMetrics `json:"metrics"`
	}
	// Specific metric point for a segment at a given time
	SegmentAttributionMetrics struct {
		Value   int64   `json:"value"`
		Ts      string  `json:"ts"`
		Anomaly float64 `json:"anomaly"`
	}
	// SegmentCollection is a set of Segments logically grouped
	// and containing relations (ordering)
	SegmentCollection struct {
		AccountId     string            `json:"account_id"`
		Id            string            `json:"id"`
		Name          string            `json:"name"`
		Slug          string            `json:"slug_name"`
		Description   string            `json:"description,omitempty"`
		Table         string            `json:"table,omitempty"`
		AuthorId      string            `json:"author_id"`
		Updated       time.Time         `json:"updated"`
		Created       time.Time         `json:"created"`
		Internal      bool              `json:"internal"`
		Collection    []*SegColRelation `json:"collection""`
		ParentSegment string            `json:"parent_segment"`
	}
	// SegColRelation maps a segment relationship to a collection
	SegColRelation struct {
		Id    string `json:"id"`
		Order int    `json:"order"`
	}
	// SegmentScanner is a stateful, forward only pager to iterate through
	// entities in a Segment
	SegmentScanner struct {
		SegmentID string
		SegmentQl string
		next      string
		previous  string
		buffer    chan []Entity
		nextChan  chan Entity
		shutdown  chan bool
		Total     int
		Batches   []int
		err       error
	}

	// Expr is the AST structures of a SegmentQL statement
	Expr struct {
		// The token, and node expressions are non
		// nil if it is an expression
		Op   string  `json:"op,omitempty"`
		Args []*Expr `json:"args,omitempty"`

		// If op is 0, and args nil then exactly one of these should be set
		Identity string `json:"ident,omitempty"`
		Value    string `json:"val,omitempty"`
	}
)

func (s *SegmentScanner) Stop() {
	defer func() { recover() }()
	close(s.shutdown)
}

func (s *SegmentScanner) Err() error {
	return s.err
}

func (s *SegmentScanner) Next() Entity {
	select {
	case e, ok := <-s.nextChan:
		if !ok {
			return nil
		}
		return e
	case <-s.shutdown:
		return nil
	}
}

// Created is a helper method to convert the timestamp into human readable format for metrics
func (s *SegmentAttributionMetrics) Created() (time.Time, error) {
	return parseLyticsTime(s.Ts)
}

// PostSegment creates a Segment
// https://www.getlytics.com/developers/rest-api#segment
func (l *Client) PostSegment(segmentQL string) (Segment, error) {
	res := ApiResp{}
	data := Segment{}

	// make the request
	err := l.PostType("text/plain", "segment", nil, segmentQL, &res, &data)

	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegment returns the details for a single segment based on id
// https://www.getlytics.com/developers/rest-api#segment
func (l *Client) GetSegment(id string) (Segment, error) {
	res := ApiResp{}
	data := Segment{}

	// make the request
	err := l.Get(parseLyticsURL(segmentEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegments returns a list of all segments for an account
// https://www.getlytics.com/developers/rest-api#segment-list
func (l *Client) GetSegments(table string) ([]Segment, error) {
	res := ApiResp{}
	data := []Segment{}
	params := url.Values{}

	params.Add("table", table)

	// make the request
	err := l.Get(segmentListEndpoint, params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentSize returns the segment size information for a single segment
// https://www.getlytics.com/developers/rest-api#segment-sizes
func (l *Client) GetSegmentSize(id string) (SegmentSize, error) {
	res := ApiResp{}
	data := SegmentSize{}

	// make the request
	err := l.Get(parseLyticsURL(segmentSizeEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentSizes returns the segment sizes for all segments on an account
// https://www.getlytics.com/developers/rest-api#segment-sizes
func (l *Client) GetSegmentSizes(segments []string) ([]SegmentSize, error) {
	params := url.Values{}
	res := ApiResp{}
	data := []SegmentSize{}

	// if we have specific segments to filter by add those to the params as comma separated string
	if len(segments) > 0 {
		params.Add("ids", strings.Join(segments, ","))
	}

	// make the request
	err := l.Get(segmentSizesEndpoint, params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentAttribution returns the attribution (change over time) for segments
// method accepts a string slice of 1 or more segments to query.
// NOT CURRENTLY DOCUMENTED
func (l *Client) GetSegmentAttribution(segments []string) ([]SegmentAttribution, error) {
	params := url.Values{}

	res := ApiResp{}
	data := []SegmentAttribution{}

	// if the request is for a specific set of segments add that as comma separated param
	if len(segments) > 0 {
		params.Add("ids", strings.Join(segments, ","))
	}

	// make the request
	err := l.Get(parseLyticsURL(segmentAttributionEndpoint, nil), params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentCollection returns a single collection of segments
// (a grouped/named lists of segments)
func (l *Client) GetSegmentCollection(id string) (SegmentCollection, error) {
	res := ApiResp{}
	data := SegmentCollection{}

	err := l.Get(parseLyticsURL(segmentCollectionEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetSegmentCollectionList returns a list of all segment
// collections for an account
func (l *Client) GetSegmentCollectionList() ([]SegmentCollection, error) {
	res := ApiResp{}
	data := []SegmentCollection{}

	err := l.Get(parseLyticsURL(segmentCollectionListEndpoint, nil), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// Other Available Endpoints
// * DELETE  remove segment

// **************************** START OF SEGMENT SCAN METHODS ****************************

// GetSegmentEntities returns a single page of entities for the given segment
// also returns the next value if there are more than limit entities in the segment
// https://www.getlytics.com/developers/rest-api#segment-scan
func (l *Client) GetSegmentEntities(segment, next string, limit int) (interface{}, string, []Entity, error) {
	res := ApiResp{}
	data := []Entity{}
	params := url.Values{}

	params.Add("start", next)
	params.Add("limit", strconv.Itoa(limit))

	// make the request
	err := l.Get(parseLyticsURL(segmentScanEndpoint, map[string]string{"id": segment}), params, nil, &res, &data)
	if err != nil {
		return "", "", data, err
	}

	return res.Status, res.Next, data, nil
}

// GetAdHocSegmentEntities returns a single page of entities for the given Ad Hoc segment
// also returns the next value if there are more than limit entities in the segment
// https://www.getlytics.com/developers/rest-api#segment-scan
func (l *Client) GetAdHocSegmentEntities(ql, next string, limit int) (interface{}, string, []Entity, error) {

	res := ApiResp{}
	data := []Entity{}
	params := url.Values{}

	params.Add("start", next)
	params.Add("limit", strconv.Itoa(limit))

	err := l.PostType("text/plain", adHocsegmentScanEndpoint, params, ql, &res, &data)
	if err != nil {
		return "", "", data, err
	}

	return res.Status, res.Next, data, nil
}

func (s *SegmentScanner) run(c *Client) {
	var (
		entities []Entity
		fails    int
		maxTries int
		err      error
	)

	maxTries = 10

	go func() {
		defer func() { recover() }()
		// This drains the Buffer into output channel
		for {
			select {
			case <-s.shutdown:
				// we are shutdown
				return
			case el, ok := <-s.buffer:
				if ok {
					for _, e := range el {
						s.nextChan <- e
					}
				} else {
					// buffer is closed, no more entities
					close(s.nextChan)
				}
			}
		}
	}()
	// make calls for next batch of segment entities until we run out of next pages
	for {

		select {
		case <-s.shutdown:
			// we are shutdown
			return
		default:
			// keep paging
		}
		switch {
		case s.SegmentQl != "":
			_, s.next, entities, err = c.GetAdHocSegmentEntities(s.SegmentQl, s.next, 100)
		case s.SegmentID != "":
			_, s.next, entities, err = c.GetSegmentEntities(s.SegmentID, s.next, 100)
		default:
			s.err = fmt.Errorf("Must have segment id or segmentql")
			return
		}

		if err != nil {
			fails++

			if fails > maxTries {
				s.err = err
				return
			}

			// if we fail and have not exceeded the limit, try again
			continue
		} else {
			maxTries = 10
		}

		// for logging add the batch details to the scanner
		s.Batches = append(s.Batches, len(entities))

		// for logging add the total entites returned to the scanner
		s.Total = s.Total + len(entities)

		// if buffer is full we will block here
		// thus preving getting too far ahead of consumption
		s.buffer <- entities

		// if there are no more pages we will have a blank next, just break and return
		if s.next == "" {
			close(s.buffer)
			break
		}
	}
}

// PageSegment Pages by either SegmentId or QL
func (l *Client) PageSegment(qlOrId string) *SegmentScanner {
	return l.pageSegment(qlOrId)
}

// PageSegmentId pages through each user in segment
func (l *Client) PageSegmentId(segmentid string) *SegmentScanner {
	return l.pageSegment(segmentid)
}
func (l *Client) pageSegment(qlOrId string) *SegmentScanner {

	segmentId, ql := "", ""

	if strings.Contains(qlOrId, " ") {
		// segmentql
		ql = qlOrId
	} else {
		segmentId = qlOrId
	}

	scanner := &SegmentScanner{
		buffer:    make(chan []Entity, 1),
		nextChan:  make(chan Entity, 1),
		shutdown:  make(chan bool),
		SegmentID: segmentId,
		SegmentQl: ql,
	}

	// fire up the go routine for paging entities
	go scanner.run(l)
	return scanner
}

// PageAdHocSegment sets the ad-hoc segment ql on the master scanner and initiates
// the main go routine for paging
func (l *Client) PageAdHocSegment(ql string) *SegmentScanner {
	return l.pageSegment(ql)
}

// CreateSegment creates a new segment from a Segment QL logic expression
// https://www.getlytics.com/developers/rest-api#segment
func (l *Client) CreateSegment(name, ql, slug string) (Segment, error) {
	res := ApiResp{}
	data := Segment{}

	payload := Segment{
		Name:     name,
		FilterQL: ql,
		SlugName: slug,
	}

	// make the request
	err := l.Post(segmentCreateEndpoint, nil, payload, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// ValidateSegment validates a single segment QL statement
// https://www.getlytics.com/developers/rest-api#segment-validate
func (l *Client) ValidateSegment(ql string) (bool, error) {
	res := ApiResp{}

	err := l.Post(segmentValidateEndpoint, nil, ql, &res, nil)
	if err != nil {
		return false, err
	}

	if res.Message == "success" {
		return true, nil
	}

	return false, nil
}
