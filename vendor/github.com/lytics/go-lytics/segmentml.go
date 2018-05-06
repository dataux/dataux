package lytics

const (
	segmentMLEndpoint     = "segmentml/:id"
	segmentMLListEndpoint = "segmentml"
)

type SegmentML struct {
	Name       string             `json:"name"`
	FieldTypes map[string]string  `json:"fieldtypes,omitempty"`
	Importance map[string]float64 `json:"importance,omitempty"`

	Summary struct {
		Mse float64 `json:"Mse"`
		Rsq float64 `json:"Rsq"`

		Conf struct {
			FalsePositive int `json:"FalsePositive,omitempty"`
			TruePositive  int `json:"TruePositive,omitempty"`
			FalseNegative int `json:"FalseNegative,omitempty"`
			TrueNegative  int `json:"TrueNegative,omitempty"`
		} `json:"Conf,omitempty"`
	} `json:"summary,omitempty"`

	Conf struct {
		Source      *Segment `json:"source"`
		Target      *Segment `json:"target"`
		Additional  []string `json:"additional"`
		Collections []string `json:"collections"`
		Collect     int      `json:"collect"`
		UseScores   bool     `json:"use_scores"`
		UseContent  bool     `json:"use_content"`
		WriteToGcs  bool     `json:"write_to_gcs"`
	} `json:"conf,omitempty"`
}

// GetSegmentMLModel returns the details for a single segmentML Model based on id
// https://www.getlytics.com/developers/rest-api#segment-m-l
func (l *Client) GetSegmentMLModel(id string) (SegmentML, error) {
	res := ApiResp{}
	data := map[string]SegmentML{}

	// make the request
	err := l.Get(parseLyticsURL(segmentMLEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return SegmentML{}, err
	}

	return data[id], nil
}

// GetSegmentMLModels returns all models for the account
// https://www.getlytics.com/developers/rest-api#segment-m-l
func (l *Client) GetSegmentMLModels() (map[string]SegmentML, error) {
	res := ApiResp{}
	data := map[string]SegmentML{}

	// make the request
	err := l.Get(segmentMLListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
