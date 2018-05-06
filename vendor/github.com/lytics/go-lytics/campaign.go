package lytics

import (
	"net/url"
	"strings"
	"time"
)

const (
	campaignEndpoint      = "program/campaign/:id"
	campaignListEndpoint  = "program/campaign" //status
	variationEndpoint     = "program/campaign/variation/:id"
	variationListEndpoint = "program/campaign/variation"
)

type Campaign struct {
	Id           string    `json:"id,omitempty"`
	Name         string    `json:"name,omitempty"`
	Status       string    `json:"status,omitempty"`
	SystemStatus string    `json:"system_status,omitempty"`
	PublishedAt  time.Time `json:"published_at,omitempty"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	StartAt      time.Time `json:"start_at,omitempty"`
	EndAt        time.Time `json:"end_at,omitempty"`
	Segments     []string  `json:"segments,omitempty"`
	DeletedAt    time.Time `json:"deleted_at,omitempty"`
	Deleted      bool      `json:"deleted,omitempty"`
	Aid          int       `json:"aid,omitempty"`
	AccountId    string    `json:"account_id,omitempty"`
	UserId       string    `json:"user_id,omitempty"`
}

type Variation struct {
	Id             string                 `json:"id,omitempty"`
	Variation      int                    `json:"variation"`
	CampaignId     string                 `json:"campaign_id,omitempty"`
	Vehicle        string                 `json:"vehicle,omitempty"`
	Reach          string                 `json:"reach,omitempty"`
	Conversion     string                 `json:"conversion,omitempty"`
	Detail         map[string]interface{} `json:"detail,omitempty"`
	DetailOverride map[string]interface{} `json:"detail_override,omitempty"`
	CreatedAt      time.Time              `json:"created_at,omitempty"`
	UpdatedAt      time.Time              `json:"updated_at,omitempty"`
	Deleted        bool                   `json:"deleted,omitempty"`
	DeletedAt      time.Time              `json:"deleted_at,omitempty"`
	Preview        bool                   `json:"preview,omitempty"`
	Aid            int                    `json:"aid,omitempty"`
	AccountId      string                 `json:"account_id,omitempty" bson:"account_id"`
	UserId         string                 `json:"user_id,omitempty" bson:"user_id"`
}

// GetCampaign returns the details for a single personalization campaign
func (l *Client) GetCampaign(id string) (Campaign, error) {
	res := ApiResp{}
	data := Campaign{}

	// make the request
	err := l.Get(parseLyticsURL(campaignEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetCampaignList returns the details for all campaigns in an account
// optional status parameter to filter by status: published, unpublished, deleted
func (l *Client) GetCampaignList(status []string) ([]Campaign, error) {
	params := url.Values{}

	res := ApiResp{}
	data := []Campaign{}

	if len(status) > 0 {
		params.Add("status", strings.Join(status, ","))
	}

	// make the request
	err := l.Get(campaignListEndpoint, params, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetVariation returns the details for a single campaign variation
func (l *Client) GetVariation(id string) (Variation, error) {
	res := ApiResp{}
	data := Variation{}

	// make the request
	err := l.Get(parseLyticsURL(variationEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetVariationList returns the details for all variations of all campaigns in the account
func (l *Client) GetVariationList() ([]Variation, error) {
	res := ApiResp{}
	data := []Variation{}

	// make the request
	err := l.Get(variationListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
