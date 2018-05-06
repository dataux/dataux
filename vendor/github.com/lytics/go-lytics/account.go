package lytics

import (
	"time"
)

const (
	accountEndpoint     = "account/:id"
	accountListEndpoint = "account"
)

type Account struct {
	Id               string    `json:"id"`
	Created          time.Time `json:"created"`
	Updated          time.Time `json:"updated"`
	Fid              string    `json:"fid"`
	Domain           string    `json:"domain"`
	Aid              int       `json:"aid"`
	ParentAid        int       `json:"parentaid"`
	ParentId         string    `json:"parent_id"`
	PartnerId        string    `json:"partner_id"`
	PackageId        string    `json:"package_id"`
	Email            string    `json:"email"`
	ApiKey           string    `json:"apikey"`
	DataApiKey       string    `json:"dataapikey"`
	Name             string    `json:"name"`
	TimeZone         string    `json:"timezone,omitempty"`
	PubUsers         bool      `json:"pubusers"`
	WhitelistFields  []string  `json:"whitelist_fields"`
	WhitelistDomains []string  `json:"whitelist_domains"`
}

// GetAccounts returns a list of all accounts associated with master account
// https://www.getlytics.com/developers/rest-api#accounts-list
func (l *Client) GetAccounts() ([]Account, error) {
	res := ApiResp{}
	data := []Account{}

	// make the request
	err := l.Get(accountListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetAccount returns the details of a single account based on id
// https://www.getlytics.com/developers/rest-api#account
func (l *Client) GetAccount(id string) (Account, error) {
	res := ApiResp{}
	data := Account{}

	// make the request
	err := l.Get(parseLyticsURL(accountEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// Other Available Endpoints
// * POST    create account
// * PUT     upate account
// * DELETE  remove account
