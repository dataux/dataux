package lytics

import (
	"time"
)

const (
	authEndpoint     = "auth/:id"
	authListEndpoint = "auth"
)

type Auth struct {
	Id           string    `json:"id"`
	Aid          int       `json:"aid"`
	Name         string    `json:"name"`
	AccountId    string    `json:"account_id"`
	ProviderId   string    `json:"provider_id"`
	ProviderName string    `json:"provider_name"`
	UserId       string    `json:"user_id"`
	Created      time.Time `json:"created"`
	Updated      time.Time `json:"updated"`
}

// GetAuths returns a list of all available auths for an account
// https://www.getlytics.com/developers/rest-api#auth
func (l *Client) GetAuths() ([]Auth, error) {
	res := ApiResp{}
	data := []Auth{}

	// make the request
	err := l.Get(authListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetAuth returns a single auth based on id
// https://www.getlytics.com/developers/rest-api#auth
func (l *Client) GetAuth(id string) (Auth, error) {
	res := ApiResp{}
	data := Auth{}

	// make the request
	err := l.Get(parseLyticsURL(authEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// Other Available Endpoints
// * POST    create auth
// * PUT     update auth
// * DELETE  remove auth
