package lytics

const (
	providerEndpoint     = "provider/:id"
	providerListEndpoint = "provider"
)

type Provider struct {
	Id              string                 `json:"id"`
	Slug            string                 `json:"slug"`
	Hidden          bool                   `json:"hidden"`
	Name            string                 `json:"name"`
	Namespace       string                 `json:"namespace,omitempty"`
	Description     string                 `json:"description"`
	Categories      []string               `json:"categories"`
	AuthMethod      string                 `json:"auth_method,omitempty"`
	AuthDescription string                 `json:"auth_description"`
	NoAuth          bool                   `json:"no_auth"`
	Extra           map[string]interface{} `json:"extra,omitempty"`
}

// GetProviders returns a list of all providers (mailchimp, campaign monitor, etc)
// https://www.getlytics.com/developers/rest-api#provider-list
func (l *Client) GetProviders() ([]Provider, error) {
	res := ApiResp{}
	data := []Provider{}

	// make the request
	err := l.Get(providerListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetProvider returns a single provider based on id
// https://www.getlytics.com/developers/rest-api#provider
func (l *Client) GetProvider(id string) (Provider, error) {
	res := ApiResp{}
	data := Provider{}

	// make the request
	err := l.Get(parseLyticsURL(providerEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
