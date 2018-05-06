package lytics

import (
	"time"
)

const (
	userEndpoint     = "user/:id"
	userListEndpoint = "user"
)

type User struct {
	Id        string        `json:"id"`
	Accounts  []UserAccount `json:"accounts"`
	Email     string        `json:"email"`
	Name      string        `json:"name"`
	AuthType  string        `json:"auth_type"`
	Password  string        `json:"password"`
	Created   time.Time     `json:"created"`
	Updated   time.Time     `json:"updated"`
	LastLogon time.Time     `json:"last_logon"`
	Aid       int           `json:"aid"`
	AccountId string        `json:"account_id"`
	Roles     []string      `json:"roles"`
}

type UserAccount struct {
	Aid       int       `json:"aid"`
	AccountId string    `json:"account_id"`
	Roles     []string  `json:"roles"`
	UserId    string    `json:"granted_by"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
}

// GetUser returns a single user
// https://www.getlytics.com/developers/rest-api#user
func (l *Client) GetUser(id string) (User, error) {
	res := ApiResp{}
	data := User{}

	// make the request
	err := l.Get(parseLyticsURL(userEndpoint, map[string]string{"id": id}), nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// GetUsers returns a list of all users
// https://www.getlytics.com/developers/rest-api#user-list
func (l *Client) GetUsers() ([]User, error) {
	res := ApiResp{}
	data := []User{}

	// make the request
	err := l.Get(userListEndpoint, nil, nil, &res, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

// Other Available Endpoints
// * POST    create user
// * PUT     update user
// * DELETE  remove user
// * POST    set new password for user
