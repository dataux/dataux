package google

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	googleOauth2 "golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/option"

	"github.com/lytics/cloudstorage"
)

const (
	// Authentication Source's

	// AuthJWTKeySource is for a complete string representing json of JWT
	AuthJWTKeySource cloudstorage.AuthMethod = "LyticsJWTkey"
	// AuthGoogleJWTKeySource is a string representing path to a file of JWT
	AuthGoogleJWTKeySource cloudstorage.AuthMethod = "GoogleJWTFile"
	// AuthGCEMetaKeySource is flag saying to use gcemetadata
	AuthGCEMetaKeySource cloudstorage.AuthMethod = "gcemetadata"
	// AuthGCEDefaultOAuthToken means use local auth where it (google client)
	// checks variety of locations for local auth tokens.
	AuthGCEDefaultOAuthToken cloudstorage.AuthMethod = "gcedefaulttoken"
)

// GoogleOAuthClient An interface so we can return any of the
// 3 Google transporter wrapper as a single interface.
type GoogleOAuthClient interface {
	Client() *http.Client
}
type gOAuthClient struct {
	httpclient *http.Client
}

func (g *gOAuthClient) Client() *http.Client {
	return g.httpclient
}

func gcsCommonClient(client *http.Client, conf *cloudstorage.Config) (cloudstorage.Store, error) {
	gcs, err := storage.NewClient(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}
	store, err := NewGCSStore(gcs, conf.Bucket, conf.TmpDir, cloudstorage.MaxResults)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// BuildGoogleJWTTransporter create a GoogleOAuthClient from jwt config.
func BuildGoogleJWTTransporter(jwtConf *cloudstorage.JwtConf) (GoogleOAuthClient, error) {
	key, err := jwtConf.KeyBytes()
	if err != nil {
		return nil, err
	}

	conf := &jwt.Config{
		Email:      jwtConf.ClientEmail,
		PrivateKey: key,
		Scopes:     jwtConf.Scopes,
		TokenURL:   googleOauth2.JWTTokenURL,
	}

	client := conf.Client(oauth2.NoContext)

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

// BuildGoogleFileJWTTransporter Build a Google Storage Client from a path to
// a json file that has JWT.
func BuildGoogleFileJWTTransporter(keyPath string, scope string) (GoogleOAuthClient, error) {
	jsonKey, err := ioutil.ReadFile(os.ExpandEnv(keyPath))
	if err != nil {
		return nil, err
	}

	conf, err := googleOauth2.JWTConfigFromJSON(jsonKey, scope)
	if err != nil {
		return nil, err
	}

	client := conf.Client(oauth2.NoContext)

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

/*
   The account may be empty or the string "default" to use the instance's main account.
*/
func BuildGCEMetadatTransporter(serviceAccount string) (GoogleOAuthClient, error) {
	client := &http.Client{
		Transport: &oauth2.Transport{

			Source: googleOauth2.ComputeTokenSource(""),
		},
	}

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

// BuildDefaultGoogleTransporter builds a transpoter that wraps the google DefaultClient:
//    Ref https://github.com/golang/oauth2/blob/master/google/default.go#L33
// DefaultClient returns an HTTP Client that uses the
// DefaultTokenSource to obtain authentication credentials
//    Ref : https://github.com/golang/oauth2/blob/master/google/default.go#L41
// DefaultTokenSource is a token source that uses
// "Application Default Credentials".
//
// It looks for credentials in the following places,
// preferring the first location found:
//
//   1. A JSON file whose path is specified by the
//      GOOGLE_APPLICATION_CREDENTIALS environment variable.
//   2. A JSON file in a location known to the gcloud command-line tool.
//      On other systems, $HOME/.config/gcloud/credentials.
//   3. On Google App Engine it uses the appengine.AccessToken function.
//   4. On Google Compute Engine, it fetches credentials from the metadata server.
//      (In this final case any provided scopes are ignored.)
//
// For more details, see:
// https://developers.google.com/accounts/docs/application-default-credentials
//
// Samples of possible scopes:
// Google Cloud Storage : https://github.com/GoogleCloudPlatform/gcloud-golang/blob/69098363d921fa3cf80f930468a41a33edd9ccb9/storage/storage.go#L51
// BigQuery             :  https://github.com/GoogleCloudPlatform/gcloud-golang/blob/522a8ceb4bb83c2def27baccf31d646bce11a4b2/bigquery/bigquery.go#L52
func BuildDefaultGoogleTransporter(scope ...string) (GoogleOAuthClient, error) {

	client, err := googleOauth2.DefaultClient(context.Background(), scope...)
	if err != nil {
		return nil, err
	}

	return &gOAuthClient{
		httpclient: client,
	}, nil
}

// NewGoogleClient create new Google Storage Client.
func NewGoogleClient(conf *cloudstorage.Config) (client GoogleOAuthClient, err error) {

	switch conf.AuthMethod {
	case AuthGCEDefaultOAuthToken:
		// This token method uses the default OAuth token with GCS created by tools like gsutils, gcloud, etc...
		// See github.com/lytics/lio/src/ext_svcs/google/google_transporter.go : BuildDefaultGoogleTransporter
		client, err = BuildDefaultGoogleTransporter("")
		if err != nil {
			return nil, err
		}
	case AuthGCEMetaKeySource:
		client, err = BuildGCEMetadatTransporter("")
		if err != nil {
			return nil, err
		}
	case AuthJWTKeySource:
		// used if you are providing string of json
		client, err = BuildGoogleJWTTransporter(conf.JwtConf)
		if err != nil {
			return nil, err
		}
	case AuthGoogleJWTKeySource:
		client, err = BuildGoogleFileJWTTransporter(conf.JwtFile, conf.Scope)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("bad AuthMethod: %v", conf.AuthMethod)
	}

	return client, err
}
