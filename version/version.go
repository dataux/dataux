package version

import ()

// http://stackoverflow.com/questions/11354518/golang-application-auto-build-versioning

// go clean && go install \
//  -ldflags "-X github.com/dataux/dataux/version.Version=${version} -X github.com/dataux/dataux/version.PublicVersion=${VERSIONPUBLIC}"

// Version will be the latest tag + number of commits after the tag
var Version = "unset"

// VersionPublic is just the hash from the latest commit.
var VersionPublic = ""
