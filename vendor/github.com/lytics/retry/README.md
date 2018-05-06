# retry
[![GoDoc](https://godoc.org/github.com/lytics/retry?status.svg)](https://godoc.org/github.com/lytics/retry)

Retry Library for Go

```go
// Retry six times with a maximum backoff of 5 seconds
// between the retry attempts. The maximum backoff is
// reached within the third attempt, since retry uses
// exponential backoff.
var err error
retry.X(6, 5*time.Second, func() bool {
    err = DoSomething()
    return err != nil
})
if err != nil {
	// The error is not nil, so all retries failed.
} else {
	// The error is nil, so one succeeded.
}
```
