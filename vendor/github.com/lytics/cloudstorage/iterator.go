package cloudstorage

import (
	"math"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

// ObjectsAll get all objects for an iterator.
func ObjectsAll(iter ObjectIterator) (Objects, error) {
	objs := make(Objects, 0)
	for {
		o, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		objs = append(objs, o)
	}
	return objs, nil
}

// ObjectResponseFromIter get all objects for an iterator.
func ObjectResponseFromIter(iter ObjectIterator) (*ObjectsResponse, error) {
	objs, err := ObjectsAll(iter)
	if err != nil {
		return nil, err
	}
	return &ObjectsResponse{Objects: objs}, nil
}

// ObjectPageIterator iterator to facilitate easy paging through store.List() method
// to read all Objects that matched query.
type ObjectPageIterator struct {
	s      Store
	ctx    context.Context
	cancel context.CancelFunc
	q      Query
	cursor int
	page   Objects
}

// NewObjectPageIterator create an iterator that wraps the store List interface.
func NewObjectPageIterator(ctx context.Context, s Store, q Query) ObjectIterator {

	cancelCtx, cancel := context.WithCancel(ctx)
	return &ObjectPageIterator{
		s:      s,
		ctx:    cancelCtx,
		cancel: cancel,
		q:      q,
	}
}
func (it *ObjectPageIterator) returnPageNext() (Object, error) {
	it.cursor++
	return it.page[it.cursor-1], nil
}

// Close the object iterator.
func (it *ObjectPageIterator) Close() {
	defer func() { recover() }()
	select {
	case <-it.ctx.Done():
		// done
	default:
		it.cancel()
	}
}

// Next iterator to go to next object or else returns error for done.
func (it *ObjectPageIterator) Next() (Object, error) {
	retryCt := 0

	select {
	case <-it.ctx.Done():
		// If iterator has been closed
		return nil, it.ctx.Err()
	default:
		if it.cursor < len(it.page) {
			return it.returnPageNext()
		} else if it.cursor > 0 && it.q.Marker == "" {
			// no new page, lets return
			return nil, iterator.Done
		}
		for {
			resp, err := it.s.List(it.ctx, it.q)
			if err == nil {
				it.page = resp.Objects
				it.cursor = 0
				it.q.Marker = resp.NextMarker
				if len(it.page) == 0 {
					return nil, iterator.Done
				}
				return it.returnPageNext()
			} else if err == iterator.Done {
				return nil, err
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				// Return to user
				return nil, err
			}
			if retryCt < 5 {
				Backoff(retryCt)
			} else {
				return nil, err
			}
			retryCt++
		}
	}
}

// Backoff sleeps a random amount so we can.
// retry failed requests using a randomized exponential backoff:
// wait a random period between [0..1] seconds and retry; if that fails,
// wait a random period between [0..2] seconds and retry; if that fails,
// wait a random period between [0..4] seconds and retry, and so on,
// with an upper bounds to the wait period being 16 seconds.
// http://play.golang.org/p/l9aUHgiR8J
func Backoff(try int) {
	nf := math.Pow(2, float64(try))
	nf = math.Max(1, nf)
	nf = math.Min(nf, 16)
	r := rand.Int31n(int32(nf))
	d := time.Duration(r) * time.Second
	time.Sleep(d)
}
