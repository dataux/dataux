package kubernetes

import (
	"database/sql/driver"
	"fmt"
	"time"

	u "github.com/araddon/gou"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"k8s.io/client-go/1.4/pkg/api"
	v1 "k8s.io/client-go/1.4/pkg/api/v1"
)

// Objects returns an iterator over the objects in the bucket that match the Query q.
// If q is nil, no filtering is done.
func Objects(ctx context.Context, kind string, q *SqlToKube) *ObjectIterator {
	it := &ObjectIterator{
		ctx:   ctx,
		kind:  kind,
		query: q,
	}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.fetch,
		func() int { return len(it.items) },
		func() interface{} { items := it.items; it.items = nil; return items })

	return it
}

type Object struct {
	val interface{}
	row []driver.Value
}

// An ObjectIterator is an iterator over Object which is a generic Object
// type representing a kube object.
type ObjectIterator struct {
	ctx      context.Context
	kind     string
	query    *SqlToKube
	pageInfo *iterator.PageInfo
	nextFunc func() error
	items    []*Object
}

// PageInfo supports pagination. See the google.golang.org/api/iterator package for details.
func (it *ObjectIterator) PageInfo() *iterator.PageInfo { return it.pageInfo }

// Next returns the next result. Its second return value is iterator.Done if
// there are no more results. Once Next returns iterator.Done, all subsequent
// calls will return iterator.Done.
func (it *ObjectIterator) Next() (*Object, error) {
	if err := it.nextFunc(); err != nil {
		return nil, err
	}
	item := it.items[0]
	it.items = it.items[1:]
	return item, nil
}

func (it *ObjectIterator) fetch(pageSize int, pageToken string) (string, error) {

	switch it.kind {
	case "pod", "pods":
		pods, err := it.query.k.Core().Pods("").List(api.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("Could not get kubernetes pods %v", err)
		}
		u.Infof("There are %d pods in the cluster", len(pods.Items))
		for _, pod := range pods.Items {
			it.items = append(it.items, &Object{&pod, podValues(&pod)})
		}
	}
	return "", nil
}

func podValues(p *v1.Pod) []driver.Value {
	/*
		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_objectmeta
		tbl.AddField(schema.NewFieldBase("name", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("generateName", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("namespace", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("selfLink", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("uid", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("resourceVersion", value.StringType, 8, "string"))
		tbl.AddField(schema.NewFieldBase("generation", value.IntType, 64, "long"))
		tbl.AddField(schema.NewFieldBase("creationTimestamp", value.TimeType, 32, "datetime"))
		tbl.AddField(schema.NewFieldBase("deletionTimestamp", value.TimeType, 32, "datetime"))
		tbl.AddField(schema.NewFieldBase("labels", value.JsonType, 256, "object"))
		colNames = append(colNames, []string{"name", "generateName", "namespace", "selfLink",
			"uid", "selfLink", "generation", "creationTimestamp", "deletionTimestamp", "labels"}...)
	*/
	dt := time.Time{}
	if p.DeletionTimestamp != nil {
		dt = p.DeletionTimestamp.Time
	}
	return []driver.Value{
		"pod", // kind
		p.Name,
		p.GenerateName,
		p.Namespace,
		p.SelfLink,
		p.UID,
		p.ResourceVersion,
		p.Generation,
		p.CreationTimestamp.Time,
		dt,
		p.Labels,
	}
}
