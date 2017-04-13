package kubernetes

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	u "github.com/araddon/gou"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/api/v1"
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
		pods, err := it.query.k.Core().Pods("").List(v1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("Could not get kubernetes pods %v", err)
		}
		u.Infof("There are %d pods in the cluster", len(pods.Items))
		for _, pod := range pods.Items {
			it.items = append(it.items, &Object{&pod, podValues(&pod)})
		}
	case "service", "services":
		services, err := it.query.k.Core().Services("").List(v1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("Could not get kubernetes services %v", err)
		}
		u.Infof("There are %d services in the cluster", len(services.Items))
		for _, svc := range services.Items {
			it.items = append(it.items, &Object{&svc, serviceValues(&svc)})
		}
	case "node", "nodes":
		nodes, err := it.query.k.Core().Nodes().List(v1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("Could not get kubernetes nodes %v", err)
		}
		u.Infof("There are %d nodes in the cluster", len(nodes.Items))
		for _, node := range nodes.Items {
			it.items = append(it.items, &Object{&node, nodeValues(&node)})
		}
	}
	return "", nil
}

func podValues(v *v1.Pod) []driver.Value {
	vals := []driver.Value{
		"pod", // kind
	}
	vals = append(vals, metadata(&v.ObjectMeta)...)
	vals = append(vals, podStatus(&v.Status)...)
	vals = append(vals, podSpec(&v.Spec)...)
	return vals
}
func serviceValues(v *v1.Service) []driver.Value {
	vals := []driver.Value{
		"service", // kind
	}
	vals = append(vals, metadata(&v.ObjectMeta)...)
	/*
		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_servicestatus
		tbl.AddField(schema.NewFieldBase("loadbalancer", value.JsonType, 256, "json object"))
		colNames = append(colNames, []string{"loadbalancer"}...)

		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_servicespec
		tbl.AddField(schema.NewFieldBase("ports", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("selector", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("clusterip", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("type", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("externalips", value.JsonType, 32, "json array"))
		tbl.AddField(schema.NewFieldBase("deprecatedpublicips", value.JsonType, 32, "json array"))
		tbl.AddField(schema.NewFieldBase("sessionaffinity", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("loadbalancerip", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("loadbalancersourceranges", value.JsonType, 32, "json array"))
		tbl.AddField(schema.NewFieldBase("externalname", value.StringType, 256, "string"))
		colNames = append(colNames, []string{"ports", "selector", "clusterip", "type",
			"externalips", "deprecatedpublicips", "sessionaffinity", "loadbalancerip",
			"loadbalancersourceranges", "externalname"}...)
	*/
	vals = append(vals, []driver.Value{
		v.Status.LoadBalancer,
		v.Spec.Ports,
		v.Spec.Selector,
		v.Spec.ClusterIP,
		v.Spec.Type,
		v.Spec.ExternalIPs,
		v.Spec.DeprecatedPublicIPs,
		v.Spec.SessionAffinity,
		v.Spec.LoadBalancerIP,
		v.Spec.LoadBalancerSourceRanges,
		v.Spec.ExternalName,
	}...)
	return vals
}
func nodeValues(v *v1.Node) []driver.Value {
	vals := []driver.Value{
		"node", // kind
	}
	vals = append(vals, metadata(&v.ObjectMeta)...)
	/*
		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_nodestatus
		tbl.AddField(schema.NewFieldBase("capacity", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("allocatable", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("phase", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("conditions", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("addresses", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("daemonendpoints", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("nodeinfo", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("images", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("volumesinuse", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("volumesattached", value.JsonType, 256, "json array"))
		colNames = append(colNames, []string{"capacity", "allocatable", "phase", "conditions",
			"addresses", "daemonendpoints", "nodeinfo", "images", "volumesinuse", "volumesattached"}...)

		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_nodespec
		tbl.AddField(schema.NewFieldBase("podcidr", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("externalid", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("providerid", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("unschedulable", value.BoolType, 1, "boolean"))

		colNames = append(colNames, []string{"podcidr", "externalid", "providerid", "unschedulable"}...)
	*/
	st := v.Status
	s := v.Spec
	vals = append(vals, []driver.Value{
		st.Capacity,
		st.Allocatable,
		st.Phase,
		st.Conditions,
		st.Addresses,
		st.DaemonEndpoints,
		st.NodeInfo,
		st.Images,
		st.VolumesInUse,
		st.VolumesAttached,
		s.PodCIDR,
		s.ExternalID,
		s.ProviderID,
		s.Unschedulable,
	}...)
	return vals
}
func metadata(p *v1.ObjectMeta) []driver.Value {
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
	return []driver.Value{
		p.Name,
		p.GenerateName,
		p.Namespace,
		p.SelfLink,
		p.UID,
		p.ResourceVersion,
		p.Generation,
		p.CreationTimestamp.Time,
		tv(p.DeletionTimestamp),
		p.Labels,
	}
}
func podStatus(p *v1.PodStatus) []driver.Value {
	/*
		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_podstatus
		tbl.AddField(schema.NewFieldBase("phase", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("conditions", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("message", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("reason", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("hostip", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("podip", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("starttime", value.TimeType, 64, "datetime"))
		tbl.AddField(schema.NewFieldBase("containerstatuses", value.JsonType, 256, "json array"))
		colNames = append(colNames, []string{"phase", "conditions", "message", "reason",
			"hostip", "podip", "starttime", "containerstatuses"}...)
	*/
	return []driver.Value{
		string(p.Phase),
		p.Conditions,
		p.Message,
		p.Reason,
		p.HostIP,
		p.PodIP,
		tv(p.StartTime),
		p.ContainerStatuses,
	}
}
func podSpec(p *v1.PodSpec) []driver.Value {
	/*
		// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_podspec
		tbl.AddField(schema.NewFieldBase("volumes", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("containers", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("restartpolicy", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("terminationgraceperiodseconds", value.IntType, 64, "long"))
		tbl.AddField(schema.NewFieldBase("activedeadlineseconds", value.IntType, 64, "long"))
		tbl.AddField(schema.NewFieldBase("dnspolicy", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("nodeselector", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("serviceaccountname", value.StringType, 256, "string"))
		//tbl.AddField(schema.NewFieldBase("serviceaccount", value.StringType, 256, "string"))
		tbl.AddField(schema.NewFieldBase("nodename", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("hostnetwork", value.BoolType, 1, "boolean"))
		tbl.AddField(schema.NewFieldBase("hostpid", value.BoolType, 1, "boolean"))
		tbl.AddField(schema.NewFieldBase("hostipc", value.BoolType, 1, "boolean"))
		tbl.AddField(schema.NewFieldBase("securitycontext", value.JsonType, 256, "json object"))
		tbl.AddField(schema.NewFieldBase("imagepullsecrets", value.JsonType, 256, "json array"))
		tbl.AddField(schema.NewFieldBase("hostname", value.StringType, 32, "string"))
		tbl.AddField(schema.NewFieldBase("subdomain", value.StringType, 32, "string"))
		colNames = append(colNames, []string{"volumes", "containers", "restartpolicy", "terminationgraceperiodseconds",
			"activedeadlineseconds", "dnspolicy", "nodeselector", "serviceaccountname",
			 "nodename", "hostnetwork", "hostpid", "hostipc",
			"securitycontext", "imagepullsecrets", "hostname", "subdomain"}...)
	*/
	return []driver.Value{
		p.Volumes,
		p.Containers,
		p.RestartPolicy,
		nni(p.TerminationGracePeriodSeconds),
		nni(p.ActiveDeadlineSeconds),
		string(p.DNSPolicy),
		p.NodeSelector,
		p.ServiceAccountName,
		p.NodeName,
		p.HostNetwork,
		p.HostPID,
		p.HostIPC,
		p.SecurityContext,
		p.ImagePullSecrets,
		p.Hostname,
		p.Subdomain,
	}
}
func nni(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
func tv(v *unversioned.Time) *time.Time {
	if v == nil {
		return nil
	}
	return &v.Time
}
func jv(v interface{}) json.RawMessage {
	by, _ := json.Marshal(v)
	return json.RawMessage(by)
}
