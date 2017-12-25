package planner

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	u "github.com/araddon/gou"
	td "github.com/araddon/qlbridge/datasource/mockcsvtestdata"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid"
	"github.com/stretchr/testify/assert"
)

var _ = u.EMPTY

func init() {
	td.LoadTestDataOnce()
	u.SetupLogging("debug")
	u.SetColorOutput()
}

type emptyActor struct {
	ready  chan bool
	server *grid.Server
	name   string
}

func (a *emptyActor) Act(c context.Context) {
	a.name, _ = grid.ContextActorName(c)
	id, _ := grid.ContextActorID(c)
	u.Debugf("actor started name=%q  id=%q", a.name, id)
	a.ready <- true
	<-c.Done()
}

func TestSink(t *testing.T) {

	etcdc, server, client := bootstrapTest(t)
	assert.NotEqual(t, nil, etcdc)
	assert.NotEqual(t, nil, server)
	assert.NotEqual(t, nil, client)

	ea := &emptyActor{server: server, ready: make(chan (bool), 10)}
	// Set grid definition.
	server.RegisterDef("worker", func(_ []byte) (grid.Actor, error) { return ea, nil })

	// Discover some peers.
	peers, err := client.Query(timeout, grid.Peers)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(peers))

	// Start the actor on the first peer.
	u.Debugf("%v", peers[0].Name())

	name := "worker-1"
	start := grid.NewActorStart(name)
	start.Type = "worker"

	res, err := client.Request(timeout, peers[0].Name(), start)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, res)

	planCtx := td.TestContext("select * from users")
	stmt, err := rel.ParseSql(planCtx.Raw)
	assert.Equal(t, nil, err)
	planCtx.Stmt = stmt

	planner := plan.NewPlanner(planCtx)
	plan.WalkStmt(planCtx, planCtx.Stmt, planner)

	ss := func(msg interface{}) (interface{}, error) {
		return client.Request(timeout, name, msg)
	}
	sink := NewSink(planCtx, name, ss)
	assert.NotEqual(t, nil, sink)

	// Listen to a mailbox with the same
	// name as the actor.
	mailbox, err := grid.NewMailbox(server, name, 10)
	assert.Equal(t, nil, err)
	defer mailbox.Close()

	source := NewSource(planCtx, "abc", mailbox.C)
	assert.NotEqual(t, nil, source)

	// get some test data
	conn, err := td.MockSchema.OpenConn("users")
	assert.Equal(t, nil, err)

	//iter := conn.(schema.ConnScanner)

	sel := planCtx.Stmt.(*rel.SqlSelect)
	srcPlan, err := plan.NewSource(planCtx, sel.From[0], true)
	assert.Equal(t, nil, err)
	srcPlan.Conn = conn
	dbsource, err := exec.NewSource(planCtx, srcPlan)
	assert.Equal(t, nil, err)

	outCh := make(chan schema.Message, 10)
	source.MessageOutSet(outCh)

	seq := exec.NewTaskSequential(planCtx)
	seq.Add(dbsource)
	seq.Add(sink)
	seq.Add(source)
	seq.Setup(0)

	go func() {

		msgCt := 0
		defer func() {
			assert.Equal(t, 3, msgCt)
			//u.Warnf("about to close")
			//iter.Close()
		}()
		outCh := source.MessageOut()
		for {
			msg := <-outCh
			if msg == nil {
				return
			}
			msgCt++
			u.Debugf("msg out %v", msg)
		}
	}()

	seq.Run()

	time.Sleep(time.Millisecond * 500)
}

func bootstrapTest(t testing.TB) (*clientv3.Client, *grid.Server, *grid.Client) {
	// Start etcd.
	u.Infof("etcdservers: %v", GridConf.EtcdServers)
	etcd, err := clientv3.New(clientv3.Config{Endpoints: GridConf.EtcdServers})
	if err != nil {
		u.Errorf("failed to start etcd client: %v", err)
		panic(err.Error())
	}

	var logger *log.Logger = log.New(os.Stderr, "testing: ", log.LstdFlags)

	// Create the server.
	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "testing", Logger: logger})
	assert.Equal(t, nil, err)

	// Create the listener on a random port.
	lis, err := net.Listen("tcp", "localhost:0")
	assert.Equal(t, nil, err)

	// Start the server in the background.
	done := make(chan error, 1)
	go func() {
		err = server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()
	time.Sleep(2 * time.Second)

	// Create a grid client.
	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: "testing", Logger: logger})
	assert.Equal(t, nil, err)

	return etcd, server, client
}
