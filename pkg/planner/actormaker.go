package planner

import (
	"fmt"

	"github.com/lytics/grid/grid2"
)

var (
	_ grid2.ActorMaker = (*maker)(nil)
	_ grid2.ActorMaker = (*nilMaker)(nil)
)

type maker struct {
	conf *Conf
}

func newActorMaker(conf *Conf) (*maker, error) {
	if conf.NrProducers > 1024 {
		return nil, fmt.Errorf("to many producer actors requested: %v", conf.NrProducers)
	}
	if conf.NrConsumers > 1024 {
		return nil, fmt.Errorf("to many consumer actors requested: %v", conf.NrConsumers)
	}
	return &maker{conf: conf}, nil
}

func (m *maker) MakeActor(def *grid2.ActorDef) (grid2.Actor, error) {
	switch def.Type {
	case "leader":
		return NewLeaderActor(def, m.conf), nil
	case "producer":
		return NewProducerActor(def, m.conf), nil
	case "consumer":
		return NewConsumerActor(def, m.conf), nil
	default:
		return nil, fmt.Errorf("type does not map to any type of actor: %v", def.Type)
	}
}

type nilMaker struct {
}

func (m *nilMaker) MakeActor(def *grid2.ActorDef) (grid2.Actor, error) {
	return nil, fmt.Errorf("NilMaker does not run actors it is lazy: %v", def.Type)
}