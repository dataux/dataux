package planner

import (
	"fmt"

	"github.com/lytics/grid"
)

var (
	_ grid.ActorMaker = (*maker)(nil)
	_ grid.ActorMaker = (*nilMaker)(nil)
)

type maker struct {
	conf *Conf
}

func newActorMaker(conf *Conf) (*maker, error) {
	return &maker{conf: conf}, nil
}

func (m *maker) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	switch def.Type {
	case "leader":
		return NewSqlActor(def, m.conf), nil
	default:
		return nil, fmt.Errorf("type does not map to any type of actor: %v", def.Type)
	}
}

type nilMaker struct {
}

func (m *nilMaker) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	return nil, fmt.Errorf("NilMaker does not run actors it is lazy: %v", def.Type)
}
