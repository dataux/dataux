package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v2"
)

var (
	_ grid.ActorMaker = (*maker)(nil)
	_                 = u.EMPTY
)

type maker struct {
	conf *Conf
}

func newActorMaker(conf *Conf) (*maker, error) {
	return &maker{conf: conf}, nil
}

func (m *maker) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	switch def.Type {
	case "sqlactor":
		sa := NewSqlActor(def, m.conf)
		//u.Infof("%p starting sql actor", sa)
		return sa, nil
	default:
		return nil, fmt.Errorf("type does not map to any type of actor: %v", def.Type)
	}
}
