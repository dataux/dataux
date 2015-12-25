package backends

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY

	// Standard errors
	ErrNotSupported     = fmt.Errorf("DataUX: Not supported")
	ErrNotImplemented   = fmt.Errorf("DataUX: Not implemented")
	ErrUnknownCommand   = fmt.Errorf("DataUX: Unknown Command")
	ErrInternalError    = fmt.Errorf("DataUX: Internal Error")
	ErrNoSchemaSelected = fmt.Errorf("No Schema Selected")
)

const (
	MaxAllowedPacket = 1024 * 1024
)

// Create Job made up of sub-tasks in DAG that is the plan for execution of this query/job
//
// Note: this doesn't really do anything but does provide an intercept
//   point for dataux to do its own planning on top of qlbridge
func BuildSqlJob(svr *models.ServerCtx, ctx *plan.Context) (*exec.SqlJob, error) {
	return exec.BuildSqlProjectedJob(ctx)
}
