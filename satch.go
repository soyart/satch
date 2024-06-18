package satch

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Config struct {
	LockWrite bool `json:"lockWrite"`
	LockRead  bool `json:"lockRead"`
}

type DataSource interface {
	LockWrite(context.Context) error // Lock the data source from writing
	LockRead(context.Context) error  // Lock the data source from reading

	Inputs(context.Context) (interface{}, error)        // Returns inputs for processing
	Commit(ctx context.Context, data interface{}) error // Commit writes
}

type Job interface {
	// Job ID for debugging only
	ID() string

	// Process inputs and returns the output to be committed
	// If you want to have side-effects that persists regardless of any error,
	// then you'd probably want to do it inside Run here
	Run(ctx context.Context, inputs interface{}, now time.Time) (output interface{}, err error)
}

// Start starts a satch job. It aborts whenever an error surfaces.
func Start(ctx context.Context, job Job, ds DataSource, conf Config) error {
	switch {
	case job == nil:
		return errors.New("job is nil")

	case ds == nil:
		return errors.New("ds is nil")
	}

	id := job.ID()
	logrus.Info("Starting job", id)

	switch {
	case conf.LockRead && conf.LockWrite:
		return fmt.Errorf("unexpected config.LockRead and config.LockWrite for job %s", id)

	case conf.LockRead:
		err := ds.LockRead(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to lock write for job %s", id)
		}

	case conf.LockWrite:
		err := ds.LockWrite(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to lock write for job %s", id)
		}
	}

	start := time.Now()

	inputs, err := ds.Inputs(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get inputs for job %s", job.ID())
	}

	results, err := job.Run(ctx, inputs, start)
	if err != nil {
		return errors.Wrapf(err, "failed to run job %s", job.ID())
	}

	err = ds.Commit(ctx, results)
	if err != nil {
		return errors.Wrapf(err, "failed to commit results from job %s", job.ID())
	}

	return nil
}
