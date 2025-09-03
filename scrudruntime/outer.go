// Package scrudruntime provides runtime code for our standard crud implementation.
package scrudruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func CreatePerItem[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetItems() []IITP
	},
	// output
	OP interface {
		*O
		proto.Message
		SetIds(ids []string)
	},
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, IITP) (string, error),
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		var err error
		ids := make([]string, 0, len(inp.GetItems()))
		for _, item := range inp.GetItems() {
			id, ferr := f(ctx, logs, tx, item)
			err = errors.Join(err, ferr)
			ids = append(ids, id)
		}

		var op OP = new(O)
		op.SetIds(ids)
		return op, err
	}
}

func appendErr(id string, err, opErr error) error {
	if opErr == nil {
		return err // nothing to join
	}

	if errors.Is(opErr, pgx.ErrNoRows) || errors.Is(opErr, sql.ErrNoRows) {
		err = errors.Join(err, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("item '%s' does not exist", id)))
	} else {
		err = errors.Join(err, opErr)
	}

	return err
}

func ModifyPerItem[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetItems() []IITP
	},
	// output
	OP interface {
		*O
		proto.Message
	},
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
		GetId() string
		GetMask() *fieldmaskpb.FieldMask
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, IITP) error,
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		var err error
		for _, item := range inp.GetItems() {
			// if the update mask is empty, it means nothing will be updated so we skip the implementation altogether.
			if len(item.GetMask().GetPaths()) < 1 {
				continue
			}

			// report invalid mask.
			if !item.GetMask().IsValid(item) {
				err = appendErr(item.GetId(), err, fmt.Errorf("invalid update mask: %v", item.GetMask().GetPaths()))
			} else {
				opErr := f(ctx, logs, tx, item)
				err = appendErr(item.GetId(), err, opErr)
			}
		}

		var op OP = new(O)
		return op, err
	}
}

func RemovePerBatch[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetIds() []string
	},
	// output
	OP interface {
		*O
		proto.Message
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, []string) error,
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		if err := f(ctx, logs, tx, inp.GetIds()); err != nil {
			return nil, err
		}

		var op OP = new(O)
		return op, nil
	}
}

func DescribePerBatch[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetIds() []string
		GetConsiderArchived() bool
	},
	// output
	OP interface {
		*O
		proto.Message
		SetItems(items []OITP)
	},
	// output item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, bool, []string) ([]OITP, error),
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		items, err := f(ctx, logs, tx, inp.GetConsiderArchived(), inp.GetIds())
		if err != nil {
			return nil, err
		}

		var op OP = new(O)
		op.SetItems(items)
		return op, err
	}
}

func ListAndDescribePerBatch[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetShowArchived() bool
	},
	// output
	OP interface {
		*O
		proto.Message
		SetItems(items []OITP)
		SetNextCursor(cursor []byte)
		SetPreviousCursor(cursor []byte)
	},
	// output item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
](
	listf func(context.Context, *zap.Logger, pgx.Tx, IP) ([]string, []byte, []byte, error),
	descf func(context.Context, *zap.Logger, pgx.Tx, bool, []string) ([]OITP, error),
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, i IP) (OP, error) {
		ids, nextCursor, previousCursor, err := listf(ctx, logs, tx, i)
		if err != nil {
			return nil, err
		}

		items, err := descf(ctx, logs, tx, i.GetShowArchived(), ids)
		if err != nil {
			return nil, err
		}

		var op OP = new(O)
		op.SetItems(items)
		op.SetNextCursor(nextCursor)
		op.SetPreviousCursor(previousCursor)
		return op, err
	}
}

func RestorePerBatch[
	I any,
	O any,
	// input
	IP interface {
		*I
		proto.Message
		GetIds() []string
	},
	// output
	OP interface {
		*O
		proto.Message
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, []string) error,
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		if err := f(ctx, logs, tx, inp.GetIds()); err != nil {
			return nil, err
		}

		var op OP = new(O)
		return op, nil
	}
}
