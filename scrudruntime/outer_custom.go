package scrudruntime

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func CustomItemsToItemsPerItem[
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
		SetItems(v []OITP)
	},
	// input item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
	},
](
	f func(context.Context, *zap.Logger, pgx.Tx, int, IITP) (OITP, error),
) func(context.Context, *zap.Logger, pgx.Tx, IP) (OP, error) {
	return func(ctx context.Context, logs *zap.Logger, tx pgx.Tx, inp IP) (OP, error) {
		var err error
		items := make([]OITP, 0, len(inp.GetItems()))
		for idx, inItem := range inp.GetItems() {
			outItem, ferr := f(ctx, logs, tx, idx, inItem)
			err = errors.Join(err, ferr)
			items = append(items, outItem)
		}

		var op OP = new(O)
		op.SetItems(items)
		return op, err
	}
}

// NOTE: if necessary, add "CustomItemsToIDsPerItem" and possibly the other variant(s).
