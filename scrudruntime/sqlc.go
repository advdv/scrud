package scrudruntime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
)

func IsOneNotFound(actualIDs, expectedIDs []string) error {
	if len(actualIDs) != len(expectedIDs) {
		var notFoundIDs []string
		for _, id := range expectedIDs {
			if slices.Contains(actualIDs, id) {
				continue
			}

			notFoundIDs = append(notFoundIDs, id)
		}

		return connect.NewError(connect.CodeNotFound,
			fmt.Errorf("could not find id(s): %s", strings.Join(notFoundIDs, ",")))
	}

	return nil
}

func RowsToItems[
	// slice of rows.
	Ts ~[]T,
	T interface {
		GetID() string
		GetArchivedAt() *time.Time
	},
	// item type
	IT any,
](
	ctx context.Context,
	tx pgx.Tx,
	ids []string,
	rows Ts,
	considerArchived bool, // wether archived rows should be considered, else they'll cause a not_found error.
	mapf func(ctx context.Context, tx pgx.Tx, row T) (IT, error),
) (items []IT, err error) {
	actualIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		// if the row is archived, and we have a request that should not consider these to exist. We don't
		// add them to the actual ids we found. That will trigger a not_found below.
		if row.GetArchivedAt() != nil && !considerArchived {
			continue
		}

		actualIDs = append(actualIDs, row.GetID())
	}

	if err := IsOneNotFound(actualIDs, ids); err != nil {
		return nil, err
	}

	// we use the original ids order as the sorting order. This is requires since
	// the "WHERE .. IN ..." sql doesn't quarnatee order. The double for loop introduces
	// non-linear complexity but we should be ok with the relatively small page sizes.
	for _, id := range ids {
		for _, row := range rows {
			if row.GetID() == id {
				mapped, merr := mapf(ctx, tx, row)
				if merr != nil {
					err = errors.Join(err, merr)
				} else {
					items = append(items, mapped)
				}
			}
		}
	}

	return items, err
}
