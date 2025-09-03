package scrudruntime

import (
	"fmt"
	"slices"

	"connectrpc.com/connect"
	scrudv1 "github.com/advdv/scrud/scrud/v1"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"google.golang.org/protobuf/proto"
)

// PaginateSelectMods will setup a bob query mode for generic cursor-based pagination via maps.
//
//nolint:gocognit
func PaginateSelectMods[
	// request's input message
	I interface {
		HasSortBy() bool
		GetSortBy() string
		GetSortDesc() bool
		HasPerPage() bool
		GetPerPage() int32
		HasCursor() bool
		GetCursor() []byte
		GetShowArchived() bool
	},
](
	inp I,
	baseTableName string,
) ([]bob.Mod[*dialect.SelectQuery], func(rows []map[string]any) ([]string, []byte, []byte, error), error) {
	// determine ordering column, direction and page size.
	sortCol := "created_at"
	if inp.HasSortBy() {
		sortCol = inp.GetSortBy()
	}

	desc := inp.GetSortDesc() // NOTE: may be flipped later
	pageSize := int32(100)
	if inp.HasPerPage() {
		pageSize = inp.GetPerPage()
	}

	// Decode the incoming cursor (if any)
	var sortValue any  // sorting value of hte cursor row
	var sortID string  // id of cursor row
	var backwards bool // true means “scan *before* anchor”
	if inp.HasCursor() {
		c, err := decodeCursor(inp.GetCursor())
		if err != nil {
			return nil, nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("decode cursor: %w", err))
		}
		sortValue, sortID = c.OrderValue(), c.GetPrimaryId()
		backwards = c.GetIsBackwards()
		if backwards {
			desc = !desc // invert the SQL ORDER BY direction
		}
	}

	// Build the (base) query, optionally add cursor's where clause.
	mods := []bob.Mod[*dialect.SelectQuery]{
		sm.Columns("id", sortCol),
		orderBy(sortCol, desc),
		orderBy("id", desc),    // always tie‑break on pk
		sm.Limit(pageSize + 1), // +1 sentinel row
	}
	if sortValue != nil {
		// (<sortCol,id) > (<anchorValue,anchorID>)  or  <
		lhs := psql.Group(psql.Quote(sortCol), psql.Quote("id"))
		rhs := psql.ArgGroup(sortValue, sortID)
		if desc {
			mods = append(mods, sm.Where(lhs.LT(rhs)))
		} else {
			mods = append(mods, sm.Where(lhs.GT(rhs)))
		}
	}

	// Show either the live rows, or the archived rows.
	if inp.GetShowArchived() {
		mods = append(mods, sm.From(baseTableName+"_archived"))
	} else {
		mods = append(mods, sm.From(baseTableName+"_live"))
	}

	return mods, func(rows []map[string]any) (ids []string, nextCursor []byte, prevCursor []byte, err error) {
		// We added a sential row to check for more. Discard it for the rest of the processing.
		hasMore := len(rows) > int(pageSize)
		if hasMore {
			rows = rows[:pageSize] // discard sentinel row
		}

		// If we fetched “backwards”, put the slice in natural order for the
		// client.  The cursors are built *before* we reverse so we can still
		// access first & last in SQL order.
		isFirstPage := !inp.HasCursor() // true only for the very first request
		if len(rows) > 0 {              //nolint:nestif
			first := rows[0]
			last := rows[len(rows)-1]

			// nextCursor ⇢ rows after “last in client order”
			// prevCursor ⇢ rows before “first in client order”
			if backwards {
				// we walked BACKWARDS so:
				//   • a *previous* page exists if hasMore
				//   • a *next*  page always exists (client can go forward again)
				nextCursor, err = mapEncodeCursor(first, sortCol, false) // forward
				if err != nil {
					return nil, nil, nil, fmt.Errorf("encode next cursor: %w", err)
				}

				if hasMore {
					prevCursor, err = mapEncodeCursor(last, sortCol, true)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("encode prev cursor: %w", err)
					}
				}
				slices.Reverse(rows)
			} else {
				// we walked FORWARS so:
				//   • a *next* page exists if hasMore
				//   • a *previous* page always exists once we have any row
				if hasMore {
					nextCursor, err = mapEncodeCursor(last, sortCol, false)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("encode next cursor: %w", err)
					}
				}
				if !isFirstPage {
					prevCursor, err = mapEncodeCursor(first, sortCol, true)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("encode prev cursor: %w", err)
					}
				}
			}
		}

		// Finally, turn them into ids to fit the contract.
		ids = make([]string, len(rows))
		for i, r := range rows {
			ids[i], err = getRowID(r)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("get final row ids: %w", err)
			}
		}

		return ids, nextCursor, prevCursor, nil
	}, nil
}

func getRowID(row map[string]any) (string, error) {
	v, ok := row["id"].(string)
	if !ok {
		return "", fmt.Errorf("row map has no <string> value for 'id' column, got: %T", row["id"])
	}

	return v, nil
}

func mapEncodeCursor(row map[string]any, sortCol string, backwards bool) ([]byte, error) {
	val, ok := row[sortCol]
	if !ok {
		return nil, fmt.Errorf("row map has no value for column: %s", sortCol)
	}

	id, err := getRowID(row)
	if err != nil {
		return nil, fmt.Errorf("get row id: %w", err)
	}

	c, err := scrudv1.NewCursor(id, val, backwards)
	if err != nil {
		return nil, fmt.Errorf("init cursor: %w", err)
	}

	buf, err := proto.Marshal(c) // errors impossible for supported types
	if err != nil {
		return nil, fmt.Errorf("marshal cursor: %w", err)
	}

	return buf, nil
}

func orderBy(col string, desc bool) bob.Mod[*dialect.SelectQuery] {
	if desc {
		return sm.OrderBy(col).Desc()
	}
	return sm.OrderBy(col)
}

func decodeCursor(buf []byte) (*scrudv1.Cursor, error) {
	var c scrudv1.Cursor
	if err := proto.Unmarshal(buf, &c); err != nil {
		return nil, fmt.Errorf("unmarshal cursor: %w", err)
	}
	return &c, nil
}
