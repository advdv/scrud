package scrudtest

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// we put an upper limit to the number of page iterations we'll do before considering it
// a failure. To prevent deadlocks in testing.
const maxPagingIters = 1000

func TestListOrganizationChanges[
	// output item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetPerPage(v int32)
		SetOrganizationId(id string)
		SetChangeRecordIds(ids []string)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
		GetItems() []OITP
	},
](
	ctx context.Context,
	tb testing.TB,
	organizationID string,
	changeRecordIDs []string,
	assert func(items []OITP),
	list func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) {
	tb.Helper()

	var inp IP = new(I)
	inp.SetPerPage(100) // skip paging, use the list tester for that
	inp.SetOrganizationId(organizationID)
	inp.SetChangeRecordIds(changeRecordIDs)

	resp, err := list(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	var outp OP = resp.Msg
	assert(outp.GetItems())
}

func TestList[
	// output item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetPerPage(v int32)
		SetSortBy(v string)
		SetSortDesc(v bool)
		SetCursor(v []byte)
		SetShowArchived(v bool)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
		GetItems() []OITP
		GetNextCursor() []byte
		GetPreviousCursor() []byte
	},
](
	ctx context.Context,
	tb testing.TB,
	perPage int32,
	sortByColumn string,
	sortDesc bool,
	organizationID string, // optional
	showArchived bool, // wether to list the archived rows
	assert func(items []OITP),
	list func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) []OITP {
	var nextCursor, prevCursor []byte
	var forwardItems, backwardsItems []OITP
	var lastBatchOfItems []OITP
	// in the list request message supports setting an organization id (and one is provided, do so)
	possiblySetOrganizationID := func(inp any) {
		if inps, ok := inp.(interface{ SetOrganizationId(v string) }); ok && organizationID != "" {
			inps.SetOrganizationId(organizationID)
		}
	}

	//
	// walk all the way forwards
	//

	for idx := range maxPagingIters {
		if idx == maxPagingIters-1 {
			tb.Fatalf("too many paging iterations: %d", idx+1)
		}

		var inp IP = new(I)
		inp.SetPerPage(perPage)
		inp.SetSortBy(sortByColumn)
		inp.SetSortDesc(sortDesc)
		inp.SetShowArchived(showArchived)
		if len(nextCursor) > 0 {
			inp.SetCursor(nextCursor)
		}

		possiblySetOrganizationID(inp)
		resp, err := list(ctx, connect.NewRequest(inp))
		require.NoError(tb, err)

		var outp OP = resp.Msg
		require.LessOrEqual(tb, len(outp.GetItems()), int(perPage))
		forwardItems = append(forwardItems, outp.GetItems()...)
		if len(outp.GetPreviousCursor()) > 0 {
			prevCursor = outp.GetPreviousCursor()
		}

		// check that the first page, without any cursor. Does not return
		// a previous cursor. That is confusing.
		if idx == 0 {
			require.Empty(tb, outp.GetPreviousCursor())
		}

		// keep the last batch so we can compare it.
		if len(outp.GetNextCursor()) < 1 {
			lastBatchOfItems = outp.GetItems()
			break
		}

		require.NotEmpty(tb, outp.GetNextCursor())
		nextCursor = outp.GetNextCursor()
	}

	// If there is no prevcursor after walking forward. It means there was only one page. In that
	// case it really doesn't make sense to walk backwards and we just assert the forward items.
	if len(prevCursor) < 1 {
		assert(forwardItems)
		return forwardItems
	}

	//
	// Walk one page back
	//

	var inp IP = new(I)
	inp.SetPerPage(perPage)
	inp.SetSortBy(sortByColumn)
	inp.SetSortDesc(sortDesc)
	inp.SetShowArchived(showArchived)
	inp.SetCursor(prevCursor)
	possiblySetOrganizationID(inp)
	resp, err := list(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	var outp OP = resp.Msg
	require.Len(tb, outp.GetItems(), int(perPage))
	require.NotNil(tb, outp.GetNextCursor())
	require.NotNil(tb, outp.GetPreviousCursor())

	// Now that we're one page back, the NextCursor of this page should be equal to
	// the last next cursor. We saw it last when walking forward.
	require.Equal(tb, nextCursor, outp.GetNextCursor())

	//
	// Walk one page forward
	//

	inp = new(I)
	inp.SetPerPage(perPage)
	inp.SetSortBy(sortByColumn)
	inp.SetSortDesc(sortDesc)
	inp.SetShowArchived(showArchived)
	inp.SetCursor(outp.GetNextCursor())
	possiblySetOrganizationID(inp)
	resp, err = list(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)
	outp = resp.Msg

	// We should have the same batch of items at the end when paging forwards.
	require.Equal(tb, lastBatchOfItems, outp.GetItems())
	// we're back at the end, so no "next" cursor.
	require.Empty(tb, outp.GetNextCursor())

	// consider this part of the first backwards items
	backwardsItems = outp.GetItems()

	//
	// Walk all the way back
	//

	// walk all the way backwards, start at the last page's prev cursor.
	prevCursor = outp.GetPreviousCursor()
	for idx := range maxPagingIters {
		if idx == maxPagingIters-1 {
			tb.Fatalf("too many paging iterations: %d", idx+1)
		}

		var inp IP = new(I)
		inp.SetPerPage(perPage)
		inp.SetSortBy(sortByColumn)
		inp.SetSortDesc(sortDesc)
		inp.SetShowArchived(showArchived)
		if len(prevCursor) > 0 {
			inp.SetCursor(prevCursor)
		}

		possiblySetOrganizationID(inp)
		resp, err := list(ctx, connect.NewRequest(inp))
		require.NoError(tb, err)
		var outp OP = resp.Msg
		require.LessOrEqual(tb, len(outp.GetItems()), int(perPage))

		// since we didn't start at the end. We can assert that all next cursors will never be empty.
		require.NotEmpty(tb, outp.GetNextCursor(), idx)

		backwardsItems = append(outp.GetItems(), backwardsItems...) // NOTE: prepend!
		prevCursor = outp.GetPreviousCursor()
		if len(prevCursor) < 1 {
			break
		}
	}

	assert(backwardsItems)
	assert(forwardItems)

	return forwardItems
}

func TestCreate[
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetItems(v []IITP)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
		GetIds() []string
	},
](
	ctx context.Context,
	tb testing.TB,
	num int,
	gen func(idx int) IITP,
	create func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) []string {
	tb.Helper()
	items := make([]IITP, 0, num)
	for i := range num {
		items = append(items, gen(i))
	}

	var inp IP = new(I)
	inp.SetItems(items)

	resp, err := create(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	var outp OP = resp.Msg
	ids := outp.GetIds()
	require.Len(tb, ids, num)

	return ids
}

func TestCreateItems[
	OIT any,
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
	},
	// output item
	OITP interface {
		*OIT
		proto.Message
		GetId() string
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetItems(v []IITP)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
		GetItems() []OITP
	},
](
	ctx context.Context,
	tb testing.TB,
	num int,
	gen func(idx int) IITP,
	create func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) (ids []string) {
	tb.Helper()
	items := make([]IITP, 0, num)
	for i := range num {
		items = append(items, gen(i))
	}

	var inp IP = new(I)
	inp.SetItems(items)

	resp, err := create(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	var outp OP = resp.Msg
	for _, item := range outp.GetItems() {
		ids = append(ids, item.GetId())
	}

	require.Len(tb, ids, num)
	return ids
}

func TestModify[
	// input item
	IIT any,
	IITP interface {
		*IIT
		proto.Message
		SetId(v string)
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetItems(v []IITP)
	},
	// output
	O any,
](
	ctx context.Context,
	tb testing.TB,
	ids []string,
	gen func(idx int, id string) IITP,
	modify func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) {
	tb.Helper()

	items := make([]IITP, 0, len(ids))
	for idx, id := range ids {
		items = append(items, gen(idx, id))
	}

	var inp IP = new(I)
	inp.SetItems(items)

	_, err := modify(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	// test that not_found state works as expected.
	for _, item := range items {
		item.SetId("org_ffffffffffffffffffffffffff")
	}

	inp = new(I)
	inp.SetItems(items)
	_, err = modify(ctx, connect.NewRequest(inp))
	require.ErrorContains(tb, err, "not_found", "should error not_found")
}

func TestDescribe[
	// output item
	OIT any,
	OITP interface {
		*OIT
		proto.Message
	},
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetIds(ids []string)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
		GetItems() []OITP
	},
](
	ctx context.Context,
	tb testing.TB,
	ids []string,
	assert func(idx int, item OITP),
	describe func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) {
	tb.Helper()

	var inp IP = new(I)
	inp.SetIds(ids)
	resp, err := describe(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	var outp OP = resp.Msg
	require.Len(tb, outp.GetItems(), len(ids))

	for idx, item := range outp.GetItems() {
		assert(idx, item)
	}

	// test that not_found state works as expected.
	inp = new(I)
	inp.SetIds([]string{"org_ffffffffffffffffffffffffff"})
	_, err = describe(ctx, connect.NewRequest(inp))
	require.ErrorContains(tb, err, "not_found", "should error not_found")
}

func TestRemove[
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetIds(ids []string)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
	},
](
	ctx context.Context,
	tb testing.TB,
	ids []string,
	choose func(idx int, id string) bool,
	remove func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) []string {
	tb.Helper()

	var chosen []string
	for idx, id := range ids {
		if choose(idx, id) {
			chosen = append(chosen, id)
		}
	}

	var inp IP = new(I)
	inp.SetIds(chosen)
	_, err := remove(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	// test that not_found state works as expected.
	inp = new(I)
	inp.SetIds([]string{"org_ffffffffffffffffffffffffff"})
	_, err = remove(ctx, connect.NewRequest(inp))
	require.ErrorContains(tb, err, "not_found", "should error not_found")

	return chosen
}

func TestRestore[
	// input
	I any,
	IP interface {
		*I
		proto.Message
		SetIds(ids []string)
	},
	// output
	O any,
	OP interface {
		*O
		proto.Message
	},
](
	ctx context.Context,
	tb testing.TB,
	ids []string,
	choose func(idx int, id string) bool,
	remove func(
		context.Context,
		*connect.Request[I],
	) (*connect.Response[O], error),
) []string {
	tb.Helper()

	var chosen []string
	for idx, id := range ids {
		if choose(idx, id) {
			chosen = append(chosen, id)
		}
	}

	var inp IP = new(I)
	inp.SetIds(chosen)
	_, err := remove(ctx, connect.NewRequest(inp))
	require.NoError(tb, err)

	// test that not_found state works as expected.
	inp = new(I)
	inp.SetIds([]string{"org_ffffffffffffffffffffffffff"})
	_, err = remove(ctx, connect.NewRequest(inp))
	require.ErrorContains(tb, err, "not_found", "should error not_found")

	return chosen
}
