package v1_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	scrudv1 "github.com/advdv/scrud/scrud/v1"
	"github.com/stretchr/testify/require"
)

func TestCursorInit(t *testing.T) {
	t.Parallel()

	// Fixed values (no monotonic clock bits) so they survive round‑tripping.
	when := time.Date(2025, 7, 24, 12, 0, 0, 0, time.UTC)
	span := 5 * time.Minute

	for idx, tt := range []struct {
		v any
		e error
	}{
		{int64(100), nil},
		{int32(42), nil},
		{uint32(1_000), nil},
		{uint64(10_000), nil},
		{float32(3.14), nil},
		{float64(2.718281828), nil},
		{true, nil},
		{"hello", nil},
		{[]byte{0xde, 0xad, 0xbe, 0xef}, nil},
		{when, nil},
		{span, nil},
		// negative case
		{struct{}{}, errors.New("unsupported order value: {} (struct {})")},
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			t.Parallel()

			cur, err := scrudv1.NewCursor("foo_1", tt.v, true)

			if tt.e != nil {
				require.EqualError(t, err, tt.e.Error())
				require.Nil(t, cur)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cur)
			require.True(t, cur.GetIsBackwards())

			got := cur.OrderValue()
			require.Equal(t, tt.v, got)
		})
	}
}

func TestCursorOrderValuePanic(t *testing.T) {
	t.Parallel()

	id := "foo_1"

	cur := scrudv1.Cursor_builder{PrimaryId: &id}.Build()
	require.NotNil(t, cur)
	require.PanicsWithValue(t, `unsupported or unset order_value in cursor: primary_id:"`+id+`"`, func() {
		_ = cur.OrderValue() // in this state, OrderValue must panic
	})
}
