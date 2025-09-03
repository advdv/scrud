package v1

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewCursor(primaryID string, orderValue any, backwards bool) (*Cursor, error) {
	crs := Cursor_builder{PrimaryId: &primaryID, IsBackwards: &backwards}.Build()
	switch val := orderValue.(type) {
	case string:
		crs.SetOrderString(val)
	case []byte:
		crs.SetOrderBytes(val)
	case int32:
		crs.SetOrderInt32(val)
	case int64, int:
		crs.SetOrderInt64(val.(int64)) //nolint:forcetypeassert
	case uint32:
		crs.SetOrderUint32(val)
	case uint64:
		crs.SetOrderUint64(val)
	case float32:
		crs.SetOrderFloat(val)
	case float64:
		crs.SetOrderDouble(val)
	case bool:
		crs.SetOrderBool(val)
	case time.Time:
		crs.SetOrderTimestamp(timestamppb.New(val))
	case time.Duration:
		crs.SetOrderDuration(durationpb.New(val))
	default:
		return nil, fmt.Errorf("unsupported order value: %v (%T)", orderValue, orderValue)
	}

	return crs, nil
}

func (x *Cursor) OrderValue() any {
	switch {
	case x.HasOrderString():
		return x.GetOrderString()
	case x.HasOrderBytes():
		return x.GetOrderBytes()
	case x.HasOrderInt32():
		return x.GetOrderInt32()
	case x.HasOrderInt64():
		return x.GetOrderInt64()
	case x.HasOrderUint32():
		return x.GetOrderUint32()
	case x.HasOrderUint64():
		return x.GetOrderUint64()
	case x.HasOrderFloat():
		return x.GetOrderFloat()
	case x.HasOrderDouble():
		return x.GetOrderDouble()
	case x.HasOrderBool():
		return x.GetOrderBool()
	case x.HasOrderTimestamp():
		return x.GetOrderTimestamp().AsTime()
	case x.HasOrderDuration():
		return x.GetOrderDuration().AsDuration()
	default:
		panic(fmt.Sprintf("unsupported or unset order_value in cursor: %v", x))
	}
}

func (x *Cursor) GetIsForwards() bool {
	return !x.GetIsBackwards()
}
