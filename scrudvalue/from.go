package scrudvalue

import (
	"database/sql"
	"slices"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func FromNullable[T any](
	hasf func() bool,
	getf func() T,
) *sql.Null[T] {
	if hasf() {
		return &sql.Null[T]{V: getf(), Valid: true}
	}

	return &sql.Null[T]{}
}

func FromMaskedNullable[T any](
	masked interface{ GetMask() *fieldmaskpb.FieldMask },
	fieldName string,
	hasf func() bool,
	getf func() T,
) *sql.Null[T] {
	if !slices.Contains(masked.GetMask().GetPaths(), fieldName) {
		return nil // don't set this value at all.
	}

	return FromNullable(hasf, getf)
}

func FromMasked[T any](
	masked interface{ GetMask() *fieldmaskpb.FieldMask },
	fieldName string,
	hasf func() bool,
	getf func() T,
) *T {
	if !slices.Contains(masked.GetMask().GetPaths(), fieldName) {
		return nil
	}

	return From(hasf, getf)
}

func From[T any](
	hasf func() bool,
	getf func() T,
) *T {
	if hasf() {
		v := getf()
		return &v
	}

	return nil
}
