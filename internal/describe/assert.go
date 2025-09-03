package describe

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/advdv/scrud/internal/config"
	scrudv1 "github.com/advdv/scrud/scrud/v1"
	goset "github.com/hashicorp/go-set/v3"
	"github.com/iancoleman/strcase"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func assertInputOutputKind(
	notify Notifier,
	desc protoreflect.MethodDescriptor,
	metKind scrudv1.ActionKind,
	inpKind scrudv1.InputKind,
	outKind scrudv1.OutputKind,
) {
	if metKind == scrudv1.ActionKind_ACTION_KIND_CUSTOM {
		if inpKind == scrudv1.InputKind_INPUT_KIND_UNSPECIFIED || outKind == scrudv1.OutputKind_OUTPUT_KIND_UNSPECIFIED {
			notify.Annotatef(desc, "custom action must have a input/output kind specified")
		}
	} else {
		if inpKind != scrudv1.InputKind_INPUT_KIND_UNSPECIFIED || outKind != scrudv1.OutputKind_OUTPUT_KIND_UNSPECIFIED {
			notify.Annotatef(desc, "non-custom actions cannot have a input/output kind specified")
		}
	}
}

func assertMethodServiceSide(notify Notifier, desc protoreflect.MethodDescriptor, act, exp scrudv1.ServiceSide) {
	if exp != act {
		notify.Annotatef(desc, "method service side: %s != %s", exp.String(), act.String())
	}
}

func assertMethodName(notify Notifier, desc protoreflect.MethodDescriptor, entName string, actKind scrudv1.ActionKind) {
	act := string(desc.Name())

	// For the "CUSTOM" type the name only needs to be suffixed with the entity.
	if actKind == scrudv1.ActionKind_ACTION_KIND_CUSTOM {
		if !strings.HasSuffix(act, entName) {
			notify.Annotatef(desc, "for custom action the method name must end with: %s", entName)
		}

		return
	}

	exp := fmt.Sprintf("%s%s", strcase.ToCamel(strings.TrimPrefix(actKind.String(), "ACTION_KIND_")), entName)
	if exp != act {
		notify.Annotatef(desc, "method name: %s != %s", exp, act)
	}
}

func assertPagination(
	notify Notifier, desc protoreflect.MessageDescriptor,
) {
	field := desc.Fields().ByName("per_page")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'per_page' field")
		return
	}

	if field.Kind() != protoreflect.Int32Kind {
		notify.Annotatef(field, "'per_page' field must be a int32 field, got: %s", field.Kind())
		return
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if fc.GetRequired() {
			m = append(m, "must NOT be marked as 'required'")
		}

		if fc.GetInt32().GetGte() != 1 {
			m = append(m, "must be constrainted with 'gte=1'")
		}
		if fc.GetInt32().GetLte() != 100 {
			m = append(m, "must be constrainted with 'lte=100'")
		}

		return
	})
}

func assertSortDesc(
	notify Notifier, desc protoreflect.MessageDescriptor,
) {
	field := desc.Fields().ByName("sort_desc")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'sort_desc' field")
		return
	}

	if field.Kind() != protoreflect.BoolKind {
		notify.Annotatef(field, "'sort_desc' field must be a bool field, got: %s", field.Kind())
		return
	}
}

func assertArchived(
	notify Notifier, desc protoreflect.MessageDescriptor,
) {
	field := desc.Fields().ByName("show_archived")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'show_archived' field")
		return
	}

	if field.Kind() != protoreflect.BoolKind {
		notify.Annotatef(field, "'show_archived' field must be a bool field, got: %s", field.Kind())
		return
	}
}

func assertSortBy(
	notify Notifier, desc protoreflect.MessageDescriptor, sortingColumNames []string,
) {
	field := desc.Fields().ByName("sort_by")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'sort_by' field")
		return
	}

	if field.Kind() != protoreflect.StringKind {
		notify.Annotatef(field, "'sort_by' field must be a string field, got: %s", field.Kind())
		return
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if fc.GetRequired() {
			m = append(m, "must NOT be marked as 'required'")
		}

		actIn := fc.GetString().GetIn()
		if !slices.Equal(actIn, sortingColumNames) {
			m = append(m, fmt.Sprintf("must have for 'in=<columns>' columns be as configured: %v, got: %v",
				sortingColumNames, actIn))
		}

		return
	})
}

func assertDescribeInputFields(notify Notifier, desc protoreflect.MessageDescriptor) {
	assertConsiderArchived(notify, desc)
}

func assertConsiderArchived(
	notify Notifier, desc protoreflect.MessageDescriptor,
) {
	field := desc.Fields().ByName("consider_archived")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'consider_archived' field")
		return
	}

	if field.Kind() != protoreflect.BoolKind {
		notify.Annotatef(field, "'consider_archived' field must be a bool field, got: %s", field.Kind())
		return
	}
}

func assertListInputFields(
	notify Notifier, desc protoreflect.MessageDescriptor, mustHaveOrganizationID bool, sortingColumnNames []string,
) {
	assertPagination(notify, desc)
	assertSortDesc(notify, desc)
	assertSortBy(notify, desc, sortingColumnNames)
	assertArchived(notify, desc)

	if mustHaveOrganizationID {
		assertMessageOrganizationIDField(notify, desc)
	}
}

const maxCursorLen = 300

func assertCursorField(
	notify Notifier, desc protoreflect.MessageDescriptor, fieldName string,
) {
	field := desc.Fields().ByName(protoreflect.Name(fieldName))
	if field == nil {
		notify.Annotatef(desc, "method's message must have an '%s' field", fieldName)
		return
	}

	if field.Kind() != protoreflect.BytesKind {
		notify.Annotatef(field, "'%s' field must be a bytes field, got: %s", fieldName, field.Kind())
		return
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if fc.GetRequired() {
			m = append(m, "must NOT be marked as 'required'")
		}

		if fc.GetBytes().GetMaxLen() != maxCursorLen {
			m = append(m, fmt.Sprintf("must have a max_len constraint of: %d", maxCursorLen))
		}

		return
	})
}

func assertCursorFields(
	notify Notifier, input, output protoreflect.MessageDescriptor,
) {
	assertCursorField(notify, input, "cursor")
	assertCursorField(notify, output, "next_cursor")
	assertCursorField(notify, output, "previous_cursor")
}

func assertMessageItemsField(
	notify Notifier, desc protoreflect.MessageDescriptor,
	checkItemIDField bool,
	checkDescribeMessageInsteaOfItem bool,
	checkUpdatedCreatedAtFields bool,
	expectItemsFieldRequired bool,
	maxItems uint64,
	mustHaveMask bool,
	mustHaveOrganizationID bool,
	mustHaveChangeRecordIDs bool,
) {
	field := desc.Fields().ByName("items")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'items' field")
		return
	}

	if field.Number() != 1 {
		notify.Annotatef(field, "'items' field must be field number 1, got: %d", field.Number())
	}

	if field.Cardinality() != protoreflect.Repeated {
		notify.Annotatef(field, "'items' field must be a repeated field, got: %s", field.Cardinality())
	}

	if field.Kind() != protoreflect.MessageKind {
		notify.Annotatef(field, "'items' field must be a message field, got: %s", field.Kind())
		return
	}

	// By default. The item's field type needs to be a message that is embedded. But in case of the List
	// operation it is the describe
	var expItemMessageFullname string
	if checkDescribeMessageInsteaOfItem {
		expItemMessageFullname = strings.ReplaceAll(string(desc.FullName()),
			".List", ".Describe")
		expItemMessageFullname += ".Item"
	} else {
		expItemMessageFullname = string(desc.FullName()) + ".Item"
	}

	if string(field.Message().FullName()) != expItemMessageFullname {
		notify.Annotatef(field, "'items' field must be a message of type: %s got: %s",
			expItemMessageFullname,
			field.Message().FullName())
	}

	// if enabled, check that the item message has an "id" field.
	if checkItemIDField {
		assertMessageIDField(notify, field.Message())
	}

	// if enabled, check that the item message has an "organization_id" field.
	if mustHaveOrganizationID {
		assertMessageOrganizationIDField(notify, field.Message())
	}

	// if enabled, check that the item has a valid date field.
	if checkUpdatedCreatedAtFields {
		assertMessageTimestampField(notify, field.Message(), "updated_at", true)
		assertMessageTimestampField(notify, field.Message(), "created_at", true)
		assertMessageTimestampField(notify, field.Message(), "archived_at", false)
	}

	// some item message should maybe require a mask field.
	if mustHaveMask {
		assertMessageItemsMaskField(notify, field.Message())
	}

	// if enabled, check that the item message has an "id" field.
	if mustHaveChangeRecordIDs {
		assertMessageItemsChangeRecordIDsField(notify, field.Message())
	}

	// check shared validation requirements between items and ids.
	assertIDsItemsFieldValidation(notify, field, expectItemsFieldRequired, maxItems)
}

// assertMessageItemsChangeRecordIDsField checks if the item has a field that contains the ids for fetching
// any changes that have been performed on the record.  This can be multiple because what is described in the
// API might be backed by multiple database rows.
func assertMessageItemsChangeRecordIDsField(notify Notifier, desc protoreflect.MessageDescriptor) {
	name := "change_record_ids"
	field := desc.Fields().ByName(protoreflect.Name(name))
	if field == nil {
		notify.Annotatef(desc, "message must have an '%s' field", name)
		return
	}

	if field.Cardinality() != protoreflect.Repeated {
		notify.Annotatef(field, "'%s' field must be a repeated field, got: %s", name, field.Cardinality())
		return
	}

	if field.Kind() != protoreflect.StringKind {
		notify.Annotatef(field, "'%s' field must be a string field, got: %s", name, field.Kind())
		return
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if !fc.GetRequired() {
			m = append(m, "must be marked as 'required'")
		}

		repeated := fc.GetRepeated()
		if repeated == nil {
			m = append(m, "must have `repeated` constrainted")
			return m
		}

		expectMinItems, maxItems := uint64(1), uint64(100)
		if repeated.GetMinItems() != expectMinItems {
			m = append(m, fmt.Sprintf("constraint 'min_items' must be: %v, got: %v", expectMinItems,
				repeated.GetMinItems()))
		}

		if repeated.GetMaxItems() != maxItems {
			m = append(m, fmt.Sprintf("must have a max-item constraint of: %d", maxItems))
		}

		items := repeated.GetItems()
		if items == nil {
			m = append(m, "must have `repeated.items` constrainted")
			return m
		}

		if !items.GetString().GetUuid() {
			m = append(m, "must have `repeated.items.uuid` constrainted set to true")
		}

		return m
	})
}

func assertMessageItemsMaskField(notify Notifier, desc protoreflect.MessageDescriptor) {
	name := "mask"
	field := desc.Fields().ByName(protoreflect.Name(name))
	if field == nil {
		notify.Annotatef(desc, "message must have an '%s' field", name)
		return
	}

	if field.Kind() != protoreflect.MessageKind {
		notify.Annotatef(field, "'%s' field must be a message field, got: %s", name, field.Kind())
		return
	}

	if field.Message().FullName() != "google.protobuf.FieldMask" {
		notify.Annotatef(field, "'%s' field must be a google.protobuf.FieldMask, got: %s", name, field.Message().FullName())
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if !fc.GetRequired() {
			m = append(m, "must be marked as 'required'")
		}

		return
	})
}

func assertMessageTimestampField(
	notify Notifier, desc protoreflect.MessageDescriptor, name string, mustBeRequired bool,
) {
	field := desc.Fields().ByName(protoreflect.Name(name))
	if field == nil {
		notify.Annotatef(desc, "message must have an '%s' field", name)
		return
	}

	if field.Kind() != protoreflect.MessageKind {
		notify.Annotatef(field, "'%s' field must be a message field, got: %s", name, field.Kind())
		return
	}

	if field.Message().FullName() != "google.protobuf.Timestamp" {
		notify.Annotatef(field, "'%s' field must be a google.protobuf.Timestamp, got: %s", name, field.Message().FullName())
	}

	if mustBeRequired {
		assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
			if !fc.GetRequired() {
				m = append(m, "must be marked as 'required'")
			}

			return
		})
	}
}

func assertMessageIDField(notify Notifier, desc protoreflect.MessageDescriptor) {
	field := desc.Fields().ByName("id")
	if field == nil {
		notify.Annotatef(desc, "message must have an 'id' field")
		return
	}

	if field.Kind() != protoreflect.StringKind {
		notify.Annotatef(field, "'id' field must be a string field, got: %s", field.Kind())
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if !fc.GetRequired() {
			m = append(m, "must be marked as 'required'")
		}

		assertTypIDStringRule(notify, field, fc.GetString())
		return
	})
}

func assertMessageOrganizationIDField(notify Notifier, desc protoreflect.MessageDescriptor) {
	field := desc.Fields().ByName("organization_id")
	if field == nil {
		notify.Annotatef(desc, "message must have an 'organization_id' field")
		return
	}

	if field.Kind() != protoreflect.StringKind {
		notify.Annotatef(field, "'organization_id' field must be a string field, got: %s", field.Kind())
	}

	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if !fc.GetRequired() {
			m = append(m, "must be marked as 'required'")
		}

		assertTypIDStringRule(notify, field, fc.GetString())
		return
	})
}

func assertMessageIDsField(notify Notifier, desc protoreflect.MessageDescriptor, maxItems uint64) {
	field := desc.Fields().ByName("ids")
	if field == nil {
		notify.Annotatef(desc, "method's message must have an 'ids' field")
		return
	}

	if field.Number() != 1 {
		notify.Annotatef(field, "'ids' field must be field number 1, got: %d", field.Number())
	}

	if field.Cardinality() != protoreflect.Repeated {
		notify.Annotatef(field, "'ids' field must be a repeated field, got: %s", field.Cardinality())
	}

	if field.Kind() != protoreflect.StringKind {
		notify.Annotatef(field, "'ids' field must be a string field, got: %s", field.Kind())
	}

	// assert shared validation between ids and items.
	assertIDsItemsFieldValidation(notify, field, true, maxItems)

	// assert that ids fields have our custom typeid cel expression.
	assertFieldValidation(notify, field, func(fc *validate.FieldRules) (m []string) {
		if !fc.HasRepeated() {
			return nil
		}

		itemStrRule := fc.GetRepeated().GetItems().GetString()
		if itemStrRule == nil {
			notify.Annotatef(field, "'ids' field must have a repeated item string rule")
			return
		}

		assertTypIDStringRule(notify, field, itemStrRule)
		return
	})
}

func assertTypIDStringRule(notify Notifier, field protoreflect.FieldDescriptor, rule *validate.StringRules) {
	// @TODO fix me
	// if !proto.HasExtension(rule, internalapiv1.E_Typeid) {
	// 	notify.Annotatef(field,
	// 		"'ids' item strings must carry the (internal.api.v1.typeid) constraint")
	// 	return
	// }

	// if ok, _ := proto.GetExtension(rule, internalapiv1.E_Typeid).(bool); !ok {
	// 	notify.Annotatef(field,
	// 		"(internal.api.v1.typeid) must be set to true, e.g. `[internal.api.v1.typeid]: true`")
	// }
}

func assertOutputMessageIsEmpty(notify Notifier, metDesc protoreflect.MethodDescriptor) {
	if metDesc.Output().FullName() != "google.protobuf.Empty" {
		notify.Annotatef(metDesc, "method output message is expected to be: google.protobuf.Empty")
	}
}

func assertIDsItemsFieldValidation(
	notify Notifier,
	desc protoreflect.FieldDescriptor,
	expectBeRequired bool,
	maxItems uint64,
) {
	assertFieldValidation(notify, desc, func(fc *validate.FieldRules) (m []string) {
		expectMinItems := uint64(1)
		if expectBeRequired != fc.GetRequired() {
			m = append(m, fmt.Sprintf("constraint 'required' must be: %v, got: %v", expectBeRequired, fc.GetRequired()))
		}

		if !expectBeRequired {
			expectMinItems = 0
		}

		if expectMinItems != fc.GetRepeated().GetMinItems() {
			m = append(m, fmt.Sprintf("constraint 'min_items' must be: %v, got: %v", expectMinItems,
				fc.GetRepeated().GetMinItems()))
		}

		if fc.GetRepeated().GetMaxItems() != maxItems {
			m = append(m, fmt.Sprintf("must have a max-item constraint of: %d", maxItems))
		}

		return m
	})
}

func assertFieldValidation(
	notify Notifier,
	desc protoreflect.FieldDescriptor,
	checkFn func(fc *validate.FieldRules) []string,
) {
	constrainedMsg := "field must have validation constraints"
	fopts, _ := desc.Options().(*descriptorpb.FieldOptions)
	if fopts == nil {
		notify.Annotatef(desc, constrainedMsg)
	}

	if fc, ok := proto.GetExtension(fopts, validate.E_Field).(*validate.FieldRules); ok {
		if fc == nil {
			notify.Annotatef(desc, constrainedMsg)
		} else {
			for _, msg := range checkFn(fc) {
				notify.Annotatef(desc, "must have constraint: "+msg)
			}
		}
	} else {
		notify.Annotatef(desc, constrainedMsg)
	}
}

var allKinds = []scrudv1.ActionKind{
	scrudv1.ActionKind_ACTION_KIND_DESCRIBE,
	scrudv1.ActionKind_ACTION_KIND_CREATE,
	scrudv1.ActionKind_ACTION_KIND_MODIFY,
	scrudv1.ActionKind_ACTION_KIND_REMOVE,
	scrudv1.ActionKind_ACTION_KIND_LIST,
	scrudv1.ActionKind_ACTION_KIND_RESTORE,
}

func assertMissingOrExtra(
	notify Notifier,
	cfg config.Config,
	file protoreflect.FileDescriptor,
	app *scrudv1.App,
) {
	// make sure all entities are also configured in the config file.
	configuredEntities := goset.From(slices.Collect(maps.Keys(cfg.Entities)))
	declaredEntities := goset.From(slices.Collect(maps.Keys(app.GetEntities())))
	if !configuredEntities.Equal(declaredEntities) {
		notify.Annotatef(file,
			"declared entities: %v, doesn't match configured entities: %v", declaredEntities, configuredEntities)
		return
	}

	// for all entities, make sure the required actions are setup.
	for name, ent := range app.GetEntities() {
		entCfg, ok := cfg.GetEntity(name)
		if !ok {
			notify.Annotatef(file, "no configuration for entity: %s", name)
			continue
		}

		has := map[scrudv1.ActionKind]struct{}{}
		for _, act := range ent.GetActions() {
			if act.GetKind() == scrudv1.ActionKind_ACTION_KIND_CUSTOM {
				continue // custom actions are not involved in this check.
			}

			has[act.GetKind()] = struct{}{}
		}

		exp := map[scrudv1.ActionKind]struct{}{}
		for _, expKind := range allKinds {
			if slices.Contains(entCfg.SkipStandardActions, expKind) {
				continue // marked by configuration as not expecting it.
			}
			exp[expKind] = struct{}{}
		}

		// the expected action setup with the actual action setup.
		expActions := goset.From(slices.Collect(maps.Keys(exp)))
		actActions := goset.From(slices.Collect(maps.Keys(has)))
		if !expActions.Equal(actActions) {
			missingActions, tooManyActions := expActions.Difference(actActions), actActions.Difference(expActions)
			if tooManyActions.Size() > 0 {
				notify.Annotatef(file,
					"%s: too many action(s) declared: %v", name, tooManyActions)
			}
			if missingActions.Size() > 0 {
				notify.Annotatef(file,
					"%s: need to declare action(s): %v", name, missingActions)
			}

			continue
		}
	}
}
