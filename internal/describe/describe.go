package describe

import (
	"errors"
	"fmt"

	"github.com/advdv/scrud/internal/config"
	scrudv1 "github.com/advdv/scrud/scrud/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Describe takes a protobuf service reflections and returns a description.
func Describe(
	notifier Notifier,
	cfg config.Config,
	file protoreflect.FileDescriptor,
) (res *scrudv1.App, err error) {
	descr := &describer{
		config:   cfg,
		notifier: notifier,
		app: scrudv1.App_builder{
			Entities: map[string]*scrudv1.Entity{},
		}.Build(),
	}

	if err := descr.describe(file); err != nil {
		return nil, fmt.Errorf("describe: %w", err)
	}

	return descr.app, nil
}

// ErrNoTargets is returned when a protobuf file does not define the ssaas app. If the caller is not looking for
// it that means the file can be skipped.
var ErrNoTargets = errors.New("no crud services in file")

// describer holsd the stafe of a single describe pass.
type describer struct {
	config   config.Config
	notifier Notifier
	app      *scrudv1.App
}

func (d describer) describe(file protoreflect.FileDescriptor) error {
	sidesSeen := map[scrudv1.ServiceSide]struct{}{}
	for idx := range file.Services().Len() {
		desc := file.Services().Get(idx)
		opts, _ := desc.Options().(*descriptorpb.ServiceOptions)
		if opts == nil {
			continue
		}

		ssaasOpts, ok := proto.GetExtension(opts, scrudv1.E_Service).(*scrudv1.ServiceOptions)
		if !ok {
			continue
		}

		side := ssaasOpts.GetSide()
		if side == scrudv1.ServiceSide_SERVICE_SIDE_UNSPECIFIED {
			continue
		}

		if _, exists := sidesSeen[side]; exists {
			d.notifier.Annotatef(desc, "too many services of side: %s", side)
			continue
		}

		if err := d.describeService(desc, side); err != nil {
			return fmt.Errorf("describe service: %w", err)
		}

		sidesSeen[side] = struct{}{}
	}

	numSides := len(sidesSeen)
	if numSides < 1 {
		return ErrNoTargets
	}

	if numSides != 2 {
		d.notifier.Annotatef(file, "require the two services: read-write and read-only, in the same file, got: %d",
			numSides)
	}

	assertMissingOrExtra(d.notifier, d.config, file, d.app)

	return nil
}

func getMethodOptions(metDesc protoreflect.MethodDescriptor) (*scrudv1.MethodOptions, string, bool) {
	opts, _ := metDesc.Options().(*descriptorpb.MethodOptions)
	if opts == nil {
		return nil, "", false
	}

	ssaasOpts, ok := proto.GetExtension(opts, scrudv1.E_Method).(*scrudv1.MethodOptions)
	if !ok {
		return nil, "", false
	}

	entName := ssaasOpts.GetEntity()
	if entName == "" {
		return nil, "", false
	}

	return ssaasOpts, ssaasOpts.GetEntity(), true
}

func (d describer) describeService(
	svcDesc protoreflect.ServiceDescriptor,
	svcSide scrudv1.ServiceSide,
) error {
	for idx := range svcDesc.Methods().Len() {
		metDesc := svcDesc.Methods().Get(idx)
		opts, entName, ok := getMethodOptions(metDesc)
		if !ok {
			continue
		}

		actKind := opts.GetAction()

		ent := d.registerEntity(entName)
		entCfg, ok := d.config.GetEntity(entName)
		if !ok {
			d.notifier.Annotatef(metDesc, "entity '%s' has no configuration", entName)
			continue
		}

		if err := d.describeMethod(
			ent,
			entCfg,
			metDesc,
			svcSide,
			actKind,
			entName,
			opts.GetInput(),
			opts.GetOutput(),
		); err != nil {
			return fmt.Errorf("describe method: %w", err)
		}
	}

	return nil
}

func (d describer) describeMethod(
	ent *scrudv1.Entity,
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	actKind scrudv1.ActionKind,
	entName string,
	inputKind scrudv1.InputKind,
	ouputKind scrudv1.OutputKind,
) error {
	assertMethodName(d.notifier, metDesc, entName, actKind)
	assertInputOutputKind(d.notifier, metDesc, actKind, inputKind, ouputKind)

	d.registerAction(ent, metDesc, actKind)

	switch actKind {
	case scrudv1.ActionKind_ACTION_KIND_CREATE:
		return d.describeMethodCreate(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_DESCRIBE:
		return d.describeMethodDescribe(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_MODIFY:
		return d.describeMethodModify(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_REMOVE:
		return d.describeMethodRemove(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_LIST:
		return d.describeMethodList(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_RESTORE:
		return d.describeMethodRestore(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output())
	case scrudv1.ActionKind_ACTION_KIND_CUSTOM:
		return d.describeMethodCustom(entCfg, metDesc, svcSide, metDesc.Input(), metDesc.Output(), inputKind, ouputKind)
	case scrudv1.ActionKind_ACTION_KIND_UNSPECIFIED:
		d.notifier.Annotatef(metDesc, "assigned to entity without specifying 'action'")
		return nil
	}

	return nil
}

// Create action.
func (d describer) describeMethodCustom(
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	_ scrudv1.ServiceSide,
	input, output protoreflect.MessageDescriptor,
	inputKind scrudv1.InputKind,
	outputKind scrudv1.OutputKind,
) error {
	switch inputKind {
	case scrudv1.InputKind_INPUT_KIND_IDS:
		assertMessageIDsField(d.notifier, input, 20)
	case scrudv1.InputKind_INPUT_KIND_ITEMS:
		assertMessageItemsField(
			d.notifier, input, true, false, false, true, 20, false, entCfg.RequireOrganizatioIDInItem(), false)
	case scrudv1.InputKind_INPUT_KIND_NO_ID_ITEMS:
		assertMessageItemsField(
			d.notifier, input, false, false, false, true, 20, false, entCfg.RequireOrganizatioIDInItem(), false)
	case scrudv1.InputKind_INPUT_KIND_UNSPECIFIED:
		fallthrough
	default:
		d.notifier.Annotatef(metDesc, "unsupported input kind: %s", inputKind)
		return nil
	}

	switch outputKind {
	case scrudv1.OutputKind_OUTPUT_KIND_IDS:
		assertMessageIDsField(d.notifier, output, 20)
	case scrudv1.OutputKind_OUTPUT_KIND_ITEMS:
		assertMessageItemsField(
			d.notifier, output, true, false, false, true, 20, false, entCfg.RequireOrganizatioIDInItem(), false)
	case scrudv1.OutputKind_OUTPUT_KIND_EMPTY:
		assertOutputMessageIsEmpty(d.notifier, metDesc)
	case scrudv1.OutputKind_OUTPUT_KIND_UNSPECIFIED:
		fallthrough
	default:
		d.notifier.Annotatef(metDesc, "unsupported output kind: %s", outputKind)
		return nil
	}

	return nil
}

// Create action.
func (d describer) describeMethodCreate(
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, output protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_WRITE)
	assertMessageItemsField(
		d.notifier, input, false, false, false, true, 20, false, entCfg.RequireOrganizatioIDInItem(), false)
	assertMessageIDsField(d.notifier, output, 20)
	return nil
}

// Describe action.
func (d describer) describeMethodDescribe(
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, output protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_ONLY)
	assertMessageIDsField(d.notifier, input, 20)
	assertMessageItemsField(
		d.notifier, output, true, false, true, true, 20, false, entCfg.RequireOrganizatioIDInItem(),
		entCfg.CanAllowChangesToBeCaptured())
	assertDescribeInputFields(d.notifier, input)
	return nil
}

// Modify action.
func (d describer) describeMethodModify(
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, _ protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_WRITE)
	assertMessageItemsField(
		d.notifier, input, true, false, false, true, 20, true, entCfg.RequireOrganizatioIDInItem(), false)
	assertOutputMessageIsEmpty(d.notifier, metDesc)
	return nil
}

// Remove action.
func (d describer) describeMethodRemove(
	_ *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, _ protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_WRITE)
	assertMessageIDsField(d.notifier, input, 20)
	assertOutputMessageIsEmpty(d.notifier, metDesc)
	return nil
}

// List action.
func (d describer) describeMethodList(
	entCfg *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, output protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_ONLY)
	assertMessageItemsField(
		d.notifier, output, true, true, true, false, 100, false, entCfg.RequireOrganizatioIDInItem(),
		entCfg.CanAllowChangesToBeCaptured())
	assertListInputFields(d.notifier, input, entCfg.RequireOrganizatioIDInItem(), entCfg.SortingColumnNames)
	assertCursorFields(d.notifier, input, output)
	return nil
}

// Restore action.
func (d describer) describeMethodRestore(
	_ *config.Entity,
	metDesc protoreflect.MethodDescriptor,
	svcSide scrudv1.ServiceSide,
	input, _ protoreflect.MessageDescriptor,
) error {
	assertMethodServiceSide(d.notifier, metDesc, svcSide, scrudv1.ServiceSide_SERVICE_SIDE_READ_WRITE)
	assertMessageIDsField(d.notifier, input, 20)
	assertOutputMessageIsEmpty(d.notifier, metDesc)
	return nil
}
