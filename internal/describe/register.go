package describe

import (
	scrudv1 "github.com/advdv/scrud/scrud/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func (d describer) registerEntity(entName string) *scrudv1.Entity {
	ents := d.app.GetEntities()
	existing, ok := ents[entName]
	if !ok {
		ents[entName] = scrudv1.Entity_builder{
			Name:    &entName,
			Actions: map[string]*scrudv1.Action{},
		}.Build()
		return ents[entName]
	}

	return existing
}

func (d describer) registerAction(
	ent *scrudv1.Entity,
	met protoreflect.MethodDescriptor,
	actKind scrudv1.ActionKind,
) *scrudv1.Action {
	acts := ent.GetActions()
	existing, ok := acts[string(met.Name())]
	if !ok {
		acts[string(met.Name())] = scrudv1.Action_builder{
			Kind:      &actKind,
			ProtoName: proto.String(string(met.Name())),
		}.Build()

		return acts[string(met.Name())]
	}

	return existing
}
