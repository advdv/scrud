package describe

import (
	"fmt"

	"buf.build/go/bufplugin/check"
	"github.com/bufbuild/protoplugin"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Notifier abstracts some feedback that is given to the developer. It is implemented for a protoc plugin and a buf
// plugin.
type Notifier interface {
	Annotatef(desc protoreflect.Descriptor, msg string, args ...any)
}

type bufPluginNotifier struct{ resp check.ResponseWriter }

func NewBufPluginNotifier(resp check.ResponseWriter) Notifier {
	return bufPluginNotifier{resp}
}

func (n bufPluginNotifier) Annotatef(desc protoreflect.Descriptor, msg string, args ...any) {
	n.resp.AddAnnotation(
		check.WithMessagef(msg, args...),
		check.WithDescriptor(desc))
}

type protocPluginNotifier struct{ resp protoplugin.ResponseWriter }

func NewProtocPluginNotifier(resp protoplugin.ResponseWriter) Notifier {
	return protocPluginNotifier{resp}
}

func (n protocPluginNotifier) Annotatef(desc protoreflect.Descriptor, msg string, args ...any) {
	n.resp.AddError(fmt.Sprintf("%s: %s", desc.FullName(), fmt.Sprintf(msg, args...)))
}
