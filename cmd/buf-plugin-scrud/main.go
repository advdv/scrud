package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"buf.build/go/bufplugin/check"
	"buf.build/go/bufplugin/check/checkutil"
	"buf.build/go/bufplugin/descriptor"
	"buf.build/go/bufplugin/info"
	"buf.build/go/bufplugin/option"
	"github.com/advdv/scrud/internal/config"
	"github.com/advdv/scrud/internal/describe"
)

var spec = &check.Spec{
	Rules: []*check.RuleSpec{
		{
			ID:      "SCRUD",
			Default: true,
			Purpose: `Apply all standard CRUD rules from the services down.`,
			Type:    check.RuleTypeLint,
			Handler: checkutil.NewFileRuleHandler(checkFile, checkutil.WithoutImports()),
		},
	},
	Info: &info.Spec{
		Documentation: `A Buf plugin for building CRUD rpcs, standardized, structured and quick.`,
		SPDXLicenseID: "apache-2.0",
		LicenseURL:    "https://github.com/bufbuild/bufplugin-go/blob/main/LICENSE",
	},
}

func checkFile(
	_ context.Context,
	resp check.ResponseWriter,
	req check.Request,
	desc descriptor.FileDescriptor,
) (err error) {
	cfg, err := requestConfig(req)
	if err != nil {
		resp.AddAnnotation(
			check.WithDescriptor(desc.ProtoreflectFileDescriptor()),
			check.WithMessagef("invalid configuration: %s", err.Error()))
		return nil
	}

	if _, err := describe.Describe(
		describe.NewBufPluginNotifier(resp),
		cfg,
		desc.ProtoreflectFileDescriptor(),
	); err != nil && !errors.Is(err, describe.ErrNoTargets) {
		return fmt.Errorf("describe: %w", err)
	}

	return nil
}

func requestConfig(req check.Request) (cfg config.Config, err error) {
	dir, err := os.Getwd()
	if err != nil {
		return cfg, fmt.Errorf("get working dir: %w", err)
	}

	filename, err := option.GetStringValue(req.Options(), "config_file")
	if err != nil {
		return cfg, fmt.Errorf("read config file option: %w", err)
	}

	cfg, err = config.Load(filepath.Join(dir, filename))
	if err != nil {
		return cfg, fmt.Errorf("load config: %w", err)
	}

	return
}

func main() {
	check.Main(spec)
}
