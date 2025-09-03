// Package config defines the ssaas configuration.
package config

import (
	"fmt"
	"os"

	scrudv1 "github.com/advdv/scrud/scrud/v1"
	validator "github.com/go-playground/validator/v10"
	yaml "github.com/goccy/go-yaml"
)

// Entity configures each entity.
type Entity struct {
	// what columns can this entity be sorted with when listing.
	SortingColumnNames []string `yaml:"sorting_column_names"`
	// which actions do not need to be implemented for this entity.
	SkipStandardActions []scrudv1.ActionKind `yaml:"skip_standard_actions"`
	// wether the entity is scoped to an organization.
	NotOrganizationScoped bool `yaml:"not_organization_scoped"`
	// whether the entity has it changes captured.
	NoChangesCaptures bool `yaml:"no_changes_captured"`
}

// Config configures the ssaas code generation and linting.
type Config struct {
	// Entities our code generator knows about.
	Entities map[string]*Entity `validate:"required,dive" yaml:"entities"`
}

// Load the configuration from a file.
func Load(filename string) (cfg Config, err error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return cfg, fmt.Errorf("read configuration file: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("unmarshal yaml configuration: %w", err)
	}

	for _, ent := range cfg.Entities {
		if len(ent.SortingColumnNames) < 1 {
			ent.SortingColumnNames = []string{"created_at", "updated_at"}
		}
	}

	if err := validator.New(validator.WithRequiredStructEnabled()).Struct(cfg); err != nil {
		return cfg, fmt.Errorf("validate: %w", err)
	}

	return cfg, nil
}

func (cfg Config) GetEntity(entName string) (*Entity, bool) {
	ent, ok := cfg.Entities[entName]
	if !ok {
		return nil, false
	}

	return ent, true
}

func (e *Entity) RequireOrganizatioIDInItem() bool {
	return !e.NotOrganizationScoped
}

func (e *Entity) CanAllowChangesToBeCaptured() bool {
	return !e.NoChangesCaptures
}
