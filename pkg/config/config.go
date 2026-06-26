// Package config is a thin, generic wrapper around viper that loads
// configuration from (in increasing order of precedence):
//
//	DefaultConfig() values  <  config file (yaml)
//
// Configuration is object-shaped only: it is read from a structured YAML file
// (no flattened environment variables, no command-line flags). Callers
// pre-populate a destination struct with their composed DefaultConfig() values
// and call Load to overlay the file's values on top.
package config

import (
	"errors"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// Options controls how the config file is discovered and decoded.
type Options struct {
	// FileName is the config file base name (without extension). Default "config".
	FileName string
	// FileType is the config file extension/type. Default "yaml".
	FileType string
	// SearchPaths are directories searched for the config file.
	// Default: ".", "./config", "/etc/pulse".
	SearchPaths []string
	// TagName is the struct tag used for field mapping. Default "mapstructure".
	TagName string
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		FileName:    "config",
		FileType:    "yaml",
		SearchPaths: []string{".", "./config", "/etc/pulse"},
		TagName:     "mapstructure",
	}
}

func (o *Options) applyDefaults() {
	d := DefaultOptions()
	if o.FileName == "" {
		o.FileName = d.FileName
	}
	if o.FileType == "" {
		o.FileType = d.FileType
	}
	if len(o.SearchPaths) == 0 {
		o.SearchPaths = d.SearchPaths
	}
	if o.TagName == "" {
		o.TagName = d.TagName
	}
}

// Load overlays the config file onto dst. dst MUST be a non-nil pointer to a
// struct that is already populated with default values (typically the app's
// composed DefaultConfig()). Keys absent from the file keep their defaults,
// because viper's Unmarshal only writes keys it actually finds.
//
// A missing config file is not an error (defaults are used); any other read
// error is returned.
func Load[T any](dst *T, o Options) error {
	if dst == nil {
		return errors.New("config: dst must be a non-nil pointer")
	}
	o.applyDefaults()

	v := viper.New()
	v.SetConfigName(o.FileName)
	v.SetConfigType(o.FileType)
	for _, p := range o.SearchPaths {
		v.AddConfigPath(p)
	}

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return err
		}
		// No file found is fine — defaults still apply.
	}

	decodeHook := mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)
	return v.Unmarshal(dst, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = o.TagName
		dc.DecodeHook = decodeHook
	})
}
