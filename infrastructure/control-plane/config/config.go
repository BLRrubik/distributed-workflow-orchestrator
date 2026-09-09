package config

import (
	"flag"
	"fmt"
	"os"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	GRPC struct {
		Port string `yaml:"port"`
	} `yaml:"grpc"`
	Worker struct {
		URL string `yaml:"url"`
	} `yaml:"worker"`
}

func Load() (*Config, error) {
	var configPath string

	flag.StringVar(&configPath, "config", "config.yaml", "path to config file")
	flag.Parse()

	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func Print(cfg *Config) {
	fmt.Println("-------CONFIG-------")
	fmt.Println("GRPC Port:", cfg.GRPC.Port)
}
