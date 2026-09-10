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
		ID       string `yaml:"id"`
		Address  string `yaml:"address"` // куда control-plane будет диспатчить задачи
		Capacity int    `yaml:"capacity"`
		Pool     int    `yaml:"pool"`
	} `yaml:"worker"`
	ControlPlane struct {
		Address string `yaml:"address"`
	} `yaml:"control_plane"`
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
	fmt.Println("Worker ID:", cfg.Worker.ID)
	fmt.Println("Worker Address:", cfg.Worker.Address)
	fmt.Println("Control Plane Address:", cfg.ControlPlane.Address)
}
