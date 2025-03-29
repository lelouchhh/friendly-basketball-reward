package config

import "os"

type Config struct {
	PostgresConn string
	CronSpec     string
}

func NewConfig() Config {
	return Config{
		PostgresConn: os.Getenv("POSTGRES_CONNECTION"),
		CronSpec:     os.Getenv("CRON_SCHEDULE"),
	}
}
