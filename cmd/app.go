package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/lelouchhh/friendly-basketball-reward/internal/config"
	"github.com/lelouchhh/friendly-basketball-reward/internal/cron"
	"github.com/lelouchhh/friendly-basketball-reward/internal/postgres"
	"log"
	"os"
)

func main() {
	log.Println("Getting config")
	err := godotenv.Load("./.env")
	if err != nil {
		return
	}
	cfg := config.NewConfig()

	db, err := postgres.NewDB(cfg.PostgresConn)
	log.Println("Connected to database")

	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()
	log.Println("starting cron job")
	fmt.Println(os.Getenv(""))
	go cron.StartCronJobs(db, cfg.CronSpec)
	select {}

}
