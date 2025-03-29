package cron

import (
	"friendly-basketball-reward/internal/postgres"
	"github.com/robfig/cron/v3"
	"log"
)

func StartCronJobs(db *postgres.DB) {
	c := cron.New()

	// Используем cron-выражение для первого дня каждого месяца
	_, err := c.AddFunc("0 0 1 * *", func() {
		log.Println("Running monthly cron job...")
		//users, err := service.CollectUserStats(db)
		//if err != nil {
		//	log.Printf("Error collecting user stats: %v", err)
		//	return
		//}
		//service.DistributeRewards(db, users)
	})
	if err != nil {
		log.Fatalf("Failed to schedule cron job: %v", err)
	}

	c.Start()
}
