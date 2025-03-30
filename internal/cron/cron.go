package cron

import (
	"context"
	"log"
	"time"

	"github.com/lelouchhh/friendly-basketball-reward/internal/postgres"
	"github.com/robfig/cron/v3"
)

// StartCronJobs запускает cron-задачи для расчета наград за предыдущий месяц.
func StartCronJobs(db *postgres.DB, input string) {
	c := cron.New()

	// Используем cron-выражение для первого дня каждого месяца
	_, err := c.AddFunc(input, func() {
		log.Println("Running monthly cron job...")

		// Определяем предыдущий месяц
		now := time.Now()
		prevMonth := now.AddDate(0, -1, 0)
		year := prevMonth.Format("2006") // Год предыдущего месяца
		month := prevMonth.Format("01")  // Месяц предыдущего месяца

		// Создаем контекст
		ctx := context.Background()

		// Вычисляем и сохраняем награды
		go processTopRatingPerMonth(ctx, db, year, month)
		go processWorstRatingPerMonth(ctx, db, year, month)
		go processTopWinratePerMonth(ctx, db, year, month)
		go processBottomWinratePerMonth(ctx, db, year, month)
		go processTopGainedRatingMonth(ctx, db, year, month)
		go processTopLostRatingMonth(ctx, db, year, month)
		go processMaxGamesPlayed(ctx, db, year, month)
		go processLongestWinner(ctx, db, year, month)
	})
	if err != nil {
		log.Fatalf("Failed to schedule cron job: %v", err)
	}

	c.Start()
	log.Println("Cron jobs started successfully")
}

// processTopRatingPerMonth вычисляет лучшего игрока месяца и сохраняет награду.
func processTopRatingPerMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing top rating for %s-%s...", year, month)
	err := db.TopRatingPerMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process top rating for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed top rating for %s-%s", year, month)
	}
}

// processWorstRatingPerMonth вычисляет худшего игрока месяца и сохраняет награду.
func processWorstRatingPerMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing worst rating for %s-%s...", year, month)
	err := db.WorstRatingPerMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process worst rating for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed worst rating for %s-%s", year, month)
	}
}

// processTopWinratePerMonth вычисляет лучший процент побед и сохраняет награду.
func processTopWinratePerMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing top winrate for %s-%s...", year, month)
	err := db.TopWinratePerMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process top winrate for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed top winrate for %s-%s", year, month)
	}
}

// processBottomWinratePerMonth вычисляет худший процент побед и сохраняет награду.
func processBottomWinratePerMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing bottom winrate for %s-%s...", year, month)
	err := db.BottomWinratePerMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process bottom winrate for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed bottom winrate for %s-%s", year, month)
	}
}

// processTopGainedRatingMonth вычисляет максимальный прирост рейтинга и сохраняет награду.
func processTopGainedRatingMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing top gained rating for %s-%s...", year, month)
	err := db.TopGainedRatingMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process top gained rating for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed top gained rating for %s-%s", year, month)
	}
}

// processTopLostRatingMonth вычисляет максимальную потерю рейтинга и сохраняет награду.
func processTopLostRatingMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing top lost rating for %s-%s...", year, month)
	err := db.TopLostRatingMonth(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process top lost rating for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed top lost rating for %s-%s", year, month)
	}
}

// processMaxGamesPlayed вычисляет наибольшее количество сыгранных игр и сохраняет награду.
func processMaxGamesPlayed(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing max games played for %s-%s...", year, month)
	err := db.MaxGamesPlayed(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process max games played for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed max games played for %s-%s", year, month)
	}
}
func processLongestWinner(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing logest winner for %s-%s...", year, month)
	err := db.LongestWinStreak(ctx, year, month)
	if err != nil {
		log.Printf("Failed to process logest winner for %s-%s: %v", year, month, err)
	} else {
		log.Printf("Successfully processed logest winner for %s-%s", year, month)
	}
}
