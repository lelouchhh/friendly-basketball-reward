package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/lelouchhh/friendly-basketball-reward/internal/postgres"
	"log"
	"os"
)

func main() {
	// Подключение к базе данных
	err := godotenv.Load(".env")
	dsn := os.Getenv("POSTGRES_CONNECTION")
	fmt.Println(dsn)
	db, err := postgres.NewDB(dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Получение текущей даты

	// Вычисление предыдущих двух месяцев
	// Форматирование дат для использования в запросах
	year1 := "2025"
	month1 := "01"
	year2 := "2025"
	year3 := "2025"
	month2 := "02"
	month3 := "03"

	// Создаем контекст
	ctx := context.Background()

	// Запуск обработки данных за предыдущие два месяца
	processRewardsForMonth(ctx, db, year1, month1)
	processRewardsForMonth(ctx, db, year2, month2)
	processRewardsForMonth(ctx, db, year3, month3)

	log.Println("Processing completed successfully")
}

// processRewardsForMonth обрабатывает все награды за указанный месяц.
func processRewardsForMonth(ctx context.Context, db *postgres.DB, year, month string) {
	log.Printf("Processing rewards for %s-%s...", year, month)

	// Обработка каждой награды
	processTopRatingPerMonth(ctx, db, year, month)
	processWorstRatingPerMonth(ctx, db, year, month)
	processTopWinratePerMonth(ctx, db, year, month)
	processBottomWinratePerMonth(ctx, db, year, month)
	processTopGainedRatingMonth(ctx, db, year, month)
	processTopLostRatingMonth(ctx, db, year, month)
	processMaxGamesPlayed(ctx, db, year, month)
	processLongestWinner(ctx, db, year, month)

	log.Printf("Finished processing rewards for %s-%s", year, month)
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
