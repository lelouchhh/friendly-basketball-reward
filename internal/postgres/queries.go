package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)

const (
	BEST_PLAYER_BY_RATING_MONTH_1x1 = "Лучший игрок месяца по рейтингу 1x1!"
	BEST_PLAYER_BY_RATING_MONTH_2x2 = "Лучший игрок месяца по рейтингу 2x2!"
	BEST_PLAYER_BY_RATING_MONTH_3x3 = "Лучший игрок месяца по рейтингу 3x3!"
	BEST_PLAYER_BY_RATING_MONTH_4x4 = "Лучший игрок месяца по рейтингу 4x4!"
	BEST_PLAYER_BY_RATING_MONTH_5x5 = "Лучший игрок месяца по рейтингу 5x5!"

	WORST_PLAYER_BY_RATING_MONTH_1x1 = "Худший игрок месяца по рейтингу 1x1!"
	WORST_PLAYER_BY_RATING_MONTH_2x2 = "Худший игрок месяца по рейтингу 2x2!"
	WORST_PLAYER_BY_RATING_MONTH_3x3 = "Худший игрок месяца по рейтингу 3x3!"
	WORST_PLAYER_BY_RATING_MONTH_4x4 = "Худший игрок месяца по рейтингу 4x4!"
	WORST_PLAYER_BY_RATING_MONTH_5x5 = "Худший игрок месяца по рейтингу 5x5!"

	TOP_WINRATE_MONTH       = "Лучший процент побед за месяц!"
	MAX_LOSERATE_MONTH      = "Худший процент побед за месяц!"
	TOP_GAINED_RATING_MONTH = "Максимальный прирост рейтинга за месяц!"
	MAX_LOST_RATING_MONTH   = "Максимальная потеря рейтинга за месяц!"
	MAX_GAMES_PLAYED_MONTH  = "Наибольшее количество сыгранных игр за месяц!"

	LONGEST_WIN_STREAK_MONTH = "Самая длинная серия подряд за месяц!"
	QueryInsertReward        = `
        INSERT INTO statistic.reward (user_id, year, month, type, value, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        RETURNING id;
    `

	QueryRewardTypeID = `
        SELECT id FROM statistic.reward_type WHERE type = $1;
    `
)

// SaveReward сохраняет награду пользователя в таблицу statistic.reward.
func (q *DB) SaveReward(ctx context.Context, userID int, year, month string, rewardType string, value string) (int, error) {
	// Поиск ID типа награды
	var rewardTypeID int
	err := q.Conn.QueryRow(ctx, QueryRewardTypeID, rewardType).Scan(&rewardTypeID)
	if err != nil {
		return 0, fmt.Errorf("failed to find reward type: %w", err)
	}

	// Вставка награды
	var rewardID int
	err = q.Conn.QueryRow(ctx, QueryInsertReward, userID, year, month, rewardTypeID, value).Scan(&rewardID)
	if err != nil {
		return 0, fmt.Errorf("failed to save reward: %w", err)
	}

	return rewardID, nil
}

func (conn *DB) TopRatingPerMonth(ctx context.Context, year string, month string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	query := `
        WITH latest_rating AS (
            SELECT
                tm.user_id,
                MAX(g.end_time) AS last_game_time
            FROM
                game.team_members tm
            JOIN
                game.team t ON tm.team_id = t.id    
            JOIN
                game.game g ON t.game_id = g.id
            WHERE
                g.type = $1
                AND DATE_TRUNC('month', g.end_time) = DATE_TRUNC('month', $2::date)
            GROUP BY
                tm.user_id
        ),
        user_ratings AS (
            SELECT
                tm.user_id,
                tm.new_rating AS rating
            FROM
                game.team_members tm
            JOIN
                game.team t ON tm.team_id = t.id
            JOIN
                game.game g ON t.game_id = g.id
            JOIN
                latest_rating lr ON tm.user_id = lr.user_id AND g.end_time = lr.last_game_time
            WHERE
                g.type = $1
        )
        SELECT
            ur.user_id,
            ur.rating
        FROM
            user_ratings ur
        JOIN
            account.user u ON ur.user_id = u.id
        ORDER BY
            ur.rating DESC
        LIMIT 1
    `

	typesName := []string{BEST_PLAYER_BY_RATING_MONTH_1x1, BEST_PLAYER_BY_RATING_MONTH_2x2, BEST_PLAYER_BY_RATING_MONTH_3x3, BEST_PLAYER_BY_RATING_MONTH_4x4, BEST_PLAYER_BY_RATING_MONTH_5x5}
	types := []string{"1x1", "2x2", "3x3", "4x4", "5x5"}
	date := fmt.Sprintf("%s-%s-01", year, month)

	for i, t := range typesName {
		var userID int
		var maxRating float64

		err = conn.Conn.QueryRow(ctx, query, types[i], date).Scan(&userID, &maxRating)

		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("No data found for type: %s and date: %s", types[i], date)
			continue
		} else if err != nil {
			return fmt.Errorf("failed to find top user for type %s: %w", types[i], err)
		}

		maxRatingString := strconv.Itoa(int(maxRating))
		if _, err = conn.SaveReward(ctx, userID, year, month, t, maxRatingString); err != nil {
			return fmt.Errorf("failed to save reward for type %s: %w", types[i], err)
		}
	}

	return nil
}

func (conn *DB) WorstRatingPerMonth(ctx context.Context, year string, month string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	query := `
        WITH latest_rating AS (
            SELECT
                tm.user_id,
                MAX(g.end_time) AS last_game_time
            FROM
                game.team_members tm
            JOIN
                game.team t ON tm.team_id = t.id    
            JOIN
                game.game g ON t.game_id = g.id
            WHERE
                g.type = $1
                AND DATE_TRUNC('month', g.end_time) = DATE_TRUNC('month', $2::date)
            GROUP BY
                tm.user_id
        ),
        user_ratings AS (
            SELECT
                tm.user_id,
                tm.new_rating AS rating
            FROM
                game.team_members tm
            JOIN
                game.team t ON tm.team_id = t.id
            JOIN
                game.game g ON t.game_id = g.id
            JOIN
                latest_rating lr ON tm.user_id = lr.user_id AND g.end_time = lr.last_game_time
            WHERE
                g.type = $1
        )
        SELECT
            ur.user_id,
            ur.rating
        FROM
            user_ratings ur
        JOIN
            account.user u ON ur.user_id = u.id
        ORDER BY
            ur.rating ASC
        LIMIT 1
    `

	typesName := []string{WORST_PLAYER_BY_RATING_MONTH_1x1, WORST_PLAYER_BY_RATING_MONTH_2x2, WORST_PLAYER_BY_RATING_MONTH_3x3, WORST_PLAYER_BY_RATING_MONTH_4x4, WORST_PLAYER_BY_RATING_MONTH_5x5}
	types := []string{"1x1", "2x2", "3x3", "4x4", "5x5"}
	date := fmt.Sprintf("%s-%s-01", year, month)

	for i, t := range typesName {
		var userID int
		var minRating float64

		err = conn.Conn.QueryRow(ctx, query, types[i], date).Scan(&userID, &minRating)

		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("No data found for type: %s and date: %s", types[i], date)
			continue
		} else if err != nil {
			return fmt.Errorf("failed to find worst user for type %s: %w", types[i], err)
		}

		minRatingString := strconv.Itoa(int(minRating))
		if _, err = conn.SaveReward(ctx, userID, year, month, t, minRatingString); err != nil {
			return fmt.Errorf("failed to save reward for type %s: %w", types[i], err)
		}
	}

	return nil
}
func (conn *DB) TopWinratePerMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
		WITH winrates AS (
			SELECT
				(SUM(CASE WHEN is_winner THEN 1 ELSE 0 END)::FLOAT / COUNT(t.game_id)::FLOAT) AS winrate,
				SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) AS win,
				COUNT(t.game_id) AS total,
				u.id,
				u.first_name,
				u.last_name,
				u.number,
				u.icon
			FROM
				game.team t
			JOIN
				game.team_members tm ON t.id = tm.team_id
			JOIN
				account.user u ON tm.user_id = u.id
            WHERE
                DATE_TRUNC('month', t.created_at) = DATE_TRUNC('month', $1::date)
			GROUP BY
				u.id
		)
		SELECT
			winrate,
			id
		FROM
			winrates
		WHERE
			total >= 10 -- Минимальное количество игр для участия в рейтинге
		ORDER BY
			winrate DESC
		LIMIT 1;
    `

	// Подготовка даты для фильтрации
	date := fmt.Sprintf("%s-%s-01", year, month) // Формат: YYYY-MM-01

	// Выполнение запроса
	var userID int
	var WinRate float64
	err = conn.Conn.QueryRow(ctx, query, date).Scan(&WinRate, &userID)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	winRateString := strconv.FormatFloat(WinRate, 'g', 2, 64)
	_, err = conn.SaveReward(ctx, userID, year, month, TOP_WINRATE_MONTH, winRateString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}
func (conn *DB) BottomWinratePerMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
		WITH winrates AS (
			SELECT
				(SUM(CASE WHEN is_winner THEN 1 ELSE 0 END)::FLOAT / COUNT(t.game_id)::FLOAT) AS winrate,
				SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) AS win,
				COUNT(t.game_id) AS total,
				u.id,
				u.first_name,
				u.last_name,
				u.number,
				u.icon
			FROM
				game.team t
			JOIN
				game.team_members tm ON t.id = tm.team_id
			JOIN
				account.user u ON tm.user_id = u.id
            WHERE
                DATE_TRUNC('month', t.created_at) = DATE_TRUNC('month', $1::date)
			GROUP BY
				u.id
		)
		SELECT
			winrate,
			id
		FROM
			winrates
		WHERE
			total >= 10 -- Минимальное количество игр для участия в рейтинге
		ORDER BY
			winrate ASC
		LIMIT 1;
    `

	// Подготовка даты для фильтрации
	date := fmt.Sprintf("%s-%s-01", year, month) // Формат: YYYY-MM-01

	// Выполнение запроса
	var userID int
	var WinRate float64
	err = conn.Conn.QueryRow(ctx, query, date).Scan(&WinRate, &userID)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	winRateString := strconv.FormatFloat(WinRate, 'g', 2, 64)
	_, err = conn.SaveReward(ctx, userID, year, month, MAX_LOSERATE_MONTH, winRateString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}

func (conn *DB) TopGainedRatingMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
		WITH user_stats AS (
			SELECT
				sum(changed_rating) AS maximum_gained,
				COUNT(t.game_id) AS total_games,
				u.id,
				u.first_name,
				u.last_name,
				u.number,
				u.icon
			FROM
				game.team t
			JOIN
				game.team_members tm ON t.id = tm.team_id
			JOIN
				account.user u ON tm.user_id = u.id
			 WHERE
				DATE_TRUNC('month', t.created_at) = DATE_TRUNC('month', $1::date)
			GROUP BY
				u.id, u.first_name, u.last_name, u.number, u.icon
		)
		SELECT
			maximum_gained,
			id
		FROM
			user_stats
		WHERE
			maximum_gained = (SELECT MAX(maximum_gained) FROM user_stats);
    `

	// Подготовка даты для фильтрации
	date := fmt.Sprintf("%s-%s-01", year, month) // Формат: YYYY-MM-01

	// Выполнение запроса
	var userID int
	var TopRatingGained int
	err = conn.Conn.QueryRow(ctx, query, date).Scan(&TopRatingGained, &userID)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	TopRatingGainedString := strconv.Itoa(TopRatingGained)
	_, err = conn.SaveReward(ctx, userID, year, month, TOP_GAINED_RATING_MONTH, TopRatingGainedString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}

func (conn *DB) TopLostRatingMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
		WITH user_stats AS (
			SELECT
				sum(changed_rating) AS maximum_gained,
				COUNT(t.game_id) AS total_games,
				u.id,
				u.first_name,
				u.last_name,
				u.number,
				u.icon
			FROM
				game.team t
			JOIN
				game.team_members tm ON t.id = tm.team_id
			JOIN
				account.user u ON tm.user_id = u.id
			 WHERE
				DATE_TRUNC('month', t.created_at) = DATE_TRUNC('month', $1::date)
			GROUP BY
				u.id, u.first_name, u.last_name, u.number, u.icon
		)
		SELECT
			maximum_gained,
			id
		FROM
			user_stats
		WHERE
			maximum_gained = (SELECT MIN(maximum_gained) FROM user_stats);
    `

	// Подготовка даты для фильтрации
	date := fmt.Sprintf("%s-%s-01", year, month) // Формат: YYYY-MM-01

	// Выполнение запроса
	var userID int
	var TopLoseRating int
	err = conn.Conn.QueryRow(ctx, query, date).Scan(&TopLoseRating, &userID)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	TopLoseRatingString := strconv.Itoa(TopLoseRating)
	_, err = conn.SaveReward(ctx, userID, year, month, MAX_LOST_RATING_MONTH, TopLoseRatingString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}

func (conn *DB) MaxGamesPlayed(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
		WITH user_game_stats AS (
			SELECT
				COUNT(DISTINCT g.id) AS games_played, -- Подсчитываем уникальные игры
				tm.user_id AS user_id
			FROM
				game.team_members tm
			JOIN
				game.team t ON t.id = tm.team_id
			JOIN
				game.game g ON g.id = t.game_id
			WHERE
				DATE_TRUNC('month', g.end_time) = DATE_TRUNC('month', $1::date)
			GROUP BY
				tm.user_id -- Группируем только по пользователю
		)
		SELECT
			ugs.games_played,
			u.id
		FROM
			user_game_stats ugs
		JOIN
			account.user u ON ugs.user_id = u.id
		ORDER BY
			ugs.games_played DESC
		LIMIT 1;
    `

	// Подготовка даты для фильтрации
	date := fmt.Sprintf("%s-%s-01", year, month) // Формат: YYYY-MM-01

	// Выполнение запроса
	var userID int
	var TopGamesPlayed int
	err = conn.Conn.QueryRow(ctx, query, date).Scan(&TopGamesPlayed, &userID)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	TopGamesPlayedString := strconv.Itoa(TopGamesPlayed)
	_, err = conn.SaveReward(ctx, userID, year, month, MAX_GAMES_PLAYED_MONTH, TopGamesPlayedString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}

func (conn DB) LongestWinStreak(ctx context.Context, year string, month string) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	query := `
    SELECT
        user_id, is_winner, t.created_at
    FROM
        game.team_members tm
    JOIN
        game.team t ON tm.team_id = t.id
    JOIN
        game.game g ON t.game_id = g.id
    WHERE
        DATE_TRUNC('month', g.end_time) = DATE_TRUNC('month', $1::date)
    ORDER BY
        g.end_time ASC -- Важно сортировать по времени в хронологическом порядке
    `

	date := fmt.Sprintf("%s-%s-01", year, month)
	rows, err := conn.Conn.Query(ctx, query, date)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Используем указатели на структуры для хранения данных
	type userGameRecord struct {
		UserID    int
		WinStreak []bool
	}
	userData := make(map[int]*userGameRecord)

	for rows.Next() {
		var (
			userID   int
			isWinner bool
			gameTime time.Time
		)
		err := rows.Scan(&userID, &isWinner, &gameTime)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Инициализируем запись, если её нет
		if _, exists := userData[userID]; !exists {
			userData[userID] = &userGameRecord{
				UserID:    userID,
				WinStreak: make([]bool, 0),
			}
		}

		// Добавляем результат игры в WinStreak
		userData[userID].WinStreak = append(userData[userID].WinStreak, isWinner)
	}

	// Подсчет максимальной серии побед
	var maxStreakUser struct {
		UserID int
		Length int
	}

	for _, record := range userData {
		currentStreak := 0
		maxStreak := 0

		for _, win := range record.WinStreak {
			if win {
				currentStreak++
				if currentStreak > maxStreak {
					maxStreak = currentStreak
				}
			} else {
				currentStreak = 0
			}
		}

		if maxStreak > maxStreakUser.Length {
			maxStreakUser.UserID = record.UserID
			maxStreakUser.Length = maxStreak
		}
	}

	// Сохранение награды, если есть победы
	if maxStreakUser.Length > 0 {
		streakValue := strconv.Itoa(maxStreakUser.Length)
		_, saveErr := conn.SaveReward(
			ctx,
			maxStreakUser.UserID,
			year,
			month,
			LONGEST_WIN_STREAK_MONTH, // Добавьте эту константу в начале файла
			streakValue,
		)
		if saveErr != nil {
			return fmt.Errorf("failed to save longest win streak: %w", saveErr)
		}
		log.Printf("Longest win streak saved for user %d (length %d)", maxStreakUser.UserID, maxStreakUser.Length)
	} else {
		log.Println("No winning streak found for the month")
	}

	return nil
}
