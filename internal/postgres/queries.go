package postgres

import (
	"context"
	"fmt"
	"strconv"
)

const (
	BEST_PLAYER_BY_RATING_MONTH  = "Лучший игрок месяца по рейтингу!"
	WORST_PLAYER_BY_RATING_MONTH = "Худший игрок месяца по рейтингу!"
	TOP_WINRATE_MONTH            = "Лучший процент побед за месяц!"
	MAX_LOSERATE_MONTH           = "Худший процент побед за месяц!"
	TOP_GAINED_RATING_MONTH      = "Максимальный прирост рейтинга за месяц!"
	MAX_LOST_RATING_MONTH        = "Максимальная потеря рейтинга за месяц!"
	MAX_GAMES_PLAYED_MONTH       = "Наибольшее количество сыгранных игр за месяц!"

	QueryInsertReward = `
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

// TopRatingPerMonth находит лучшего пользователя по рейтингу за месяц и сохраняет награду.
func (conn *DB) TopRatingPerMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			// Логируем панику
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения топ пользователя за месяц
	query := `
        SELECT
            u.id AS user_id,
            AVG(r.value) AS max_rating
        FROM
            account.rating r
        JOIN
            account."user" u ON u.id = r.user_id
        GROUP BY
            u.id
        ORDER BY
            max_rating DESC
        LIMIT 1;
    `

	// Подготовка даты для фильтрации

	// Выполнение запроса
	var userID int
	var maxRating float64
	err = conn.Conn.QueryRow(ctx, query).Scan(&userID, &maxRating)
	if err != nil {
		return fmt.Errorf("failed to find top user: %w", err)
	}
	maxRatingString := strconv.Itoa(int(maxRating))
	_, err = conn.SaveReward(ctx, userID, year, month, BEST_PLAYER_BY_RATING_MONTH, maxRatingString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
	}
	return nil
}

func (conn *DB) WorstRatingPerMonth(ctx context.Context, year string, month string) (err error) {
	// Используем defer для перехвата паники
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	// Запрос для получения худшего пользователя за месяц
	query := `
        SELECT
            u.id AS user_id,
            AVG(r.value) AS min_rating
        FROM
            account.rating r
        JOIN
            account."user" u ON u.id = r.user_id
        GROUP BY
            u.id
        ORDER BY
            min_rating ASC
        LIMIT 1;
    `

	// Подготовка даты для фильтрации

	// Выполнение запроса
	var userID int
	var minRating float64
	err = conn.Conn.QueryRow(ctx, query).Scan(&userID, &minRating)
	if err != nil {
		return fmt.Errorf("failed to find worst user: %w", err)
	}
	maxRatingString := strconv.Itoa(int(minRating))
	_, err = conn.SaveReward(ctx, userID, year, month, WORST_PLAYER_BY_RATING_MONTH, maxRatingString)
	if err != nil {
		return fmt.Errorf("failed to save top user: %w", err)
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
