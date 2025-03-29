package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

type DB struct {
	Conn *pgxpool.Pool
}

func NewDB(url string) (*DB, error) {
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		log.Println("Can't connect to db", err)
		return nil, err
	}
	return &DB{Conn: pool}, nil
}

func (db *DB) Close() {
	db.Conn.Close()
	log.Println("Database connection closed")
}
