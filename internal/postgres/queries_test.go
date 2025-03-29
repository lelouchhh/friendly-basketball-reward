package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"testing"
)

func TestDB_Close(t *testing.T) {
	type fields struct {
		Conn *pgxpool.Pool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{
				Conn: tt.fields.Conn,
			}
			db.Close()
		})
	}
}

func TestDB_TopRatingPerMonth(t *testing.T) {
	type fields struct {
		Conn *pgxpool.Pool
	}
	type args struct {
		ctx   context.Context
		year  string
		month string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &DB{
				Conn: tt.fields.Conn,
			}
			if err := conn.TopRatingPerMonth(tt.args.ctx, tt.args.year, tt.args.month); (err != nil) != tt.wantErr {
				t.Errorf("TopRatingPerMonth() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_WorstRatingPerMonth(t *testing.T) {
	type fields struct {
		Conn *pgxpool.Pool
	}
	type args struct {
		ctx   context.Context
		year  string
		month string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &DB{
				Conn: tt.fields.Conn,
			}
			if err := conn.WorstRatingPerMonth(tt.args.ctx, tt.args.year, tt.args.month); (err != nil) != tt.wantErr {
				t.Errorf("WorstRatingPerMonth() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewDB(t *testing.T) {
	type args struct {
		url string
	}
	tests := []struct {
		name    string
		args    args
		want    *DB
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDB(tt.args.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDB() got = %v, want %v", got, tt.want)
			}
		})
	}
}
