package postgres

type Rating struct {
	UserID    int
	FirstName string
	LastName  string
	Number    string
	Icon      string
	MaxRating float64
}
