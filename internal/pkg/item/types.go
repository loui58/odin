package product

import "github.com/gocql/gocql"

type ProductFunc func(*ProductInstance) error

type ProductInstance struct {
	cassandraSess *gocql.Session
	keySpace      string
	tableName     string
}

type Product struct {
	ProductID   int    `json:"product_id", db:"product_id"`
	Name        string `json:"name", db:"name"`
	CategoryID  int    `json:"cat_id", db:"cat_id"`
	Description string `json:"description", db:"description`
}
