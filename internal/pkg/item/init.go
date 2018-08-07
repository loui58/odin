package product

import "github.com/gocql/gocql"

// Initialization
func New(options ...ProductFunc) (instance *ProductInstance, err error) {
	instance = &ProductInstance{
		cassandraSess: nil,
		keySpace:      "common",
		tableName:     "product_v1",
	}

	for _, option := range options {
		if err = option(instance); err != nil {
			return nil, err
		}
	}

	return
}

func (i *ProductInstance) GetKeyspace() (keyspace string) {
	if i == nil {
		return ""
	}
	return i.keySpace
}

// SetKeyspace set keyspace manually
func (i *ProductInstance) SetKeyspace(keyspace string) {
	if i != nil {
		i.keySpace = keyspace
	}
}

// GetTableName get table name manually
func (i *ProductInstance) GetTableName() (tablename string) {
	if i == nil {
		return ""
	}
	return i.tableName
}

// SetTableName set keyspace manually
func (i *ProductInstance) SetTableName(tablename string) {
	if i != nil {
		i.tableName = tablename
	}
}

// GetSession return cassandra session
func (i *ProductInstance) GetSession() *gocql.Session {
	if i == nil {
		return nil
	}
	return i.cassandraSess
}
