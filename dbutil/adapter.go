package dbutil

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// Adapter provides an interface for connecting to various kinds of database backends.
type Adapter interface {
	Open() error
	Close() error
	Create() error
	DBX() sqlx.Ext
	Tx(func(Adapter) error) error
	Sqrl() sq.StatementBuilderType
	TableExists(string) (bool, error)
	// Insert, Update, Delete
	Select(context.Context, sq.SelectBuilder, any) error
	Get(context.Context, sq.SelectBuilder, any) error
	Insert(context.Context, sq.InsertBuilder) (int, error)
	Update(context.Context, sq.UpdateBuilder) error
	Delete(context.Context, sq.DeleteBuilder) error
	// Entity Select, Insert, Update, Delete
	FindEnt(context.Context, any) error
	InsertEnt(context.Context, any) (int, error)
	UpdateEnt(context.Context, any, ...string) error
	DeleteEnt(context.Context, any) error
	// Multi-insert
	MultiInsertEnts(context.Context, []any) ([]int, error)
	CopyInsertEnts(context.Context, []any) error
}
