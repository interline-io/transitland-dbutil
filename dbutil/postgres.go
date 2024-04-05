package dbutil

import (
	"context"
	"database/sql"
	"errors"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/interline-io/log"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func init() {
	var _ Adapter = &PostgresAdapter{}
}

// PostgresAdapter connects to a Postgres/PostGIS database.
type PostgresAdapter struct {
	DBURL string
	db    sqlx.Ext
}

func NewPostgresAdapterFromDBX(db sqlx.Ext) *PostgresAdapter {
	return &PostgresAdapter{DBURL: "", db: db}
}

// Open the adapter.
func (adapter *PostgresAdapter) Open() error {
	if adapter.db != nil {
		return nil
	}
	db, err := sqlx.Open("postgres", adapter.DBURL)
	if err != nil {
		log.Error().Err(err).Msg("could not open database")
		return err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		log.Error().Err(err).Msgf("could not connect to database")
		return err
	}
	db.Mapper = MapperCache.Mapper
	return nil
}

// Close the adapter.
func (adapter *PostgresAdapter) Close() error {
	return nil
}

// Create an initial database schema.
func (adapter *PostgresAdapter) Create() error {
	if _, err := adapter.db.Exec("SELECT * FROM schema_migrations LIMIT 0"); err == nil {
		return nil
	}
	return errors.New("please run postgres migrations manually")
}

// DBX returns sqlx.Ext
func (adapter *PostgresAdapter) DBX() sqlx.Ext {
	return adapter.db
}

// Tx runs a callback inside a transaction.
func (adapter *PostgresAdapter) Tx(cb func(Adapter) error) error {
	var err error
	var tx *sqlx.Tx
	// Special check for wrapped connections
	commit := false
	switch a := adapter.db.(type) {
	case *sqlx.Tx:
		tx = a
	case *QueryLogger:
		if b, ok := a.Ext.(*sqlx.Tx); ok {
			tx = b
		}
	}
	// If we aren't already in a transaction, begin one, and commit at end
	if a, ok := adapter.db.(canBeginx); tx == nil && ok {
		tx, err = a.Beginx()
		commit = true
	}
	if err != nil {
		return err
	}
	adapter2 := &PostgresAdapter{DBURL: adapter.DBURL, db: &QueryLogger{Ext: tx}}
	if err2 := cb(adapter2); err2 != nil {
		if commit {
			if errTx := tx.Rollback(); errTx != nil {
				return errTx
			}
		}
		return err2
	}
	if commit {
		return tx.Commit()
	}
	return nil
}

// Sqrl returns a properly configured Squirrel StatementBuilder.
func (adapter *PostgresAdapter) Sqrl() sq.StatementBuilderType {
	return sq.StatementBuilder.RunWith(adapter.db).PlaceholderFormat(sq.Dollar)
}

// TableExists returns true if the requested table exists
func (adapter *PostgresAdapter) TableExists(t string) (bool, error) {
	qstr := `SELECT EXISTS ( SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = ?);`
	exists := false
	err := sqlx.Get(adapter.db, &exists, adapter.db.Rebind(qstr), t)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return exists, err
}

func (adapter *PostgresAdapter) Select(context.Context, sq.SelectBuilder, any) error {
	return nil
}

func (adapter *PostgresAdapter) Get(context.Context, sq.SelectBuilder, any) error {
	return nil
}

func (adapter *PostgresAdapter) Insert(context.Context, sq.InsertBuilder) (int, error) {
	return 0, nil
}

func (adapter *PostgresAdapter) Update(context.Context, sq.UpdateBuilder) error {
	return nil
}

func (adapter *PostgresAdapter) Delete(context.Context, sq.DeleteBuilder) error {
	return nil
}

func (adapter *PostgresAdapter) FindEnt(context.Context, any) error {
	return nil
}

func (adapter *PostgresAdapter) UpdateEnt(context.Context, any, ...string) error {
	return nil
}

func (adapter *PostgresAdapter) DeleteEnt(context.Context, any) error {
	return nil
}

// Insert builds and executes an insert statement for the given entity.
func (adapter *PostgresAdapter) InsertEnt(ctx context.Context, ent any) (int, error) {
	if v, ok := ent.(canUpdateTimestamps); ok {
		v.UpdateTimestamps()
	}
	table := getTableName(ent)
	header, err := MapperCache.GetHeader(ent)
	if err != nil {
		return 0, err
	}
	vals, err := MapperCache.GetInsert(ent, header)
	if err != nil {
		return 0, err
	}
	var eid sql.NullInt64
	q := adapter.Sqrl().
		Insert(table).
		Columns(header...).
		Values(vals...)
	if _, ok := ent.(canSetID); ok {
		err = q.Suffix(`RETURNING "id"`).QueryRow().Scan(&eid)
	} else {
		_, err = q.Exec()
	}
	if err != nil {
		return 0, err
	}
	if v, ok := ent.(canSetID); ok {
		v.SetID(int(eid.Int64))
	}
	return int(eid.Int64), err
}

// MultiInsert builds and executes a multi-insert statement for the given entities.
func (adapter *PostgresAdapter) MultiInsertEnts(ctx context.Context, ents []any) ([]int, error) {
	retids := []int{}
	if len(ents) == 0 {
		return retids, nil
	}
	for _, ent := range ents {
		if v, ok := ent.(canUpdateTimestamps); ok {
			v.UpdateTimestamps()
		}
	}
	header, err := MapperCache.GetHeader(ents[0])
	table := getTableName(ents[0])
	_, setid := ents[0].(canSetID)
	batchSize := 65536 / (len(header) + 1)
	for i := 0; i < len(ents); i += batchSize {
		batch := ents[i:min(i+batchSize, len(ents))]
		q := adapter.Sqrl().Insert(table).Columns(header...)
		for _, d := range batch {
			vals, _ := MapperCache.GetInsert(d, header)
			q = q.Values(vals...)
		}
		if setid {
			q = q.Suffix(`RETURNING "id"`)
			rows, err := q.Query()
			if err != nil {
				return retids, err
			}
			defer rows.Close()
			var eid sql.NullInt64
			for rows.Next() {
				err := rows.Scan(&eid)
				if err != nil {
					return retids, err
				}
				retids = append(retids, int(eid.Int64))
			}
		} else {
			_, err = q.Exec()
		}
	}
	return retids, err
}

// CopyInsert inserts data using COPY.
func (adapter *PostgresAdapter) CopyInsertEnts(ctx context.Context, ents []any) error {
	if len(ents) == 0 {
		return nil
	}
	for _, ent := range ents {
		if v, ok := ent.(canUpdateTimestamps); ok {
			v.UpdateTimestamps()
		}
	}
	// Must run in transaction
	return adapter.Tx(func(atx Adapter) error {
		a, ok := atx.DBX().(sqlx.Preparer)
		if !ok {
			return errors.New("not Preparer")
		}
		header, err := MapperCache.GetHeader(ents[0])
		if err != nil {
			return err
		}
		table := getTableName(ents[0])
		stmt, err := a.Prepare(pq.CopyIn(table, header...))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, d := range ents {
			vals, err := MapperCache.GetInsert(d, header)
			if err != nil {
				return err
			}
			_, err = stmt.Exec(vals...)
			if err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
		return nil
	})
}
