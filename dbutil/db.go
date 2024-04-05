package dbutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/interline-io/log"
	"github.com/interline-io/transitland-dbutil/tags"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

var MapperCache = tags.NewCache(reflectx.NewMapperFunc("db", tags.ToSnakeCase))

func OpenDB(url string) (*sqlx.DB, error) {
	db, err := sqlx.Open("postgres", url)
	if err != nil {
		log.Error().Err(err).Msg("could not open database")
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		log.Error().Err(err).Msgf("could not connect to database")
		return nil, err
	}
	db.Mapper = MapperCache.Mapper
	return db.Unsafe(), nil
}

// Select runs a query and reads results into dest.
func Select(ctx context.Context, db sqlx.Ext, q sq.SelectBuilder, dest interface{}) error {
	useStatement := false
	qstr, qargs, err := q.ToSql()
	if err != nil {
		return err
	}
	qstr = db.Rebind(qstr)
	if a, ok := db.(sqlx.PreparerContext); ok && useStatement {
		stmt, prepareErr := sqlx.PreparexContext(ctx, a, qstr)
		if prepareErr != nil {
			err = prepareErr
		} else {
			err = stmt.SelectContext(ctx, dest, qargs...)
		}
	} else if a, ok := db.(sqlx.QueryerContext); ok {
		err = sqlx.SelectContext(ctx, a, dest, qstr, qargs...)
	} else {
		err = sqlx.Select(db, dest, qstr, qargs...)
	}

	// Check for canceled query
	if ctx.Err() == context.Canceled {
		log.Trace().Err(err).Str("query", qstr).Interface("args", qargs).Msg("query canceled")
	} else if err != nil {
		log.Error().Err(err).Str("query", qstr).Interface("args", qargs).Msg("query failed")
	}
	return err
}

// Select runs a query and reads results into dest.
func Get(ctx context.Context, db sqlx.Ext, q sq.SelectBuilder, dest interface{}) error {
	useStatement := false
	qstr, qargs, err := q.ToSql()
	if err != nil {
		return err
	}
	qstr = db.Rebind(qstr)
	if a, ok := db.(sqlx.PreparerContext); ok && useStatement {
		stmt, prepareErr := sqlx.PreparexContext(ctx, a, qstr)
		if prepareErr != nil {
			err = prepareErr
		} else {
			err = stmt.GetContext(ctx, dest, qargs...)
		}
	} else if a, ok := db.(sqlx.QueryerContext); ok {
		err = sqlx.GetContext(ctx, a, dest, qstr, qargs...)
	} else {
		err = sqlx.Get(db, dest, qstr, qargs...)
	}

	// Check for canceled query
	if ctx.Err() == context.Canceled {
		log.Trace().Err(err).Str("query", qstr).Interface("args", qargs).Msg("query canceled")
	} else if err != nil {
		log.Error().Err(err).Str("query", qstr).Interface("args", qargs).Msg("query failed")
	}
	return err
}

func Delete(ctx context.Context, db sqlx.Ext, q sq.DeleteBuilder) error {
	qstr, qargs, err := q.ToSql()
	if err != nil {
		return err
	}
	qstr = db.Rebind(qstr)
	if a, ok := db.(sqlx.ExecerContext); ok {
		_, err = a.ExecContext(ctx, qstr, qargs...)
	} else {
		_, err = db.Exec(qstr, qargs...)
	}
	return err
}

func Update(ctx context.Context, db sqlx.Ext, q sq.UpdateBuilder) error {
	qstr, qargs, err := q.ToSql()
	if err != nil {
		return err
	}
	qstr = db.Rebind(qstr)
	if a, ok := db.(sqlx.ExecerContext); ok {
		_, err = a.ExecContext(ctx, qstr, qargs...)
	} else {
		_, err = db.Exec(qstr, qargs...)
	}
	return err
}

// Entity helpers

type canBeginx interface {
	Beginx() (*sqlx.Tx, error)
}

type hasTableName interface {
	TableName() string
}

type canSetID interface {
	GetID() int
	SetID(int)
}

type canUpdateTimestamps interface {
	UpdateTimestamps()
}

func FindEnt(ctx context.Context, db sqlx.Ext, ent any) error {
	table := getTableName(ent)
	entId := 0
	if v, ok := ent.(canSetID); ok {
		entId = v.GetID()
	} else {
		return errors.New("cannot get ID")
	}
	q := sq.StatementBuilder.Select("*").From(table).Where(sq.Eq{"id": entId})
	return Get(ctx, db, q, ent)
}

func UpdateEnt(ctx context.Context, db sqlx.Ext, ent any, columns []string) error {
	if v, ok := ent.(canUpdateTimestamps); ok {
		v.UpdateTimestamps()
	}
	entId := 0
	if v, ok := ent.(canSetID); ok {
		entId = v.GetID()
	} else {
		return errors.New("cannot get ID")
	}
	table := getTableName(ent)
	header, err := MapperCache.GetHeader(ent)
	if err != nil {
		return err
	}
	vals, err := MapperCache.GetInsert(ent, header)
	if err != nil {
		return err
	}
	colmap := make(map[string]interface{})
	for i, col := range header {
		if len(columns) > 0 && !contains(col, columns) {
			continue
		}
		colmap[col] = vals[i]
	}
	q := sq.StatementBuilder.
		Update(table).
		Where(sq.Eq{"id": entId}).
		SetMap(colmap)
	return Update(ctx, db, q)
}

func InsertEnt(ctx context.Context, db sqlx.Ext, ent any) (int, error) {
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

	// Prepare statement
	var eid sql.NullInt64
	q := sq.StatementBuilder.
		Insert(table).
		Columns(header...).
		Values(vals...)
	pgCheck := (db.DriverName() == "postgres")
	if pgCheck {
		q = q.Suffix(`RETURNING "id"`)
	}

	qstr, qargs, err := q.ToSql()
	if err != nil {
		return 0, err
	}
	qstr = db.Rebind(qstr)

	// Exec
	// Check for postgres vs sqlite
	entId := 0
	if pgCheck {
		row := db.QueryRowx(qstr, qargs...)
		if err := row.Err(); err != nil {
			return 0, err
		}
		row.Scan(&entId)
	} else {
		res, err := db.Exec(qstr, qargs...)
		if err != nil {
			return 0, err
		}
		if a, err := res.LastInsertId(); err != nil {
			return 0, err
		} else {
			entId = int(a)
		}
	}

	// Set ID
	if v, ok := ent.(canSetID); ok {
		v.SetID(int(eid.Int64))
	}
	return int(eid.Int64), err
}

// helpers

func getTableName(ent interface{}) string {
	if v, ok := ent.(hasTableName); ok {
		return v.TableName()
	}
	s := strings.Split(fmt.Sprintf("%T", ent), ".")
	return tags.ToSnakeCase(s[len(s)-1])
}

func contains(a string, b []string) bool {
	for _, v := range b {
		if a == v {
			return true
		}
	}
	return false
}
