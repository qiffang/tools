package importer

import (
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"log"
)

func doInsert(table *table, db *sql.DB, count int) {
	sqls, err := genRowDatas(table, count)
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}

	for _, sql := range sqls {
		_, err = txn.Exec(sql)
		if err != nil {
			log.Fatalf(errors.ErrorStack(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}
}

func CreateDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}
