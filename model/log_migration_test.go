package model

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestMigrateLOGDBAddsContentColumns(t *testing.T) {
	oldLogDB := LOG_DB
	defer func() { LOG_DB = oldLogDB }()

	db, err := gorm.Open(sqlite.Open("file:log_migration?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	LOG_DB = db
	if err = migrateLOGDB(); err != nil {
		t.Fatal(err)
	}

	migrator := db.Migrator()
	if !migrator.HasColumn(&Log{}, "request_body") || !migrator.HasColumn(&Log{}, "response_body") {
		t.Fatal("log content columns were not migrated")
	}
}
