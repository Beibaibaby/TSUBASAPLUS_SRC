package main

import (
  "database/sql"
  "fmt"
  //"reflect"

  _ "github.com/lib/pq"
)

const (
  host      = "127.0.0.1"
  port      = 5432
  user      = "jliu158"
  password  = "99golang"
  dbname    = "jliu158"
  tablename = "pairs"
)

func openDB(dbNamePtr *string) *sql.DB {
  var psqlInfo string
  if (dbNamePtr == nil) {
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s sslmode=disable",
    host, port, user, password)
  } else {
    psqlInfo =  fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, *dbNamePtr)
  }
  // Open a connection, 1st arg: server name, 2nd arg: connection string
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    panic(err)
  }
  // Check whether or not the connection string was 100% correct
  err = db.Ping()
  if err != nil {
    panic(err)
  }
  fmt.Println("Successfully connected!")
  return db
}

func main() {
  openDB(nil)
}
