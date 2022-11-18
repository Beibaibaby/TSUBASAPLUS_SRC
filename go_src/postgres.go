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
  dbname    = "climatedb"
  tablename = "pairs"
)

func execDB(db *sql.DB, sqlStatementPtr *string) {
  _, err := db.Exec(*sqlStatementPtr)
  if err != nil {
    panic(err)
  }
}

func queryRowDB(db *sql.DB) {
  sqlStatement := "SELECT * FROM pairs WHERE id=1;"
  row := db.QueryRow(sqlStatement)
  var id int
  var pair string
  var st string
  err := row.Scan(&id, &pair, &st)
  fmt.Println("id: ", id)
  fmt.Println("pair: ", pair)
  fmt.Println("st: ", st)
  switch err {
    case sql.ErrNoRows:
      fmt.Println("No rows were returned!")
      return
    case nil:
      fmt.Println(user)
    default:
      panic(err)
  }
}

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

func closeDB(db *sql.DB) {
  db.Close()
}

func createNewDB(dbName string) {
  db := openDB(nil)

  sqlStatement := "CREATE DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE CREATED: ", dbName)
  closeDB(db)
}

func deleteDB(dbName string) {
  db := openDB(nil)
  defer closeDB(db)

  sqlStatement := "DROP DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE DELETED: ", dbName)
}

func createTable(db *sql.DB) {
  sqlStatement := fmt.Sprintf(
  "CREATE TABLE %s (id SERIAL PRIMARY KEY, pair CHAR(50) UNIQUE, statistics CHAR(50));", tablename)
  execDB(db, &sqlStatement)
  fmt.Println("TABLE CREATED: ", tablename)
}

func deleteTable(db *sql.DB) {
  sqlStatement := "DROP TABLE " + tablename + ";"
  execDB(db, &sqlStatement)
  fmt.Println("TABLE DELETED: ", tablename)
}

func insert(db *sql.DB, tablename string, pairname string, st string) {
  sqlStatement := fmt.Sprintf("INSERT INTO %s (pair, statistics) VALUES ('%s', '%s');", tablename, pairname, st)
  execDB(db, &sqlStatement)
}

func main() {
  createNewDB(dbname)

  // Create connection string
  dbName := dbname
  db := openDB(&dbName)

  createTable(db)

  insert(db, tablename, "01", "sfgsadga")
  insert(db, tablename, "10", "sdygergh")
  queryRowDB(db)

  deleteTable(db)

  closeDB(db)

  deleteDB(dbname)
}
