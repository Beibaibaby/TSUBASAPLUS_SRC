package main

import (
  "fmt"
  "os"
  "bufio"
  "io"
  "time"
  "strings"
  "strconv"
  "math"
  "runtime"
  // packages for db
  "database/sql"
  _ "github.com/lib/pq"
)

const (
  // const vars for db
  host              = "127.0.0.1"
  port              = 5432
  user              = "yxu103"
  password          = "99golang"
  dbname            = "climatedb"
  tablename         = "pairsbwr"
  tablenamedft      = "pairsbwrdft"
  pairsbwrschema    = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), cxy VARCHAR(10000)"
  pairsbwrheader    = "(id, pair, meanx, meany, sigmax, sigmay, cxy)"
  pairsbwrdftschema = "id INT UNIQUE NOT NULL, pair VARCHAR(30) UNIQUE NOT NULL, meanx VARCHAR(10000), meany VARCHAR(10000), sigmax VARCHAR(10000), sigmay VARCHAR(10000), cxy VARCHAR(10000), dxy VARCHAR(10000)"
  pairsbwrdftheader = "(id, pair, meanx, meany, sigmax, sigmay, cxy, dxy)"
)

type Pair struct {
  leftLocation int    // location of left stream
  rightLocation int   // location of right stream
  indexOfRow int      // row index in matrix
  indexOfCol int      // column index in matrix
}

type Point struct {
  timestamp int
  latitude int
  longitude int
  location int
  temperature float64
}

/* Struct to store basic window statistics */
type BasicWindowResult struct {
  pair Pair
  slicesOfMeanX *([]float64)
  slicesOfMeanY *([]float64)
  slicesOfSigmaX *([]float64)
  slicesOfSigmaY *([]float64)
  slicesOfCXY *([]float64)
  //granularity int
}

/* Struct to store basic window dft statistics */
type BasicWindowDFTResult struct {
  BasicWindowResult
  slicesOfDXY *([]float64)
}

/* Struct for insertion to db, unique to each other */
type SerializedPair struct {
  value string
}

/* Serialized BasicWindowResult */
type RowBWR struct {
  pair SerializedPair // leftLocation,rightLocation,indexOfRow,indexOfCol
  meanX string        // mean_x_1,mean_x_2,mean_x_3...
  meanY string        // mean_y_1,mean_y_2,mean_y_3...
  sigmaX string       // sigma_x_1,sigma_x_2,sigma_x_3...
  sigmaY string       // sigma_y_1,sigma_y_2,sigma_y_3...
  cXY string          // cxy_1,cxy_2,cxy_3...
}

/* Serialized BasicWindowDFTResult */
type RowBWRDFT struct {
  RowBWR
  dXY string          // dxy_1,dxy_2,dxy_3...
}

/* --- Functions related to database operations --- */
/* Exec handler */
func execDB(db *sql.DB, sqlStatementPtr *string) {
  _, err := db.Exec(*sqlStatementPtr)
  if err != nil {
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
    psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
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

/* Close db */
func closeDB(db *sql.DB) {
  db.Close()
}

/* Create a new database in postgreSQL */
func createNewDB(dbName string) {
  db := openDB(nil)
  // Create a new table
  sqlStatement := "CREATE DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE CREATED: ", dbName)
  closeDB(db)
  fmt.Println("Closed!")
}

/* Delete the database (dbname) when it is closed */
func deleteDB(dbName string) {
  db := openDB(nil)
  defer closeDB(db)
  // Delete the table
  sqlStatement := "DROP DATABASE " + dbName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("DATABASE DELETED: ", dbName)
  fmt.Println("Successfully deleted " + dbName + "!")
}

/* Create a table with schema in the specific database */
func createTable(db *sql.DB, tableName string, schema string) {
  sqlStatement := fmt.Sprintf("CREATE TABLE %s (%s);", tableName, schema)
  execDB(db, &sqlStatement)
  fmt.Println("TABLE CREATED: ", tableName)
}

/* Delete a table in the database */
func deleteTable(db *sql.DB, tableName string) {
  sqlStatement := "DROP TABLE " + tableName + ";"
  execDB(db, &sqlStatement)
  fmt.Println("TABLE DELETED: ", tableName)
}

/* Insert one row (basic window result) to db */
func insertRowBWR(db *sql.DB, bwr *BasicWindowResult, id int, tableName string) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  sqlStatement := fmt.Sprintf("INSERT INTO %s %s VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s');", 
  tableName, pairsbwrheader, id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY)
  execDB(db, &sqlStatement)
}

/* Append row statistics to rows statement */
func appendRowBWR(statement *strings.Builder, bwr *BasicWindowResult, id int) {
  rowBWR := RowBWR{SerializedPair{""}, "", "", "", "", ""}
  serializeBWR(bwr, &rowBWR)
  (*statement).WriteString(fmt.Sprintf(" (%d, '%s', '%s', '%s', '%s', '%s', '%s')",
  id, rowBWR.pair.value, rowBWR.meanX, rowBWR.meanY, rowBWR.sigmaX, rowBWR.sigmaY, rowBWR.cXY))
}

/* Insert rows to db */
func insertRowsBWR(db *sql.DB, statement *strings.Builder) {
  str := (*statement).String()
  execDB(db, &str)
}

/* Helper function: transfer slices of float64 to a row of string */
func slicesToString(slices *([]float64), row *string) {
  var sb strings.Builder
  for i := 0; i < len(*slices); i += 1 {
    sb.WriteString(fmt.Sprintf("%.5f", (*slices)[i]))
    if (i != len(*slices) - 1) {
      sb.WriteString(",")
    }
  }
  *row = sb.String()
  if len(*row) > 10000 {
    panic("Size of string is too large")
  }
}

/* Serialize BasicWindowResult to RowBWR in case for insertion */
func serializeBWR(bwr *BasicWindowResult, rowBWR *RowBWR) {
  // Serialize Pair
  serializedPairString := fmt.Sprintf("%d,%d,%d,%d", 
    bwr.pair.leftLocation, bwr.pair.rightLocation, bwr.pair.indexOfRow, bwr.pair.indexOfCol)
  if len(serializedPairString) > 30 {
    fmt.Println("len(serializedPairString): ", len(serializedPairString))
  }
  serializedPair := SerializedPair{serializedPairString}
  rowBWR.pair = serializedPair
  slicesToString(bwr.slicesOfMeanX, &rowBWR.meanX)
  slicesToString(bwr.slicesOfMeanY, &rowBWR.meanY)
  slicesToString(bwr.slicesOfSigmaX, &rowBWR.sigmaX)
  slicesToString(bwr.slicesOfSigmaY, &rowBWR.sigmaY)
  slicesToString(bwr.slicesOfCXY, &rowBWR.cXY)
}

/* Helper function: transfer a row of string to slices of float64 */
func stringToSlices(row *string, slices *([]float64)) {
  strSlices := strings.Split(*row, ",")
  for index, str := range strSlices {
    if index >= len(*slices) {
      fmt.Println("ERROR")
      return
    }
    floatVal, err := strconv.ParseFloat(str, 64)
    if err != nil {
      panic(err)
    }
    (*slices)[index] = floatVal
  }
}

/* Serialize RowBWR to BasicWindowResult */
func deserializRowBWR(rowBWR *RowBWR, bwr *BasicWindowResult) {
  _, err := fmt.Sscanf(rowBWR.pair.value, "%d,%d,%d,%d", &bwr.pair.leftLocation, &bwr.pair.rightLocation, &bwr.pair.indexOfRow, &bwr.pair.indexOfCol)
  if err != nil {
    panic(err)
  }
  stringToSlices(&rowBWR.meanX, bwr.slicesOfMeanX)
  stringToSlices(&rowBWR.meanY, bwr.slicesOfMeanY)
  stringToSlices(&rowBWR.sigmaX, bwr.slicesOfSigmaX)
  stringToSlices(&rowBWR.sigmaY, bwr.slicesOfSigmaY)
  stringToSlices(&rowBWR.cXY, bwr.slicesOfCXY)
}

/* Query by the range of ids, updates matrix meanwhile */
func queryRowsDB(db *sql.DB, tableName string, 
  startID int, endID int, matrix *([][]int), thres float64, numberOfBasicwindows int) {
  sqlStatement := fmt.Sprintf("SELECT * FROM %s WHERE id >= %d AND id < %d",
    tableName, startID, endID)
  rows, err := db.Query(sqlStatement)
  if err != nil {
    panic(err)
  }
  defer rows.Close()
  var rowBWR RowBWR
  for rows.Next() {
    var id int
    var pair string
    var meanX string
    var meanY string
    var sigmaX string
    var sigmaY string
    var cXY string
    err = rows.Scan(&id, &pair, &meanX, &meanY, &sigmaX, &sigmaY, &cXY)
    if err != nil {
      panic(err)
    }
    rowBWR = RowBWR{SerializedPair{pair}, meanX, meanY, sigmaX, sigmaY, cXY}
    slicesOfMeanX := make([]float64, numberOfBasicwindows)
    slicesOfMeanY := make([]float64, numberOfBasicwindows)
    slicesOfSigmaX := make([]float64, numberOfBasicwindows)
    slicesOfSigmaY := make([]float64, numberOfBasicwindows)
    slicesOfCXY := make([]float64, numberOfBasicwindows)
    var bwr BasicWindowResult = BasicWindowResult{Pair{0, 0, 0, 0}, &slicesOfMeanX, &slicesOfMeanY, &slicesOfSigmaX, &slicesOfSigmaY, &slicesOfCXY}
    deserializRowBWR(&rowBWR, &bwr)
    // Update matrix
    var corr float64 = 0
    var numerator float64 = 0
    var demoninator1 float64 = 0
    var demoninator2 float64 = 0
    meanXValue := getAvg(bwr.slicesOfMeanX)
    meanYValue := getAvg(bwr.slicesOfMeanY)
    slicesOfDeltaX := make([]float64, len(*bwr.slicesOfMeanX))
    slicesOfDeltaY := make([]float64, len(*bwr.slicesOfMeanY))
    size := len(slicesOfDeltaX)
    for i := 0; i < size; i += 1 {
      slicesOfDeltaX[i] = (*bwr.slicesOfMeanX)[i] - meanXValue
      slicesOfDeltaY[i] = (*bwr.slicesOfMeanY)[i] - meanYValue
    }
    for i := 0; i < size; i += 1 {
      numerator += (*bwr.slicesOfSigmaX)[i] * (*bwr.slicesOfSigmaY)[i] * (*bwr.slicesOfCXY)[i] + 
      slicesOfDeltaX[i] * slicesOfDeltaY[i]
      demoninator1 += (*bwr.slicesOfSigmaX)[i] * (*bwr.slicesOfSigmaX)[i] + slicesOfDeltaX[i] * slicesOfDeltaX[i]
      demoninator2 += (*bwr.slicesOfSigmaY)[i] * (*bwr.slicesOfSigmaY)[i] + slicesOfDeltaY[i] * slicesOfDeltaY[i]
    }
    corr = numerator/(math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
    if math.Abs(corr) >= thres {
      (*matrix)[bwr.pair.indexOfRow][bwr.pair.indexOfCol] = 1
      (*matrix)[bwr.pair.indexOfCol][bwr.pair.indexOfRow] = 1
    }
  }
}

/* String representation of Point */
func displayPoint(dataPoint Point) {
  fmt.Println(fmt.Sprintf("%#v", dataPoint))
}

/* Get average value of a variable-length array */
func getAvg(arr *([]float64)) float64 {
  var sum float64 = 0
  for i := 0; i < len(*arr); i += 1 {
    sum += (*arr)[i]
  }
  return sum/float64(len(*arr))
}

/* Transfer a line ([]byte) to Point */
func processLine(line []byte) Point {
  var lineString string = string(line[:])
  strSlices := strings.Split(lineString, ",")
  dataPoint := Point{-1, -1, -1, -1, 0}
  for index, str := range strSlices {
    intVal, intErr := strconv.Atoi(str)
    if (index < 3 && intErr != nil) {
      break
    }
    switch index {
      case 0:
        dataPoint.timestamp = intVal
      case 1:
        dataPoint.latitude = intVal
      case 2:
        dataPoint.longitude = intVal
      case 3:
        str = strings.TrimRight(str, "\n")
        str = strings.TrimRight(str, "\r")
        floatVal, _ := strconv.ParseFloat(str, 64)
        dataPoint.location = dataPoint.longitude + 1000 * dataPoint.latitude
        dataPoint.temperature = floatVal
      default:
        fmt.Println("WARNING: Invalid number of items!")
        break
    }
  }
  return dataPoint
}

/* Arguments: before: set timestamp limit, count: set number of locations limit */
func ReadLine(filePth string, dataMap *(map[int][]Point), 
              before int, count int) error {
  f, err := os.Open(filePth)
  if err != nil {
    return err
  }
  fmt.Println("Open file: SUCCESS")
  defer f.Close()

  memo := map[int]bool{}

  bfRd := bufio.NewReader(f)
  //i := 0
  for {
    line, err := bfRd.ReadBytes('\n')
    if err != nil {
      if err == io.EOF {
        fmt.Println(len((*dataMap)[1]))
        return nil
      }
      fmt.Println(err)
      return err
    }
    dataPoint := processLine(line)
    if dataPoint.timestamp < 0 {
      continue
    }

    if before > 0 && dataPoint.timestamp >= before {
      break
    }

    //
    if count < 0 {
      _, ok := (*dataMap)[dataPoint.location]
      if !ok {
        var points []Point
        points = append(points, dataPoint)
        (*dataMap)[dataPoint.location] = points
      } else {
        (*dataMap)[dataPoint.location] = append((*dataMap)[dataPoint.location], dataPoint)
      }
      continue
    }
    _, ok_memo := memo[dataPoint.location]
    _, ok := (*dataMap)[dataPoint.location]
    if len(memo) < count {
      if ok_memo || ok {
        panic("ERROR")
      }
      var points []Point
      points = append(points, dataPoint)
      (*dataMap)[dataPoint.location] = points
      // Update memo
      memo[dataPoint.location] = true
      //fmt.Println("location: ", dataPoint.location)
    } else {
      if (!ok_memo) {
        continue
      }
      if !ok {
        panic("ERROR!")
      }
      if (ok) {
        (*dataMap)[dataPoint.location] = append((*dataMap)[dataPoint.location], dataPoint)
        //fmt.Println("location_1: ", dataPoint.location)
      }
    }

    //displayPoint(dataPoint) // For debugging
    //i += 1
  }
  return nil
}

/* Set all items in the mastrix as 0 */
func clearMatrix(matrix *([][]int)) {
  for i := 0; i < len(*matrix); i += 1 {
    for j := 0; j < len((*matrix)[0]); j += 1 {
      (*matrix)[i][j] = 0
    }
  }
}

func checkMatrix(matrix *([][]int)) {
  sumOfConnectedPairs := 0
  for i := 0; i < len(*matrix); i += 1 {
    for j := i + 1; j < len((*matrix)[0]); j += 1 {
      if (*matrix)[i][j] == 1 {
        sumOfConnectedPairs += 1
      }
    }
  }
  fmt.Println(sumOfConnectedPairs)
}

func getLocations(dataMap *(map[int][]Point), locations *([]int)) {
  i := 0
  for key := range *dataMap {
    (*locations)[i] = key
    i += 1
  }
}

/* Helper function: get bwr from a specific pair, also get number of basic windows and store the value to the reference */
func getBasicWindowResult(dataMap *(map[int][]Point), granularity int,
  pair *Pair, bwr *BasicWindowResult, numberOfBasicwindows *int) {
  // Pair{leftLocation, rightLocation, i, j}
  leftPointsSlices := (*dataMap)[pair.leftLocation]
  rightPointsSlices := (*dataMap)[pair.rightLocation]
  *numberOfBasicwindows = len(leftPointsSlices)/granularity
  var basicWindowIndex int = 0
  // Statistics for basic windows
  var count float64 = 0
  var sumOfX float64 = 0
  var sumOfY float64 = 0
  var sumSquaredX float64 = 0
  var sumSquaredY float64 = 0
  var sumOfXY float64 = 0
  var countOfRemained float64 = 0
  var sumOfXRemained float64 = 0
  var sumOfYRemained float64 = 0
  var sumSquaredXRemained float64 = 0
  var sumSquaredYRemained float64 = 0
  var sumOfXYRemained float64 = 0
  slicesOfMeanX := make([]float64, *numberOfBasicwindows)
  slicesOfMeanY := make([]float64, *numberOfBasicwindows)
  slicesOfSigmaX := make([]float64, *numberOfBasicwindows)
  slicesOfSigmaY := make([]float64, *numberOfBasicwindows)
  slicesOfCXY := make([]float64, *numberOfBasicwindows)
  // Compute basic window statistics
  for k := 0; k < len(leftPointsSlices); k += 1 {
    countOfRemained += 1
    sumOfXRemained += leftPointsSlices[k].temperature
    sumOfYRemained += rightPointsSlices[k].temperature
    sumSquaredXRemained += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
    sumSquaredYRemained += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
    sumOfXYRemained += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    if int(countOfRemained) == granularity {
      var sigmaX float64 = math.Sqrt((sumSquaredXRemained/countOfRemained) - (sumOfXRemained*sumOfXRemained)/(countOfRemained*countOfRemained))
      var sigmaY float64 = math.Sqrt((sumSquaredYRemained/countOfRemained) - (sumOfYRemained*sumOfYRemained)/(countOfRemained*countOfRemained))
      var cXY float64 = (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained)/
                        (math.Sqrt(countOfRemained*sumSquaredXRemained - sumOfXRemained*sumOfXRemained)*
                        math.Sqrt(countOfRemained*sumSquaredYRemained - sumOfYRemained*sumOfYRemained))
      if (countOfRemained*sumOfXYRemained - sumOfXRemained*sumOfYRemained) == 0 {
        cXY = 0
      }
      // Update statistics
      count += countOfRemained
      sumOfX += sumOfXRemained
      sumOfY += sumOfYRemained
      sumSquaredX += sumSquaredXRemained
      sumSquaredY += sumSquaredYRemained
      sumOfXY += sumOfXYRemained
      slicesOfMeanX[basicWindowIndex] = sumOfXRemained/countOfRemained
      slicesOfMeanY[basicWindowIndex] = sumOfYRemained/countOfRemained
      slicesOfSigmaX[basicWindowIndex] = sigmaX
      slicesOfSigmaY[basicWindowIndex] = sigmaY
      slicesOfCXY[basicWindowIndex] = cXY
      // Reset remained values
      countOfRemained = 0
      sumOfXRemained = 0
      sumOfYRemained = 0
      sumSquaredXRemained = 0
      sumSquaredYRemained = 0
      sumOfXYRemained = 0
      // Basic Window Index increment
      basicWindowIndex += 1
    }
  }
  bwr.pair = *pair
  bwr.slicesOfMeanX = &slicesOfMeanX
  bwr.slicesOfMeanY = &slicesOfMeanY
  bwr.slicesOfSigmaX = &slicesOfSigmaX
  bwr.slicesOfSigmaY = &slicesOfSigmaY
  bwr.slicesOfCXY = &slicesOfCXY
}

/* Sketching part for TSUBASA */
func getBasicWindows(dataMap *(map[int][]Point), granularity int, 
  db *sql.DB, id *int, numberOfBasicwindows *int, blockSize int) {
  // Get locations
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  // Nested loops
  var i, j int
  *id = 0 // Set *id to 0
  var accumulate int = 0
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tablename, pairsbwrheader)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      var pair Pair = Pair{leftLocation, rightLocation, i, j}
      var bwr BasicWindowResult
      getBasicWindowResult(dataMap, granularity, &pair, &bwr, numberOfBasicwindows)
      if blockSize <= 0 {
        insertRowBWR(db, &bwr, *id, tablename)
      } else {
        // Accumulate
        if accumulate > 0 {
          statementSB.WriteString(",")
        }
        appendRowBWR(&statementSB, &bwr, *id)
        accumulate += 1
        if accumulate == blockSize {
          // Insert rows
          statementSB.WriteString(";")
          insertRowsBWR(db, &statementSB)
          // Reset values
          accumulate = 0
          statementSB.Reset()
          statementSB.WriteString(blockInsertionSQLStarter)
        }
      }
      (*id) += 1 // id increment
    }
  }
  if blockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    insertRowsBWR(db, &statementSB)
  }
}

/* TSUBASA */
func networkConstructionBW(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int) {
  //var pairWindowsMap map[Pair]BasicWindowResult
  //pairWindowsMap = make(map[Pair]BasicWindowResult)

  // Create a new database
  createNewDB(dbname)
  dbName := dbname
  db := openDB(&dbName) // Open and get the new database
  createTable(db, tablename, pairsbwrschema) // Create a new table for mapping pairs to statistics
  
  /* Sketch part */
  t0 := time.Now()
  var id int = 0
  var numberOfBasicwindows int = 0
  getBasicWindows(dataMap, granularity, db, &id, &numberOfBasicwindows, writeBlockSize)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)

  /* Query part */
  t1 := time.Now()
  // Read by blocks
  startID := 0
  endID := 0
  for startID < id {
    if startID + readBlockSize > id {
      endID = id
    } else {
      endID = startID + readBlockSize
    }
    queryRowsDB(db, tablename, startID, endID, matrix, thres, numberOfBasicwindows)
    startID = endID
  }
  /*
  for pair := range pairWindowsMap {
    bwr := pairWindowsMap[pair]
    var corr float64 = 0
    var numerator float64 = 0
    var demoninator1 float64 = 0
    var demoninator2 float64 = 0
    meanX := getAvg(bwr.slicesOfMeanX)
    meanY := getAvg(bwr.slicesOfMeanY)
    slicesOfDeltaX := make([]float64, len(*bwr.slicesOfMeanX))
    slicesOfDeltaY := make([]float64, len(*bwr.slicesOfMeanY))
    size := len(slicesOfDeltaX)
    for i := 0; i < size; i += 1 {
      slicesOfDeltaX[i] = (*bwr.slicesOfMeanX)[i] - meanX
      slicesOfDeltaY[i] = (*bwr.slicesOfMeanY)[i] - meanY
    }
    for i := 0; i < size; i += 1 {
      numerator += (*bwr.slicesOfSigmaX)[i] * (*bwr.slicesOfSigmaY)[i] * (*bwr.slicesOfCXY)[i] + 
      slicesOfDeltaX[i] * slicesOfDeltaY[i]
      demoninator1 += (*bwr.slicesOfSigmaX)[i] * (*bwr.slicesOfSigmaX)[i] + slicesOfDeltaX[i] * slicesOfDeltaX[i]
      demoninator2 += (*bwr.slicesOfSigmaY)[i] * (*bwr.slicesOfSigmaY)[i] + slicesOfDeltaY[i] * slicesOfDeltaY[i]
    }
    corr = numerator/(math.Sqrt(demoninator1) * math.Sqrt(demoninator2))
    if math.Abs(corr) >= thres {
      (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
      (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
    }
  }
  */
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)
  deleteTable(db, tablename) // Delete the table
  closeDB(db) // Close the database

  // Delete the database
  deleteDB(dbname)
}

/* TSUBASA DFT */
func networkConstructionBWDFT(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int) {


}

func networkConstructionNaive(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  sumOfConnectedPairs := 0
  var i, j int
  for i = 0; i < locationsNum; i += 1 {
    for j = i + 1; j < locationsNum; j += 1 {
      var leftLocation int = locations[i]
      var rightLocation int = locations[j]
      leftPointsSlices := (*dataMap)[leftLocation]
      rightPointsSlices := (*dataMap)[rightLocation]
      var count float64 = 0
      var sumOfX float64 = 0
      var sumOfY float64 = 0
      var sumSquaredX float64 = 0
      var sumSquaredY float64 = 0
      var sumOfXY float64 = 0
      var k int
      for k = 0; k < len(leftPointsSlices); k += 1 {
        count += 1
        sumOfX += leftPointsSlices[k].temperature
        sumOfY += rightPointsSlices[k].temperature
        sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
        sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
        sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
      }
      std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
      (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
        math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
      if math.Abs(std) >= thres {
        (*matrix)[i][j] = 1
        (*matrix)[j][i] = 1
        sumOfConnectedPairs += 1
      }
    }
  }
  fmt.Println(sumOfConnectedPairs)
}

/* ---|--------------------|--- */
/* ---| Parallel Computing |--- */
/* ---|____________________|--- */
func getNumCPU() int {
  return runtime.NumCPU()
}

/* Partition data to NCPU lists */
func partitionData(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair)) {
  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  locationsNum := len(*dataMap)
  locations := make([]int, locationsNum)
  getLocations(dataMap, &locations)
  numOfPairs := (locationsNum * (locationsNum - 1)) / 2
  quotient := numOfPairs / NCPU
  remained := numOfPairs % NCPU
  for i := 0; i < NCPU; i += 1 {
    if (i < remained) {
      (*listOfPairs)[i] = make([]Pair, quotient + 1)
    } else {
      (*listOfPairs)[i] = make([]Pair, quotient)
    }
  }
  indexOfRow := 0
  indexOfCol := 1
  for i := 0; i < NCPU; i += 1 {
    for j := 0; j < len((*listOfPairs)[i]); j += 1 {
      (*listOfPairs)[i][j] = Pair{locations[indexOfRow], locations[indexOfCol], indexOfRow, indexOfCol}
      indexOfCol += 1
      if indexOfCol == locationsNum {
        indexOfRow += 1
        indexOfCol = indexOfRow + 1
      }
    }
  }
  fmt.Println(indexOfRow)
  fmt.Println(indexOfCol)
  fmt.Println("Assigned locations: FINISHED")
}

func doAllNaive(NCPU int, dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  sem := make(chan int, NCPU)

  // Separate the data map by NCPU
  // The pairs of locations locations are assigned to the list evenly
  listOfPairs := make([][]Pair, NCPU)
  partitionData(NCPU, dataMap, &listOfPairs)

  // doPart
  for i := 0; i < NCPU; i += 1 {
    go doPartNaive(sem, i, &listOfPairs, dataMap, matrix, thres)
  }

  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks are finished.")
}

func doPartNaive(sem chan int, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  for i := 0; i < len((*listOfPairs)[taskNum]); i += 1 {
    pair := (*listOfPairs)[taskNum][i]
    leftPointsSlices := (*dataMap)[pair.leftLocation]
    rightPointsSlices := (*dataMap)[pair.rightLocation]
    var count float64 = 0
    var sumOfX float64 = 0
    var sumOfY float64 = 0
    var sumSquaredX float64 = 0
    var sumSquaredY float64 = 0
    var sumOfXY float64 = 0
    var k int
    for k = 0; k < len(leftPointsSlices); k += 1 {
      count += 1
      sumOfX += leftPointsSlices[k].temperature
      sumOfY += rightPointsSlices[k].temperature
      sumSquaredX += leftPointsSlices[k].temperature * leftPointsSlices[k].temperature
      sumSquaredY += rightPointsSlices[k].temperature * rightPointsSlices[k].temperature
      sumOfXY += leftPointsSlices[k].temperature * rightPointsSlices[k].temperature
    }
    std := ((sumOfXY/count) - (sumOfX*sumOfY)/(count*count))/
    (math.Sqrt((sumSquaredX/count) - ((sumOfX*sumOfX)/(count*count)))*
      math.Sqrt((sumSquaredY/count) - ((sumOfY*sumOfY)/(count*count))))
    if math.Abs(std) >= thres {
      (*matrix)[pair.indexOfRow][pair.indexOfCol] = 1
      (*matrix)[pair.indexOfCol][pair.indexOfRow] = 1
    }
  }
  // Signal that the part is done
  sem <-1
}

func doAllBWSketch(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  db *sql.DB, granularity int, writeBlockSize int, numberOfBasicwindows *int) {
  sem := make(chan int, NCPU)

  // doPart
  for i := 0; i < NCPU; i += 1 {
    go doPartBWSketch(sem, i, listOfPairs, dataMap, db, granularity, writeBlockSize, numberOfBasicwindows)
  }

  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks for sketching are finished.")
}

func doPartBWSketch(sem chan int, taskNum int, listOfPairs *([][]Pair), dataMap *(map[int][]Point), 
  db *sql.DB, granularity int, writeBlockSize int, numberOfBasicwindows *int) {

  var accumulate int = 0
  tableName := fmt.Sprintf("%s_%d", tablename, taskNum)
  blockInsertionSQLStarter := fmt.Sprintf("INSERT INTO %s %s VALUES ", tableName, pairsbwrheader)
  var statementSB strings.Builder
  statementSB.WriteString(blockInsertionSQLStarter)

  for i := 0; i < len((*listOfPairs)[taskNum]); i += 1 {
    pair := (*listOfPairs)[taskNum][i]
    var bwr BasicWindowResult
    getBasicWindowResult(dataMap, granularity, &pair, &bwr, numberOfBasicwindows)

    if writeBlockSize <= 0 {
      insertRowBWR(db, &bwr, i, tableName) // i is id
    } else {
      // Accumulate
      if accumulate > 0 {
        statementSB.WriteString(",")
      }
      appendRowBWR(&statementSB, &bwr, i) // i is id
      accumulate += 1
      if accumulate == writeBlockSize {
        // Insert rows
        statementSB.WriteString(";")
        insertRowsBWR(db, &statementSB)
        // Reset values
        accumulate = 0
        statementSB.Reset()
        statementSB.WriteString(blockInsertionSQLStarter)
      }
    }
  }
  if writeBlockSize > 0 && accumulate > 0 {
    // Insert remained rows
    statementSB.WriteString(";")
    insertRowsBWR(db, &statementSB)
  }

  // Signal that the part is done
  sem <-1
}

func doAllBWQuery(NCPU int, dataMap *(map[int][]Point), listOfPairs *([][]Pair),
  matrix *([][]int), db *sql.DB, thres float64, readBlockSize int, numberOfBasicwindows int) {
  sem := make(chan int, NCPU)
  // doPart
  for i := 0; i < NCPU; i += 1 {
    tableName := fmt.Sprintf("%s_%d", tablename, i)
    go doPartBWQuery(sem, i, listOfPairs, matrix, db, thres, tableName, readBlockSize, numberOfBasicwindows)
  }
  // Waiting for NCPU tasks to be finished
  for i := 0; i < NCPU; i += 1 {
    <-sem
  }
  fmt.Println("All tasks for querying are finished.")
}

func doPartBWQuery(sem chan int, taskNum int, listOfPairs *([][]Pair),
  matrix *([][]int), db *sql.DB, thres float64, tableName string, readBlockSize int, numberOfBasicwindows int) {
  var totalCnt int = len((*listOfPairs)[taskNum])
  // Read by blocks
  startID := 0
  endID := 0
  for startID < totalCnt {
    if startID + readBlockSize > totalCnt {
      endID = totalCnt
    } else {
      endID = startID + readBlockSize
    }
    queryRowsDB(db, tableName, startID, endID, matrix, thres, numberOfBasicwindows)
    startID = endID
  }

  // Signal that the part is done
  sem <-1
}

/* Construct network for naive implemetation with parallel computing */
func networkConstructionNaiveParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)
  doAllNaive(NCPU, dataMap, matrix, thres)
}


func networkConstructionBWParallel(dataMap *(map[int][]Point), matrix *([][]int), thres float64, granularity int, 
  writeBlockSize int, readBlockSize int) {
  NCPU := getNumCPU()
  fmt.Println("CPU Num: ", NCPU)
  runtime.GOMAXPROCS(NCPU)

  // Create a new database
  dbName := fmt.Sprintf("%s", dbname)
  createNewDB(dbName)
  db := openDB(&dbName) // Open and get the new database

  // Create NCPU tables
  for i := 0; i < NCPU; i += 1 {
    tableName := fmt.Sprintf("%s_%d", tablename, i)
    createTable(db, tableName, pairsbwrschema) // Create a new table for mapping pairs to basic window statistics
  }

  var numberOfBasicwindows int = 0
  listOfPairs := make([][]Pair, NCPU)
  partitionData(NCPU, dataMap, &listOfPairs)

  t0 := time.Now()
  doAllBWSketch(NCPU, dataMap, &listOfPairs, db, granularity, writeBlockSize, &numberOfBasicwindows)
  elapsed := time.Since(t0)
  fmt.Println("Sketch time: ", elapsed)
  t1 := time.Now()
  doAllBWQuery(NCPU, dataMap, &listOfPairs, matrix, db, thres, readBlockSize, numberOfBasicwindows)
  elapsed = time.Since(t1)
  fmt.Println("Query time: ", elapsed)

  // Delete tables
  for i := 0; i < NCPU; i += 1 {
    tableName := fmt.Sprintf("%s_%d", tablename, i)
    deleteTable(db, tableName)
  }

  closeDB(db) // Close the database
  deleteDB(dbName) // Delete the database
}


func main() {
  if len(os.Args) != 8 {
    panic("Invalid number of arguments.")
  }
  fileName := os.Args[1]
  intVal, _ := strconv.Atoi(os.Args[2])
  var before int = intVal
  intVal, _ = strconv.Atoi(os.Args[3])
  var numOfLocations int = intVal
  floatVal, _ := strconv.ParseFloat(os.Args[4], 64)
  var thres float64 = floatVal
  intVal, _ = strconv.Atoi(os.Args[5])
  var granularity int = intVal
  intVal, _ = strconv.Atoi(os.Args[6])
  var writeBlockSize int = intVal
  intVal, _ = strconv.Atoi(os.Args[7])
  var readBlockSize int = intVal
  inputArgs := fmt.Sprintf("fileName: %s, before: %d, numOfLocations: %d, thres: %.2f, granularity: %d, writeBlockSize: %d, readBlockSize: %d", 
    fileName, before, numOfLocations, thres, granularity, writeBlockSize, readBlockSize)
  fmt.Println(inputArgs)

  // Read data from *.csv to map, which is stored in memory
  t1 := time.Now()
  dataMap := make(map[int][]Point)
  readErr := ReadLine(fileName, &dataMap, before, numOfLocations) // 3rd: timestamp limit, 4th: number of locations
  if (readErr != nil) {
    panic(readErr)
  }
  fmt.Println("Length of dataMap: ", len(dataMap))
  /*for k, v := range(dataMap) {
    fmt.Println("k: ", k)
    fmt.Println(len(v))
  }*/
  elapsed := time.Since(t1)
  fmt.Println("Read time: ", elapsed)
  fmt.Println("Read: FINISHED")

  // Matrix initiation
  matrix := make([][]int, len(dataMap))
  for i := range matrix {
    matrix[i] = make([]int, len(dataMap))
  }

  // Naive implementation without parallel computing
  /*t2 := time.Now()
  networkConstructionNaive(&dataMap, &matrix, thres)
  elapsed = time.Since(t2)
  fmt.Println("Construction time: ", elapsed)
  // Naive implementation with parallel computing
  clearMatrix(&matrix)
  t3 := time.Now()
  networkConstructionNaiveParallel(&dataMap, &matrix, thres)
  elapsed = time.Since(t3)
  checkMatrix(&matrix)
  fmt.Println("Construction time: ", elapsed)*/

  // TSUBASA without parallel computing, integreted with PostgreSQL
  clearMatrix(&matrix)
  t4 := time.Now()
  networkConstructionBW(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize)
  elapsed = time.Since(t4)
  checkMatrix(&matrix)
  fmt.Println("Running time: ", elapsed)

  // TSUBASA with parallel computing, integreted with PostgreSQL
  clearMatrix(&matrix)
  t5 := time.Now()
  networkConstructionBWParallel(&dataMap, &matrix, thres, granularity, writeBlockSize, readBlockSize)
  elapsed = time.Since(t5)
  checkMatrix(&matrix)
  fmt.Println("Running time: ", elapsed)

  // TSUBASA DFT without parallel computing, integreted with PostgreSQL
  /*clearMatrix(&matrix)
  t6 := time.Now()
  elapsed = time.Since(t6)
  checkMatrix(&matrix)
  fmt.Println("Running time: ", elapsed)*/

}
