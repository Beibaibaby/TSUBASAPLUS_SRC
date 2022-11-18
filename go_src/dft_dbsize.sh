#!/bin/bash

go run program.go /u/jliu158/climate_data/data.csv 3000 2000 0.75 50 5000 5000 0.75 0 8 > dft_db_50.out
go run program.go /u/jliu158/climate_data/data.csv 3000 2000 0.75 100 5000 5000 0.75 0 8 > dft_db_100.out
go run program.go /u/jliu158/climate_data/data.csv 3000 2000 0.75 150 5000 5000 0.75 0 8 > dft_db_150.out
go run program.go /u/jliu158/climate_data/data.csv 3000 2000 0.75 200 5000 5000 0.75 0 8 > dft_db_200.out
go run program.go /u/jliu158/climate_data/data.csv 3000 2000 0.75 250 5000 5000 0.75 0 8 > dft_db_250.out

exit 0
