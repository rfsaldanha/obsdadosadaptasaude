# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Tables
tb_obs <- tbl(con, "obs")

# Explore
tb_obs |>
  head() |>
  collect()
