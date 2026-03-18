# Packages
library(readr)
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(janitor)
library(tibble)
library(purrr)
library(tidyr)
library(stringr)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Read data
proadess_raw <- read_delim(
  file = "db_proadess/adaptacao.csv",
  delim = ";",
  locale = locale(decimal_mark = ".")
)

# Write to database
dbWriteTable(conn = con, name = "proadess", value = proadess_raw)

# Database disconnect
dbDisconnect(conn = con)
