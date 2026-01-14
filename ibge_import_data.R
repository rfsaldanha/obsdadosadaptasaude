# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(sidrar)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

res <- get_sidra(
  x = 5817,
  # variable = 529,
  period = "2024",
  geo = "State",
  header = TRUE
  # format = 4
)
