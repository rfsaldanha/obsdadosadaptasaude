# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Data file
data_file <- "db_observatorio/valores_indicadores_prontos20251003.csv"

# Import data
duckdb_read_csv(
  conn = con,
  name = "obs",
  files = data_file,
  delim = ";",
  col.types = c(
    codmun = "BIGINT",
    codind = "VARCHAR",
    valor = "DOUBLE",
    uf = "VARCHAR",
    tema = "VARCHAR",
    anos = "INTEGER",
    codind6 = "VARCHAR"
  ),
  header = TRUE,
  na.strings = ""
)

# Database disconnect
dbDisconnect(conn = con)
