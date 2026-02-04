# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(stringr)
library(purrr)
library(readr)
library(fs)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Table
tb_ibge <- tbl(con, "ibge")

# UFs codes
ufs <- tibble(
  abbrev = c(
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO"
  ),
  code = c(
    12,
    27,
    13,
    16,
    29,
    23,
    53,
    32,
    52,
    21,
    31,
    50,
    51,
    15,
    25,
    26,
    22,
    41,
    33,
    24,
    11,
    14,
    43,
    42,
    28,
    35,
    17
  )
)

# Collect data
res <- tb_ibge |>
  group_by(code_uf) |>
  collect() |>
  group_split()

# Export data
write_csv2_mod <- function(x, dir, prefix) {
  if (!dir_exists(dir)) {
    dir_create(path = path(dir))
  }

  uf_sigla <- tolower(ufs[[which(ufs$code == x$code_uf[1]), 1]])

  write_csv2(x = x, file = path(dir, paste0(prefix, uf_sigla, ".csv")))
}

map(
  .x = res,
  .f = write_csv2_mod,
  dir = "export/ibge",
  prefix = "ibge_"
)
