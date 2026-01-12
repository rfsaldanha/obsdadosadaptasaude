# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(stringr)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Tables
tb_obs <- tbl(con, "obs")

# Indicators
cods <- c("to0011", "to0012", "to0043", "to0001", "to0002", "to0005")

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
res <- tb_obs |>
  mutate(codind = substr(codind, 0, 6)) |>
  filter(codind %in% cods) |>
  select(code_muni = codmun, cod_ind = codind, ano = anos, valor) |>
  collect()
