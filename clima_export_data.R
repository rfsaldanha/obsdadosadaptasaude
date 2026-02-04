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
library(zendown)
library(arrow)
library(fs)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Tables
temp_max <- tbl(con, "temp_max") |>
  mutate(
    code_indi = "tmax",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    ano = year,
    mes = month,
    valor = temp_max
  ) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

temp_min <- tbl(con, "temp_min") |>
  mutate(
    code_indi = "tmin",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    ano = year,
    mes = month,
    valor = temp_min
  ) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

prec <- tbl(con, "prec") |>
  mutate(
    code_indi = "prec",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    ano = year,
    mes = month,
    valor = prec
  ) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

rh <- tbl(con, "rh") |>
  mutate(
    code_indi = "rh",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    ano = year,
    mes = month,
    valor = rh
  ) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

temp_max_normal <- tbl(con, "temp_max_normal") |>
  mutate(
    code_indi = "temp_max_normal",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    mes = month,
    valor = normal_mean
  ) |>
  mutate(valor = round(valor, 2)) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

temp_min_normal <- tbl(con, "temp_min_normal") |>
  mutate(
    code_indi = "temp_min_normal",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    mes = month,
    valor = normal_mean
  ) |>
  mutate(valor = round(valor, 2)) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

prec_normal <- tbl(con, "prec_normal") |>
  mutate(
    code_indi = "prec_normal",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    mes = month,
    valor = normal_mean
  ) |>
  mutate(valor = round(valor, 2)) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

rh_normal <- tbl(con, "rh_normal") |>
  mutate(
    code_indi = "rh_normal",
    code_uf = as.numeric(substr(as.character(code_muni), 0, 2))
  ) |>
  select(
    code_indi,
    code_uf,
    code_muni,
    mes = month,
    valor = normal_mean
  ) |>
  mutate(valor = round(valor, 2)) |>
  group_by(code_uf) |>
  collect() |>
  group_split()

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

# Export data
write_csv2_mod <- function(x, dir, prefix) {
  if (!dir_exists(dir)) {
    dir_create(path = path(dir))
  }

  uf_sigla <- tolower(ufs[[which(ufs$code == x$code_uf[1]), 1]])

  write_csv2(x = x, file = path(dir, paste0(prefix, uf_sigla, ".csv")))
}

map(
  .x = temp_max,
  .f = write_csv2_mod,
  dir = "export/clima/tmax",
  prefix = "tmax_"
)

map(
  .x = temp_min,
  .f = write_csv2_mod,
  dir = "export/clima/tmin",
  prefix = "tmin_"
)

map(
  .x = prec,
  .f = write_csv2_mod,
  dir = "export/clima/prec",
  prefix = "prec_"
)

map(
  .x = rh,
  .f = write_csv2_mod,
  dir = "export/clima/rh",
  prefix = "rh_"
)

map(
  .x = temp_max_normal,
  .f = write_csv2_mod,
  dir = "export/clima/tmax_normal",
  prefix = "tmax_normal_"
)

map(
  .x = temp_min_normal,
  .f = write_csv2_mod,
  dir = "export/clima/tmin_normal",
  prefix = "tmin_normal_"
)

map(
  .x = prec_normal,
  .f = write_csv2_mod,
  dir = "export/clima/prec_normal",
  prefix = "prec_normal_"
)

map(
  .x = rh_normal,
  .f = write_csv2_mod,
  dir = "export/clima/rh_normal",
  prefix = "rh_normal_"
)
