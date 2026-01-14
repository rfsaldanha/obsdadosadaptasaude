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

# Tables
tb_obs <- tbl(con, "obs")

# OCS indicators codes
cods <- c("to0043", "to0011", "to0012")

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
res <- tb_obs |>
  filter(codind6 %in% cods) |>
  mutate(
    code_uf = substr(as.character(codmun), 0, 2),
    mes = as.numeric(substr(codind, 9, 10)),
    codind = substr(codind, 0, 6),
    valor = round(valor, digits = 2)
  ) |>
  select(
    code_ind = codind,
    code_uf,
    code_muni = codmun,
    ano = anos,
    mes,
    valor
  ) |>
  arrange(code_ind, code_uf, code_muni, ano, mes) |>
  collect()

# Yearly data
ocs_anual <- res |>
  filter(mes == 0) |>
  select(-mes) |>
  group_by(code_uf) |>
  group_split()

# Monthly data
ocs_mensal <- res |>
  filter(mes != 0) |>
  select(-mes) |>
  group_by(code_uf) |>
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
  .x = ocs_anual,
  .f = write_csv2_mod,
  dir = "export/ambiental_anual",
  prefix = "ambiental_anual_"
)

map(
  .x = ocs_mensal,
  .f = write_csv2_mod,
  dir = "export/ambiental_mensal",
  prefix = "ambiental_mensal_"
)
