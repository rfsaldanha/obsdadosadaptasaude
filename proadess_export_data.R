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
library(fs)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Table
proadess_tb <- tbl(con, "proadess")

# Indicators
indi <- tibble(
  cod = c("Z050", "Z120", "Z110", "Z130", "L400", "B110"),
  name = c(
    "Enfermeiros por 100 mil habitantes",
    "Médicos por 1000 habitantes",
    "Leitos por 1000 habitantes",
    "Número de estabelecimentos hospitalares",
    "Densidade demográfica",
    "Percentual de internações por Condições Sensíveis à Atenção Primária"
  )
)

# Filter data
proadess <- proadess_tb |>
  filter(
    substr(cod, 0, 4) == "Z050" |
      substr(cod, 0, 4) == "Z120" |
      substr(cod, 0, 4) == "Z110" |
      substr(cod, 0, 4) == "Z130" & substr(cod, 9, 24) == "0000000000000200" |
      substr(cod, 0, 4) == "L400" |
      (substr(cod, 0, 4) == "B110" & substr(cod, 9, 24) == "0000000000000200")
  ) |>
  collect() |>
  mutate(
    code_indi = case_when(
      substr(cod, 0, 4) == "Z050" ~ "proadess001",
      substr(cod, 0, 4) == "Z120" ~ "proadess002",
      substr(cod, 0, 4) == "Z110" ~ "proadess003",
      substr(cod, 0, 4) == "Z130" &
        substr(cod, 9, 24) == "0000000000000200" ~ "proadess004",
      substr(cod, 0, 4) == "L400" ~ "proadess005",
      substr(cod, 0, 4) == "B110" &
        substr(cod, 9, 24) == "0000000000000200" ~ "proadess006"
    ),
    code_uf = as.numeric(substr(codmun, 0, 2)),
    ano = as.numeric(paste0("20", substr(cod, 7, 8))),
    valor = round(valor, digits = 2)
  ) |>
  select(code_indi, code_uf, code_muni = codmun, ano, valor)

# Check
proadess |>
  group_by(code_indi, ano) |>
  summarise(freq = n()) |>
  print(n = Inf)

# Collect data
res <- proadess |>
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
  .x = res,
  .f = write_csv2_mod,
  dir = "export/proadess",
  prefix = "proadess_"
)
