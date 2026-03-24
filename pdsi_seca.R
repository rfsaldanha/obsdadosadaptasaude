# Packages
library(readr)
library(dplyr)
library(lubridate)
library(tidyr)
library(readr)
library(glue)
library(zip)
library(arrow)
library(zendown)

read_parquet(
  file = "dados_brutos/terraclimate/pdsi_mean_mean.parquet"
) |>
  filter(year(date) >= 2010) |>
  mutate(ano = year(date), mes = month(date)) |>
  rename(cod_ibge = code_muni, data = date, pdsi = value) |>
  mutate(
    classificacao_seca = case_when(
      pdsi >= 4 ~ "Chuva extrema",
      pdsi >= 3 & pdsi < 4 ~ "Chuva severa",
      pdsi >= 2 & pdsi < 3 ~ "Chuva moderada",
      pdsi >= 1 & pdsi < 2 ~ "Chuva fraca",
      pdsi >= -1 & pdsi < 1 ~ "Normal",
      pdsi <= -1 & pdsi > -2 ~ "Seca fraca",
      pdsi <= -2 & pdsi > -3 ~ "Seca moderada",
      pdsi <= -3 & pdsi > -4 ~ "Seca severa",
      pdsi <= -4 ~ "Seca extrema"
    )
  ) |>
  relocate(cod_ibge, data, ano, mes, pdsi, classificacao_seca) |>
  write_csv("pdsi_seca.csv")

zip(
  zipfile = "pdsi_seca.csv.zip",
  files = "pdsi_seca.csv"
)
unlink("pdsi_seca.csv")
