# Packages
library(readr)
library(dplyr)
library(lubridate)
library(zendown)
library(arrow)
library(climindi)
library(glue)
library(zip)
library(nseq)

read_parquet(zen_file(16374139, "pm25_max_mean.parquet")) |>
  mutate(
    mes = month(date),
    ano = year(date),
    acima_15 = ifelse(value > 15, 1, 0)
  ) |>
  group_by(code_muni, ano, mes) |>
  summarise(
    pm25_media_mensal = round(mean(value, na.rm = TRUE), 2),
    dias_acima_15 = sum(acima_15, na.rm = TRUE),
    episodio_3dias_consecutivos = trle_cond(
      x = acima_15,
      a_op = "gte",
      a = 3,
      b_op = "e",
      b = 1
    )
  ) |>
  ungroup() |>
  mutate(data = ymd(glue("{ano}-{mes}-1"))) |>
  rename(cod_ibge = code_muni) |>
  select(
    cod_ibge,
    data,
    ano,
    mes,
    pm25_media_mensal,
    dias_acima_15,
    episodio_3dias_consecutivos
  ) |>
  write_csv("pm25_poluicao.csv")

zip(
  zipfile = "pm25_poluicao.csv.zip",
  files = "pm25_poluicao.csv"
)
unlink("pm25_poluicao.csv")
