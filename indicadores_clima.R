# Packages
library(readr)
library(dplyr)
library(lubridate)
library(zendown)
library(arrow)
library(climindi)
library(glue)
library(zip)

date_start <- ymd("2000-01-01")

temp_max <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_max.parquet"),
    zen_file(10947952, "2m_temperature_max.parquet"),
    zen_file(15748125, "2m_temperature_max.parquet")
  )
) |>
  filter(name == "2m_temperature_max_mean") |>
  filter(date >= date_start) |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(temp_max = round(mean(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  rename(cod_ibge = code_muni) |>
  mutate(data = ymd(glue("{year}-{month}-1"))) |>
  mutate(ano = year) |>
  mutate(mes = month) |>
  select(cod_ibge, data, ano, mes, temp_max)

temp_min <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_min.parquet"),
    zen_file(10947952, "2m_temperature_min.parquet"),
    zen_file(15748125, "2m_temperature_min.parquet")
  )
) |>
  filter(name == "2m_temperature_min_mean") |>
  filter(date >= date_start) |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(temp_min = round(mean(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  rename(cod_ibge = code_muni) |>
  mutate(data = ymd(glue("{year}-{month}-1"))) |>
  mutate(ano = year) |>
  mutate(mes = month) |>
  select(cod_ibge, data, ano, mes, temp_min)

temp_mean <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_mean.parquet"),
    zen_file(10947952, "2m_temperature_mean.parquet"),
    zen_file(15748125, "2m_temperature_mean.parquet")
  )
) |>
  filter(name == "2m_temperature_mean_mean") |>
  filter(date >= date_start) |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(temp_media = round(mean(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  rename(cod_ibge = code_muni) |>
  mutate(data = ymd(glue("{year}-{month}-1"))) |>
  mutate(ano = year) |>
  mutate(mes = month) |>
  select(cod_ibge, data, ano, mes, temp_media)


prec <- open_dataset(
  sources = c(
    zen_file(10036212, "total_precipitation_sum.parquet"),
    zen_file(10947952, "total_precipitation_sum.parquet"),
    zen_file(15748125, "total_precipitation_sum.parquet")
  )
) |>
  filter(name == "total_precipitation_sum_mean") |>
  filter(date >= date_start) |>
  mutate(value = value * 1000) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(precip_mm = round(mean(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  rename(cod_ibge = code_muni) |>
  mutate(data = ymd(glue("{year}-{month}-1"))) |>
  mutate(ano = year) |>
  mutate(mes = month) |>
  select(cod_ibge, data, ano, mes, precip_mm)

rh <- open_dataset(
  sources = c(
    zen_file(18758355, "rh_mean_mean_1950_2022.parquet"),
    zen_file(18758355, "rh_mean_mean_2023.parquet"),
    zen_file(18758355, "rh_mean_mean_2024.parquet"),
    zen_file(18758355, "rh_mean_mean_2025.parquet")
  )
) |>
  filter(name == "rh_mean_mean") |>
  filter(date >= date_start) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(umidade_rel = round(mean(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  rename(cod_ibge = code_muni) |>
  mutate(data = ymd(glue("{year}-{month}-1"))) |>
  mutate(ano = year) |>
  mutate(mes = month) |>
  select(cod_ibge, data, ano, mes, umidade_rel)

inner_join(temp_max, temp_min) |>
  inner_join(temp_mean) |>
  inner_join(rh) |>
  inner_join(prec) |>
  select(
    cod_ibge,
    data,
    ano,
    mes,
    temp_media,
    temp_max,
    temp_min,
    precip_mm,
    umidade_rel
  ) |>
  write_csv("indicadores_clima.csv")

zip(zipfile = "indicadores_clima.csv.zip", files = "indicadores_clima.csv")
unlink("indicadores_clima.csv")
