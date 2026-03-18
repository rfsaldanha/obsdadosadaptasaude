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

temp_max_normal <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_max.parquet"),
    zen_file(10947952, "2m_temperature_max.parquet"),
    zen_file(15748125, "2m_temperature_max.parquet")
  )
) |>
  filter(name == "2m_temperature_max_mean") |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  ungroup() |>
  select(cod_ibge = code_muni, mes = month, temp_max_normal = normal_mean)

temp_min_normal <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_min.parquet"),
    zen_file(10947952, "2m_temperature_min.parquet"),
    zen_file(15748125, "2m_temperature_min.parquet")
  )
) |>
  filter(name == "2m_temperature_min_mean") |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  ungroup() |>
  select(cod_ibge = code_muni, mes = month, temp_min_normal = normal_mean)

temp_mean_normal <- open_dataset(
  sources = c(
    zen_file(10036212, "2m_temperature_mean.parquet"),
    zen_file(10947952, "2m_temperature_mean.parquet"),
    zen_file(15748125, "2m_temperature_mean.parquet")
  )
) |>
  filter(name == "2m_temperature_mean_mean") |>
  mutate(value = value - 273.15) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  ungroup() |>
  select(cod_ibge = code_muni, mes = month, temp_media_normal = normal_mean)

prec_normal <- open_dataset(
  sources = c(
    zen_file(10036212, "total_precipitation_sum.parquet"),
    zen_file(10947952, "total_precipitation_sum.parquet"),
    zen_file(15748125, "total_precipitation_sum.parquet")
  )
) |>
  filter(name == "total_precipitation_sum_mean") |>
  mutate(value = value * 1000) |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  ungroup() |>
  select(cod_ibge = code_muni, mes = month, precip_normal_mm = normal_mean)

rh_normal <- open_dataset(
  sources = c(
    zen_file(18758355, "rh_mean_mean_1950_2022.parquet"),
    zen_file(18758355, "rh_mean_mean_2023.parquet"),
    zen_file(18758355, "rh_mean_mean_2024.parquet"),
    zen_file(18758355, "rh_mean_mean_2025.parquet")
  )
) |>
  filter(name == "rh_mean_mean") |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = value,
    year_start = 1981,
    year_end = 2010
  ) |>
  ungroup() |>
  select(cod_ibge = code_muni, mes = month, umidade_rel_normal = normal_mean)

inner_join(temp_max_normal, temp_min_normal) |>
  inner_join(temp_mean_normal) |>
  inner_join(rh_normal) |>
  inner_join(prec_normal) |>
  select(
    cod_ibge,
    mes,
    temp_media_normal,
    temp_max_normal,
    temp_min_normal,
    precip_normal_mm,
    umidade_rel_normal
  ) |>
  write_csv("normais_climatologicas.csv")

zip(
  zipfile = "normais_climatologicas.csv.zip",
  files = "normais_climatologicas.csv"
)
unlink("normais_climatologicas.csv")
