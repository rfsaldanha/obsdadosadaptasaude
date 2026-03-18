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
library(climindi)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Datasets
temp_max <- open_dataset(
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
  group_by(code_muni, year, month) |>
  summarise(temp_max = round(mean(value, na.rm = TRUE), 2))

temp_min <- open_dataset(
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
  group_by(code_muni, year, month) |>
  summarise(temp_min = round(mean(value, na.rm = TRUE), 2))

temp_mean <- open_dataset(
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
  group_by(code_muni, year, month) |>
  summarise(temp_mean = round(mean(value, na.rm = TRUE), 2))


prec <- open_dataset(
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
  group_by(code_muni, year, month) |>
  summarise(prec = round(mean(value, na.rm = TRUE), 2))

rh <- open_dataset(
  sources = c(
    zen_file(18392587, "rh_mean_mean_2022_1950.parquet"),
    zen_file(18392587, "rh_mean_mean_2023.parquet"),
    zen_file(18392587, "rh_mean_mean_2024.parquet"),
    zen_file(18392587, "rh_mean_mean_2025.parquet")
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
  group_by(code_muni, year, month) |>
  summarise(rh = round(mean(value, na.rm = TRUE), 2))

ws <- open_dataset(
  sources = c(
    zen_file(18390794, "wind_speed_mean_mean_1950_2022.parquet"),
    zen_file(18390794, "wind_speed_mean_mean_2023.parquet"),
    zen_file(18390794, "wind_speed_mean_mean_2024.parquet"),
    zen_file(18390794, "wind_speed_mean_mean_2025.parquet")
  )
) |>
  filter(name == "wind_speed_mean_mean") |>
  select(-name) |>
  arrange(code_muni, date) |>
  mutate(
    year = year(date),
    month = month(date)
  ) |>
  collect() |>
  group_by(code_muni, year, month) |>
  summarise(ws = round(mean(value, na.rm = TRUE), 2))

# dew_point <- open_dataset(
#   sources = c(
#     zen_file(10036212, "2m_dewpoint_temperature_mean.parquet"),
#     zen_file(10947952, "2m_dewpoint_temperature_mean.parquet"),
#     zen_file(15748125, "2m_dewpoint_temperature_mean.parquet")
#   )
# ) |>
#   filter(name == "2m_dewpoint_temperature_mean_mean") |>
#   mutate(value = value - 273.15) |>
#   select(-name) |>
#   arrange(code_muni, date) |>
#   mutate(
#     year = year(date),
#     month = month(date)
#   ) |>
#   collect() |>
#   group_by(code_muni, year, month) |>
#   summarise(dew_point = round(mean(value, na.rm = TRUE), 2))

# rh <- inner_join(temp_mean, dew_point) |>
#   mutate(
#     e_actual = exp((17.625 * dew_point) / (243.04 + dew_point)),
#     e_saturation = exp((17.625 * temp_mean) / (243.04 + temp_mean)),
#     rh = round(100 * (e_actual / e_saturation), 2)
#   ) |>
#   select(code_muni, year, month, rh)

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
    year_start = 1961,
    year_end = 1990
  ) |>
  ungroup()

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
    year_start = 1961,
    year_end = 1990
  ) |>
  ungroup()

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
    year_start = 1961,
    year_end = 1990
  ) |>
  ungroup()

# temp_mean_2 <- open_dataset(
#   sources = c(
#     zen_file(10036212, "2m_temperature_mean.parquet"),
#     zen_file(10947952, "2m_temperature_mean.parquet"),
#     zen_file(15748125, "2m_temperature_mean.parquet")
#   )
# ) |>
#   filter(name == "2m_temperature_mean_mean") |>
#   mutate(value = value - 273.15) |>
#   select(-name) |>
#   arrange(code_muni, date) |>
#   mutate(
#     year = year(date),
#     month = month(date)
#   ) |>
#   rename(temp_mean = value) |>
#   collect()

# dew_point_2 <- open_dataset(
#   sources = c(
#     zen_file(10036212, "2m_dewpoint_temperature_mean.parquet"),
#     zen_file(10947952, "2m_dewpoint_temperature_mean.parquet"),
#     zen_file(15748125, "2m_dewpoint_temperature_mean.parquet")
#   )
# ) |>
#   filter(name == "2m_dewpoint_temperature_mean_mean") |>
#   mutate(value = value - 273.15) |>
#   select(-name) |>
#   arrange(code_muni, date) |>
#   mutate(
#     year = year(date),
#     month = month(date)
#   ) |>
#   rename(dew_point = value) |>
#   collect()

rh_normal <- inner_join(temp_mean_2, dew_point_2) |>
  mutate(
    e_actual = exp((17.625 * dew_point) / (243.04 + dew_point)),
    e_saturation = exp((17.625 * temp_mean) / (243.04 + temp_mean)),
    rh = round(100 * (e_actual / e_saturation), 2)
  ) |>
  select(code_muni, date, month, rh) |>
  group_by(code_muni, month) |>
  summarise_normal(
    date_var = date,
    value_var = rh,
    year_start = 1961,
    year_end = 1990
  ) |>
  ungroup()

# Write to database
dbWriteTable(conn = con, name = "temp_max", value = temp_max, overwrite = TRUE)
dbWriteTable(conn = con, name = "temp_min", value = temp_min, overwrite = TRUE)
dbWriteTable(conn = con, name = "prec", value = prec, overwrite = TRUE)
dbWriteTable(conn = con, name = "rh", value = rh, overwrite = TRUE)
dbWriteTable(conn = con, name = "ws", value = rh, overwrite = TRUE)
dbWriteTable(
  conn = con,
  name = "temp_max_normal",
  value = temp_max_normal,
  overwrite = TRUE
)
dbWriteTable(
  conn = con,
  name = "temp_min_normal",
  value = temp_min_normal,
  overwrite = TRUE
)
dbWriteTable(
  conn = con,
  name = "prec_normal",
  value = prec_normal,
  overwrite = TRUE
)
dbWriteTable(
  conn = con,
  name = "rh_normal",
  value = rh_normal,
  overwrite = TRUE
)

# Database disconnect
dbDisconnect(conn = con)
