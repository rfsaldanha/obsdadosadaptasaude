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

tmax_eventos <- read_parquet(
  zen_file(18787683, "tmax_monthly_indi_n1981_2010.parquet")
) |>
  select(code_muni, year, month, hw3, hw5, hot_days)

tmin_eventos <- read_parquet(
  zen_file(18787683, "tmin_monthly_indi_n1981_2010.parquet")
) |>
  select(code_muni, year, month, cw3, cw5, cold_days)

prec_eventos <- read_parquet(
  zen_file(18787683, "prec_monthly_indi_n1981_2010.parquet")
) |>
  select(code_muni, year, month, rs3, rs5, d_3, d_5)

rh_eventos <- read_parquet(
  zen_file(18787683, "rh_monthly_indi_n1981_2010.parquet")
) |>
  select(code_muni, year, month, ds3, ds5, ws3, ws5, dry_days, wet_days)

inner_join(tmax_eventos, tmin_eventos) |>
  inner_join(prec_eventos) |>
  inner_join(rh_eventos) |>
  write_csv("eventos_climaticos.csv")

zip(
  zipfile = "eventos_climaticos.csv.zip",
  files = "eventos_climaticos.csv"
)
unlink("eventos_climaticos.csv")
