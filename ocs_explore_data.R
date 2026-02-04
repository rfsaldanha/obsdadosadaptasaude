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

# Explore
tb_obs |>
  # arrange(-valor) |>
  head(n = 100) |>
  collect()

# Cobertura vegetal e uso da terra
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0063")) |>
  head(10) |>
  collect()

# Concentração de material particulado fino na atmo...
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0011")) |>
  head(10) |>
  collect()

# Concentração de monóxido de carbono na atmosfera
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0012")) |>
  head(10) |>
  collect()

tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0012")) |>
  group_by(anos) |>
  distinct(codmun) |>
  summarise(freq = n()) |>
  collect() |>
  print(n = 100)

# Índice de estado da vegetação (NDVI)
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0043")) |>
  head(10) |>
  collect()

tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0043")) |>
  group_by(anos) |>
  distinct(codmun) |>
  summarise(freq = n()) |>
  collect() |>
  print(n = 100)

# Nível do rio (cotas hidrológica)
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0070")) |>
  head(10) |>
  collect()

# Percentagem de área urbana
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0045")) |>
  head(10) |>
  collect()

# Precipitação (pontual)
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0001")) |>
  head(10) |>
  collect()

tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0001")) |>
  group_by(anos) |>
  distinct(codmun) |>
  summarise(freq = n()) |>
  collect() |>
  print(n = 100)

# Proporção de áreas inundáveis
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0055")) |>
  head(10) |>
  collect()

# Taxa de desmatamento
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0062")) |>
  head(10) |>
  collect()

# Temperatura máxima (pontual)
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0002")) |>
  head(10) |>
  collect()

tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0002")) |>
  group_by(anos) |>
  distinct(codmun) |>
  summarise(freq = n()) |>
  collect() |>
  print(n = 100)

# Temperatura média (pontual)
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0005")) |>
  head(10) |>
  collect()

tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0005")) |>
  group_by(anos) |>
  distinct(codmun) |>
  summarise(freq = n()) |>
  collect() |>
  print(n = 100)

# Temperatura mínima (pontual)
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0003")) |>
  head(10) |>
  collect()

# Umidade relativa do ar (pontual)
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0006")) |>
  head(10) |>
  collect()

# Velocidade do vento (pontual)
# NÃO ENCONTRADO
tb_obs |>
  filter(str_detect(string = codind, pattern = "^to0007")) |>
  head(10) |>
  collect()

# Precipitação acumulada
# INDICADOR NÃO ENCONTRADO
