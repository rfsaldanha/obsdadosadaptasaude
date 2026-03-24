# Packages
library(readr)
library(dplyr)
library(lubridate)
library(tidyr)
library(readr)
library(glue)
library(zip)

# Dados Vanderlei
res <- read_delim(
  file = "dados_brutos/arboviroses/tb_adaptação_veri.csv",
  col_names = c("ind", "codmun", "anos", "mes", "valor"),
  col_types = "ciiid",
  locale = locale(decimal_mark = ".", grouping_mark = ",")
)


res2 <- res |>
  mutate(
    ind = recode_values(
      x = ind,
      "dengue total" ~ "inc_dengue",
      "zika" ~ "inc_zika",
      "chikungunya" ~ "inc_chikungunya",
      "leptospirose" ~ "inc_leptospirose",
      "lta" ~ "inc_leishmaniose",
      "malaria_vivax" ~ "inc_malaria_vivax",
      "malaria_falci" ~ "inc_malaria_falciparum"
    ),
    valor = round(valor, 2)
  ) |>
  pivot_wider(
    id_cols = c(codmun, anos, mes),
    names_from = ind,
    values_from = valor
  ) |>
  rename(cod_ibge = codmun)

res2 |>
  write_csv("arboviroses.csv")

zip(
  zipfile = "arboviroses.csv.zip",
  files = "arboviroses.csv"
)
unlink("arboviroses.csv")
