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
  file = "dados_brutos/arboviroses/tb_adaptação 1/tb_adaptação.csv",
  col_names = c("ind", "codmun", "anos", "mes", "valor"),
  col_types = "ciiid",
  locale = locale(decimal_mark = ".", grouping_mark = ",")
)


res |>
  dplyr::summarise(n = dplyr::n(), .by = c(codmun, anos, mes, ind)) |>
  dplyr::filter(n > 1L)


res2 <- res |>
  mutate(
    ind = recode_values(
      x = ind,
      "Dengue total" ~ "inc_dengue",
      "Zika" ~ "inc_zika",
      "Chikungunya" ~ "inc_chikungunya",
      "Leptospirose" ~ "inc_leptospirose",
      "lta" ~ "lta",
      "mal_vivax" ~ "inc_malaria_vivax",
      "mal_falciparum" ~ "inc_malaria_falciparum"
    ),
    valor = round(valor, 2)
  ) |>
  pivot_wider(
    id_cols = c(codmun, anos, mes),
    names_from = ind,
    values_from = valor
  )
