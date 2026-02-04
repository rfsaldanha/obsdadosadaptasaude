# Packages
library(DBI)
library(duckdb)
library(dplyr)
library(lubridate)
library(sidrar)
library(janitor)
library(tibble)
library(geobr)
library(purrr)
library(tidyr)
library(stringr)

# Setup
# mirai::daemons(n = 6)

# Municipality codes
mun2022 <- read_municipality(year = 2022) |>
  select(code_muni) |>
  sf::st_drop_geometry() |>
  pull(code_muni)

# Database connection
con <- dbConnect(duckdb(), "db_adaptasaude.duckdb")

# Tabela 9605 - População residente, por cor ou raça, nos Censos Demográficos
# res_9605_raw <- get_sidra(
#   x = 9605,
#   period = "2022",
#   geo = "City",
#   header = TRUE
# )

# saveRDS(object = res_9605_raw, file = "res_9605_raw.rds", compress = "xz")

res_9605_raw <- readRDS("res_9605_raw.rds")

res_9605 <- res_9605_raw |>
  clean_names() |>
  as_tibble() |>
  select(code_indi = cor_ou_raca, code_muni = municipio_codigo, ano, valor) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano),
    code_indi = case_match(
      .x = code_indi,
      "Total" ~ "ibge001",
      "Branca" ~ "ibge002",
      "Preta" ~ "ibge003",
      "Amarela" ~ "ibge004",
      "Parda" ~ "ibge005",
      "Indígena" ~ "ibge006",
    )
  ) |>
  relocate(code_uf, .before = code_muni) |>
  arrange(code_indi, code_uf, code_muni, ano)

# Tabela 9514 - População residente, por sexo, idade e forma de declaração da idade
# res_9514_raw <- map(
#   .x = mun2022,
#   .f = in_parallel(
#     \(x) {
#       retry::retry(
#         expr = sidrar::get_sidra(
#           x = 9514,
#           period = "2022",
#           geo = "City",
#           geo.filter = list("City" = x),
#           header = TRUE
#         ),
#         until = function(val, cnd) is.data.frame(val),
#         max_tries = 10
#       )
#     }
#   )
# ) |>
#   list_rbind()

# saveRDS(object = res_9514_raw, file = "res_9514_raw.rds", compress = "xz")

res_9514_raw <- readRDS("res_9514_raw.rds")

res_9514 <- res_9514_raw |>
  clean_names() |>
  as_tibble() |>
  filter(forma_de_declaracao_da_idade_codigo == 113635) |>
  select(sexo, idade, code_muni = municipio_codigo, ano, valor) |>
  filter(
    idade %in%
      c(
        "0 a 4 anos",
        "5 a 9 anos",
        "10 a 14 anos",
        "15 a 19 anos",
        "20 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
        "70 a 74 anos",
        "75 a 79 anos",
        "80 a 84 anos",
        "85 a 89 anos",
        "90 a 94 anos",
        "95 a 99 anos",
        "100 anos ou mais"
      )
  ) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_uf, .before = code_muni) |>
  mutate(
    idade = case_match(
      idade,
      "0 a 4 anos" ~ "01. 0 a 4 anos",
      "5 a 9 anos" ~ "02. 5 a 9 anos",
      "10 a 14 anos" ~ "03. 10 a 14 anos",
      "15 a 19 anos" ~ "04. 15 a 19 anos",
      "20 a 24 anos" ~ "05. 20 a 24 anos",
      "25 a 29 anos" ~ "06. 25 a 29 anos",
      "30 a 34 anos" ~ "07. 30 a 34 anos",
      "35 a 39 anos" ~ "08. 35 a 39 anos",
      "40 a 44 anos" ~ "09. 40 a 44 anos",
      "45 a 49 anos" ~ "10. 45 a 49 anos",
      "50 a 54 anos" ~ "11. 50 a 54 anos",
      "55 a 59 anos" ~ "12. 55 a 59 anos",
      "60 a 64 anos" ~ "13. 60 a 64 anos",
      "65 a 69 anos" ~ "14. 65 a 69 anos",
      "70 a 74 anos" ~ "15. 70 a 74 anos",
      "75 a 79 anos" ~ "16. 75 a 79 anos",
      "80 a 84 anos" ~ "17. 80 a 84 anos",
      "85 a 89 anos" ~ "18. 85 a 89 anos",
      "90 a 94 anos" ~ "19. 90 a 94 anos",
      "95 a 99 anos" ~ "20. 95 a 99 anos",
      "100 anos ou mais" ~ "21. 100 anos ou mais"
    )
  )

indi_names_9514 <- crossing(
  sexo = unique(res_9514$sexo),
  idade = unique(res_9514$idade)
) |>
  arrange(sexo, idade) |>
  mutate(
    code_indi = paste0("ibge", str_pad(row_number() + 6, width = 3, pad = "0"))
  )

res_9514 <- res_9514 |>
  left_join(indi_names_9514, by = c("sexo", "idade")) |>
  relocate(code_indi) |>
  select(-sexo, -idade)

# Tabela 9883 - Número de favelas e comunidades urbanas, segundo os Municípios
# res_9883_raw <- get_sidra(
#   x = 9883,
#   period = "2022",
#   geo = "City",
#   header = TRUE
# )

# saveRDS(object = res_9883_raw, file = "res_9883_raw.rds", compress = "xz")

res_9883_raw <- readRDS("res_9883_raw.rds")

res_9883 <- res_9883_raw |>
  clean_names() |>
  as_tibble() |>
  select(code_muni = municipio_codigo, ano, valor) |>
  mutate(
    code_indi = "ibge070",
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_indi, code_uf, .before = code_muni) |>
  arrange(code_indi, code_uf, code_muni, ano)

# Tabela 9884 - População residente em favelas e comunidades urbanas, por cor ou raça, sexo e grupos de idade, segundo as Favelas e Comunidades Urbanas
# res_9884_raw <- map(
#   .x = mun2022,
#   .f = in_parallel(
#     \(x) {
#       retry::retry(
#         expr = sidrar::get_sidra(
#           x = 9884,
#           period = "2022",
#           geo = "City",
#           geo.filter = list("City" = x),
#           header = TRUE
#         ),
#         until = function(val, cnd) is.data.frame(val),
#         max_tries = 10
#       )
#     }
#   )
# ) |>
#   list_rbind()

# saveRDS(object = res_9884_raw, file = "res_9884_raw.rds", compress = "xz")

res_9884_raw <- readRDS("res_9884_raw.rds")

res_9884 <- res_9884_raw |>
  clean_names() |>
  as_tibble() |>
  select(
    sexo,
    idade = grupo_de_idade,
    cor_ou_raca,
    code_muni = municipio_codigo,
    ano,
    valor
  ) |>
  filter(
    idade %in%
      c(
        "0 a 4 anos",
        "5 a 9 anos",
        "10 a 14 anos",
        "15 a 19 anos",
        "20 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
        "70 a 74 anos",
        "75 a 79 anos",
        "80 a 84 anos",
        "85 a 89 anos",
        "90 a 94 anos",
        "95 a 99 anos",
        "100 anos ou mais",
        "Total"
      )
  ) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_uf, .before = code_muni) |>
  mutate(
    idade = case_match(
      idade,
      "0 a 4 anos" ~ "01. 0 a 4 anos",
      "5 a 9 anos" ~ "02. 5 a 9 anos",
      "10 a 14 anos" ~ "03. 10 a 14 anos",
      "15 a 19 anos" ~ "04. 15 a 19 anos",
      "20 a 24 anos" ~ "05. 20 a 24 anos",
      "25 a 29 anos" ~ "06. 25 a 29 anos",
      "30 a 34 anos" ~ "07. 30 a 34 anos",
      "35 a 39 anos" ~ "08. 35 a 39 anos",
      "40 a 44 anos" ~ "09. 40 a 44 anos",
      "45 a 49 anos" ~ "10. 45 a 49 anos",
      "50 a 54 anos" ~ "11. 50 a 54 anos",
      "55 a 59 anos" ~ "12. 55 a 59 anos",
      "60 a 64 anos" ~ "13. 60 a 64 anos",
      "65 a 69 anos" ~ "14. 65 a 69 anos",
      "70 a 74 anos" ~ "15. 70 a 74 anos",
      "75 a 79 anos" ~ "16. 75 a 79 anos",
      "80 a 84 anos" ~ "17. 80 a 84 anos",
      "85 a 89 anos" ~ "18. 85 a 89 anos",
      "90 a 94 anos" ~ "19. 90 a 94 anos",
      "95 a 99 anos" ~ "20. 95 a 99 anos",
      "100 anos ou mais" ~ "21. 100 anos ou mais",
      "Total" ~ "22. Total"
    )
  ) |>
  replace_na(list(valor = 0))

indi_names_9884 <- crossing(
  sexo = unique(res_9884$sexo),
  cor_ou_raca = unique(res_9884$cor_ou_raca),
  idade = unique(res_9884$idade)
) |>
  arrange(sexo, cor_ou_raca, idade) |>
  mutate(
    code_indi = paste0("ibge", str_pad(row_number() + 70, width = 3, pad = "0"))
  )

res_9884 <- res_9884 |>
  left_join(indi_names_9884, by = c("sexo", "idade", "cor_ou_raca")) |>
  relocate(code_indi) |>
  select(-sexo, -idade, -cor_ou_raca)

# Tabela 10062 - Número médio de anos de estudo das pessoas com 11 anos ou mais de idade, segundo os grupos de idade, o sexo e a cor ou raça
# res_10062_raw <- map(
#   .x = mun2022,
#   .f = in_parallel(
#     \(x) {
#       retry::retry(
#         expr = sidrar::get_sidra(
#           x = 10062,
#           period = "2022",
#           geo = "City",
#           geo.filter = list("City" = x),
#           header = TRUE
#         ),
#         until = function(val, cnd) is.data.frame(val),
#         max_tries = 10
#       )
#     }
#   )
# ) |>
#   list_rbind()

# saveRDS(object = res_10062_raw, file = "res_10062_raw.rds", compress = "xz")

res_10062_raw <- readRDS("res_10062_raw.rds")

res_10062 <- res_10062_raw |>
  clean_names() |>
  as_tibble() |>
  select(
    sexo,
    idade = grupo_de_idade,
    code_muni = municipio_codigo,
    ano,
    valor
  ) |>
  filter(
    idade %in%
      c(
        "11 a 14 anos",
        "15 a 17 anos",
        "18 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
        "70 a 74 anos",
        "75 a 79 anos",
        "80 anos ou mais",
        "Total"
      )
  ) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_uf, .before = code_muni) |>
  mutate(
    idade = case_match(
      idade,
      "11 a 14 anos" ~ "01. 11 a 14 anos",
      "15 a 17 anos" ~ "02. 15 a 17 anos",
      "18 a 24 anos" ~ "03. 18 a 24 anos",
      "25 a 29 anos" ~ "04. 25 a 29 anos",
      "30 a 34 anos" ~ "05. 30 a 34 anos",
      "35 a 39 anos" ~ "06. 35 a 39 anos",
      "40 a 44 anos" ~ "07. 40 a 44 anos",
      "45 a 49 anos" ~ "08. 45 a 49 anos",
      "50 a 54 anos" ~ "09. 50 a 54 anos",
      "55 a 59 anos" ~ "10. 55 a 59 anos",
      "60 a 64 anos" ~ "11. 60 a 64 anos",
      "65 a 69 anos" ~ "12. 65 a 69 anos",
      "70 a 74 anos" ~ "13. 70 a 74 anos",
      "75 a 79 anos" ~ "14. 75 a 79 anos",
      "80 anos ou mais" ~ "15. 80 anos ou mais",
      "Total" ~ "16. Total"
    )
  ) |>
  replace_na(list(valor = 0))

indi_names_10062 <- crossing(
  sexo = unique(res_10062$sexo),
  idade = unique(res_10062$idade)
) |>
  arrange(sexo, idade) |>
  mutate(
    code_indi = paste0(
      "ibge",
      str_pad(row_number() + 532, width = 3, pad = "0")
    )
  )

res_10062 <- res_10062 |>
  left_join(indi_names_10062, by = c("sexo", "idade")) |>
  relocate(code_indi) |>
  select(-sexo, -idade)

# Tabela 10125 - Pessoas residentes de 2 anos ou mais de idade, total e pessoas com deficiência, por sexo e grupos de idade
# res_10125_raw <- map(
#   .x = mun2022,
#   .f = in_parallel(
#     \(x) {
#       retry::retry(
#         expr = sidrar::get_sidra(
#           x = 10125,
#           period = "2022",
#           geo = "City",
#           geo.filter = list("City" = x),
#           header = TRUE
#         ),
#         until = function(val, cnd) is.data.frame(val),
#         max_tries = 10
#       )
#     }
#   )
# ) |>
#   list_rbind()

# saveRDS(object = res_10125_raw, file = "res_10125_raw.rds", compress = "xz")

res_10125_raw <- readRDS("res_10125_raw.rds")

res_10125 <- res_10125_raw |>
  clean_names() |>
  as_tibble() |>
  filter(
    unidade_de_medida == "Pessoas",
    variavel_codigo == 12785
  ) |>
  select(
    sexo,
    idade = grupo_de_idade,
    code_muni = municipio_codigo,
    ano,
    valor
  ) |>
  filter(
    idade %in%
      c(
        "2 a 4 anos",
        "5 a 9 anos",
        "10 a 14 anos",
        "15 a 19 anos",
        "20 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
        "70 a 74 anos",
        "75 a 79 anos",
        "80 a 84 anos",
        "85 a 89 anos",
        "90 a 94 anos",
        "95 a 99 anos",
        "100 anos ou mais",
        "Total"
      )
  ) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_uf, .before = code_muni) |>
  mutate(
    idade = case_match(
      idade,
      "2 a 4 anos" ~ "01. 2 a 4 anos",
      "5 a 9 anos" ~ "02. 5 a 9 anos",
      "10 a 14 anos" ~ "03. 10 a 14 anos",
      "15 a 19 anos" ~ "04. 15 a 19 anos",
      "20 a 24 anos" ~ "05. 20 a 24 anos",
      "25 a 29 anos" ~ "06. 25 a 29 anos",
      "30 a 34 anos" ~ "07. 30 a 34 anos",
      "35 a 39 anos" ~ "08. 35 a 39 anos",
      "40 a 44 anos" ~ "09. 40 a 44 anos",
      "45 a 49 anos" ~ "10. 45 a 49 anos",
      "50 a 54 anos" ~ "11. 50 a 54 anos",
      "55 a 59 anos" ~ "12. 55 a 59 anos",
      "60 a 64 anos" ~ "13. 60 a 64 anos",
      "65 a 69 anos" ~ "14. 65 a 69 anos",
      "70 a 74 anos" ~ "15. 70 a 74 anos",
      "75 a 79 anos" ~ "16. 75 a 79 anos",
      "80 a 84 anos" ~ "17. 80 a 84 anos",
      "85 a 89 anos" ~ "18. 85 a 89 anos",
      "90 a 94 anos" ~ "19. 90 a 94 anos",
      "95 a 99 anos" ~ "20. 95 a 99 anos",
      "100 anos ou mais" ~ "21. 100 anos ou mais",
      "Total" ~ "22. Total"
    )
  ) |>
  replace_na(list(valor = 0))

indi_names_10125 <- crossing(
  sexo = unique(res_10125$sexo),
  idade = unique(res_10125$idade)
) |>
  arrange(sexo, idade) |>
  mutate(
    code_indi = paste0(
      "ibge",
      str_pad(row_number() + 580, width = 3, pad = "0")
    )
  )

res_10125 <- res_10125 |>
  left_join(indi_names_10125, by = c("sexo", "idade")) |>
  relocate(code_indi) |>
  select(-sexo, -idade)

# Tabela 10127 - Pessoas residentes de 2 anos ou mais de idade com deficiência por tipos de dificuldades funcionais
# res_10127_raw <- map(
#   .x = mun2022,
#   .f = in_parallel(
#     \(x) {
#       retry::retry(
#         expr = sidrar::get_sidra(
#           x = 10127,
#           period = "2022",
#           geo = "City",
#           geo.filter = list("City" = x),
#           header = TRUE
#         ),
#         until = function(val, cnd) is.data.frame(val),
#         max_tries = 10
#       )
#     }
#   )
# ) |>
#   list_rbind()

# saveRDS(object = res_10127_raw, file = "res_10127_raw.rds", compress = "xz")

res_10127_raw <- readRDS("res_10127_raw.rds")

res_10127 <- res_10127_raw |>
  clean_names() |>
  as_tibble() |>
  filter(
    unidade_de_medida == "Pessoas",
    variavel_codigo == 12785
  ) |>
  select(
    code_muni = municipio_codigo,
    tipos_de_dificuldades_funcionais,
    ano,
    valor
  ) |>
  mutate(
    code_muni = as.numeric(substr(code_muni, 0, 6)),
    code_uf = as.numeric(substr(code_muni, 0, 2)),
    ano = as.numeric(ano)
  ) |>
  relocate(code_uf, .before = code_muni) |>
  replace_na(list(valor = 0))

indi_names_10127 <- crossing(
  tipo = unique(res_10127$tipos_de_dificuldades_funcionais)
) |>
  arrange(tipo) |>
  mutate(
    code_indi = paste0(
      "ibge",
      str_pad(row_number() + 646, width = 3, pad = "0")
    )
  )

res_10127 <- res_10127 |>
  left_join(
    indi_names_10127,
    by = c("tipos_de_dificuldades_funcionais" = "tipo")
  ) |>
  relocate(code_indi) |>
  select(-tipos_de_dificuldades_funcionais)

# Bind all row results
res <- bind_rows(
  res_9605,
  res_9514,
  res_9883,
  res_10062,
  res_10125,
  res_10127
)

# Expand zeroes among all indicators and municipalities
res_comp <- res |>
  complete(code_indi, code_muni, ano) |>
  mutate(code_uf = as.numeric(substr(code_muni, 0, 2))) |>
  replace_na(list(valor = 0)) |>
  mutate(valor = round(valor, digits = 2)) |>
  select(code_indi, code_uf, code_muni, ano, valor)

# Write to database
dbWriteTable(conn = con, name = "ibge", value = res_comp, overwrite = TRUE)

# Database disconnect
dbDisconnect(conn = con)
