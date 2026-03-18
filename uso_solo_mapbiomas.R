# Packages
library(dplyr)
library(lubridate)
library(readxl)
library(tidyr)
library(geobr)
library(sf)

mun <- read_municipality() |>
  st_drop_geometry() |>
  select(code_muni, name_muni, abbrev_state) |>
  mutate(name_muni = toupper(name_muni)) |>
  tibble::as_tibble()

# Dados originais
raw <- read_xlsx(
  path = "dados_brutos/mapbiomas/MAPBIOMAS_BRAZIL-COVERAGE_STATISTICS-COL.10.1-MUNICIPALITIES_STATES_BIOMES.xlsx",
  sheet = "COVERAGE_10.1"
)

# Level 2
mapeamento_mapbiomas <- c(
  "6. Not Observed" = "Não Observado",
  "1.1. Forest Formation" = "Formação Florestal",
  "1.4 Floodable Forest" = "Floresta Alagável",
  "2.1. Wetland" = "Áreas Úmidas",
  "2.2. Grassland" = "Formação Campestre",
  "3.1. Pasture" = "Pastagem",
  "4.2. Urban Area" = "Área Urbana",
  "4.5. Other non Vegetated Areas" = "Outras Áreas não Vegetadas",
  "5.2. Aquaculture" = "Aquicultura",
  "5.1. River, Lake and Ocean" = "Rio, Lago e Oceano",
  "3.2. Agriculture" = "Agricultura",
  "1.2. Savanna Formation" = "Formação Savânica",
  "4.3. Mining" = "Mineração",
  "2.4. Rocky Outcrop" = "Afloramento Rochoso",
  "1.3. Mangrove" = "Manguezal",
  "3.3. Forest Plantation" = "Silvicultura",
  "4.1. Beach, Dune and Sand Spot" = "Praia, Duna e Areal",
  "2.3. Hypersaline Tidal Flat" = "Apicum",
  "3.4. Mosaic of Uses" = "Mosaico de Usos",
  "2.4. Herbaceous Sandbank Vegetation" = "Restinga Herbácea",
  "4.4. Photovoltaic Project" = "Projeto Fotovoltaico",
  "1.5. Wooded Sandbank Vegetation" = "Restinga Arbórea",
  "2.6. Other non Forest Formations" = "Outras Formações não Florestais"
)

raw |>
  pivot_longer(cols = `1985`:`2024`, names_to = "ano") |>
  group_by(state_acronym, municipality, ano, class_level_2) |>
  summarise(value = sum(value, na.rm = TRUE)) |>
  mutate(value = round(100 * value / sum(value, na.rm = TRUE), 2)) |>
  ungroup() |>
  mutate(class_level_2 = recode(class_level_2, !!!mapeamento_mapbiomas)) |>
  rename(classe = class_level_2, valor = value) |>
  mutate(municipality = toupper(municipality)) |>
  inner_join(
    mun,
    by = c("municipality" = "name_muni", "state_acronym" = "abbrev_state")
  ) |>
  mutate(code_muni = as.integer(code_muni)) |>
  select(cod_ibge = code_muni, ano, classe, percentual = valor) |>
  View()
