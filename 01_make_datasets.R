source("init.R")
library(lubridate)
library(purrr)

juegos = dbGetQuery(con,"select mc_indice.*, devel, publisher, release_date
                          from mc_indice
                          left join mc_juegos
                          on mc_indice.id = mc_juegos.id;")
juegos = mutate(juegos,
                metascore = as.numeric(metascore)/10,
                userscore = as.numeric(userscore),
                release_date = mdy(release_date),
                release_year = year(release_date),
                release_month = release_date - day(release_date) + 1)
saveRDS(juegos,"dataset/juegos.RDS")

detalles_db = dbGetQuery(con,"select * from mc_detail")
detalles = detalles_db %>% 
  left_join(juegos) %>% 
  mutate(detail_metascore_value = as.numeric(detail_metascore_value)/10)
saveRDS(detalles,"dataset/detalles.RDS")