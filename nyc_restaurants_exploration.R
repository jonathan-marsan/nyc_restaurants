library(dplyr, lib.loc = '../r_libs/')
library(ggplot2, lib.loc = '../r_libs/')
library(labeling, lib.loc = '../r_libs/')

### Read in files as tables

# Prep table names names
dataset_files <- list.files(path = 'data/', pattern = '*.tsv')
table_names <- gsub('\\.tsv', '', dataset_files)

# Read in tables
for(i in 1:length(dataset_files)){
  assign(table_names[i],
         read.table(paste0('data/', dataset_files[i]),
                    sep = '\t',
                    header = TRUE,
                    fill = TRUE,
                    quote = "",
                    stringsAsFactors = FALSE,
                    row.names = NULL))
}

### Check data
for(i in 1:length(table_names)){
  print(table_names[i])
  dataset_tbl <- eval(parse(text = table_names[i]))
  print(dataset_tbl %>% names())
  print(dataset_tbl %>% str())
}

### Create a few summary metric
nb_types_of_cuisine <- nrow(cuisine_names)
nb_restaurants <- nrow(restaurant_names)
nb_total_violations <- nrow(restaurant_violations)

# Add naming to keys
restaurant_attributes_enriched <- restaurant_attributes %>%
  dplyr::left_join(restaurant_names, by = c('restaurant_id' = 'id')) %>%
  dplyr::left_join(borough_names, by = c('borough_id' = 'id')) %>%
  dplyr::left_join(cuisine_names, by = c('cuisine_id' = 'id'))

# Summary table of violations by restaurant id
# Clean violation_names data frame
violation_names_cleaned <- violation_names %>%
    mutate(id = as.integer(row.names),
           violation_description = id..violation_description) %>%
    select(id, violation_description)

violations_by_restaurant <- restaurant_violations %>%
  dplyr::left_join(violation_names_cleaned, by = c('violation_id' = 'id')) %>%
  dplyr::mutate(critical_flag = critical_flag == 'Critical') %>%
  dplyr::group_by(restaurant_id) %>%
  dplyr::summarise(critical_flag = max(critical_flag),
                   mean_violation_score = mean(score))

restaurant_all_attributes <- restaurant_attributes_enriched %>%
  dplyr::left_join(violations_by_restaurant)

# Shorten certain cuistine types
restaurant_all_attributes$cuisine_description[grepl('Latin', restaurant_all_attributes$cuisine_description)] <- 'Latin'
restaurant_all_attributes$cuisine_description[grepl('Coffee', restaurant_all_attributes$cuisine_description)] <- 'Coffee Shop'

#####
# Story

###
# Part 1: General Metrics + Relationship between violation score and geograph / cuisine type
# Number of restaurants (total)
nb_restaurants

# Number of restaurants by borough
barplot_restaurants_by_borough <- ggplot(restaurant_all_attributes,
                                      aes(x = borough_name)) +
  geom_bar()
barplot_restaurants_by_borough

# Relationship between borough and average violation scores
boxplot_score_by_borough <- ggplot(restaurant_all_attributes,
                                   aes(x = borough_name,
                                       y = mean_violation_score)) +
  geom_boxplot()
boxplot_score_by_borough

borough_lm <- lm(mean_violation_score ~ borough_name,
                 data = restaurant_all_attributes)
summary(borough_lm)

# Top ten cuisine types by count
restaurant_cuisine_type <- restaurant_all_attributes %>%
  dplyr::group_by(cuisine_description) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::arrange(desc(count))

top_ten_cuisines_by_count <- restaurant_cuisine_type %>% head(10)
print(top_ten_cuisines_by_count)

top_ten_cuisines_by_count_scores <- restaurant_all_attributes %>%
  filter(cuisine_description %in% top_ten_cuisines_by_count$cuisine_description)

# Relationship between cuisine type and average score
boxplot_top_ten_cuisines_by_count_scores <- ggplot(top_ten_cuisines_by_count_scores,
                                                   aes(x = cuisine_description,
                                                       y = mean_violation_score)) +
  geom_boxplot() +
  theme(axis.text.x=element_text(angle=60,hjust=1))
boxplot_top_ten_cuisines_by_count_scores

cuisine_type_linear_model <- lm(mean_violation_score ~ cuisine_description,
                   data = top_ten_cuisines_by_count_scores)
summary(cuisine_type_linear_model)

### Part 2
# Changes over time

