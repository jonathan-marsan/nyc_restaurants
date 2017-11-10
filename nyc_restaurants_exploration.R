library(dplyr, lib.loc = '../r_libs/')
library(ggplot2, lib.loc = '../r_libs/')
library(labeling, lib.loc = '../r_libs/')
library(lubridate, lib.loc = '../r_libs/')

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
nb_types_of_cuisine <- restaurant_attributes %>% .$cuisine_id %>% unique %>% length
nb_restaurants <- restaurant_attributes %>% .$restaurant_id %>% unique %>% length
nb_total_violations <- nrow(restaurant_violations)
nb_total_violations_by_grade <- restaurant_violations %>%
  dplyr::group_by(grade) %>%
  dplyr::summarise(count = n())
barplot_violations_by_grade <- ggplot(restaurant_violations,
                                      aes(x = grade)) +
  geom_bar() +
  theme(text = element_text(size=24))
barplot_violations_by_grade

# Enrich restaurant info
restaurant_attributes_enriched <- restaurant_attributes %>%
  dplyr::left_join(restaurant_names, by = c('restaurant_id' = 'id')) %>%
  dplyr::left_join(borough_names, by = c('borough_id' = 'id')) %>%
  dplyr::left_join(cuisine_names, by = c('cuisine_id' = 'id'))

# Clean violation_names data frame
violation_names_cleaned <- violation_names %>%
    mutate(id = as.integer(row.names),
           violation_description = id..violation_description) %>%
    select(id, violation_description)

# Summarize violations by restaurant id
violations_by_restaurant <- restaurant_violations %>%
  dplyr::left_join(violation_names_cleaned, by = c('violation_id' = 'id')) %>%
  dplyr::mutate(critical_flag = critical_flag == 'Critical',
                is_grade_A = grade == 'A') %>%
  dplyr::group_by(restaurant_id) %>%
  dplyr::summarise(critical_flag = max(critical_flag),
                   mean_violation_score = mean(score),
                   median_violation_score = median(score),
                   received_grade_A = max(is_grade_A) == 1,
                   received_only_grade_A = sum(!is_grade_A) == 0)

# Add violation data to restaurant attributes
restaurant_all_attributes <- restaurant_attributes_enriched %>%
  dplyr::left_join(violations_by_restaurant)

probability_only_grade_A <- sum(restaurant_all_attributes$received_only_grade_A)/nrow(restaurant_all_attributes)
probability_only_grade_A_by_cuisine_type <- restaurant_all_attributes %>%
  dplyr::group_by(cuisine_description) %>%
  dplyr::summarise(probability = sum(received_only_grade_A)/n(),
                   count = n()) %>%
  dplyr::arrange(desc(probability)) %>%
  dplyr::filter(count > 50)

probability_only_grade_A_by_cuisine_type %>% head()
probability_only_grade_A_by_cuisine_type %>% tail()

# Shorten name of certain cuistine types
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
  geom_bar() +
  theme(text = element_text(size=24))

barplot_restaurants_by_borough

# Relationship between borough and average violation scores
boxplot_score_by_borough <- ggplot(restaurant_all_attributes,
                                   aes(x = borough_name,
                                       y = median_violation_score)) +
  geom_boxplot() +
  theme(text = element_text(size=24))
boxplot_score_by_borough

borough_lm <- lm(median_violation_score ~ borough_name,
                 data = restaurant_all_attributes)
summary(borough_lm)

# Top ten cuisine types by count
restaurant_cuisine_type <- restaurant_all_attributes %>%
  dplyr::group_by(cuisine_description) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::arrange(desc(count))

top_ten_cuisines_by_count <- restaurant_cuisine_type %>% head(10) %>%
print(top_ten_cuisines_by_count)

barplot_topten_cuisines_by_count <- ggplot(top_ten_cuisines_by_count,
                                           aes(x = cuisine_description,
                                               y = count)) +
  geom_bar(stat = "identity") +
  theme(text = element_text(size=24),
        axis.text.x=element_text(angle=60,hjust=1))

barplot_topten_cuisines_by_count

top_ten_cuisines_by_count_scores <- restaurant_all_attributes %>%
  filter(cuisine_description %in% top_ten_cuisines_by_count$cuisine_description)

# Relationship between cuisine type and average score
boxplot_top_ten_cuisines_by_count_scores <- ggplot(top_ten_cuisines_by_count_scores,
                                                   aes(x = cuisine_description,
                                                       y = median_violation_score)) +
  geom_boxplot() +
  theme(axis.text.x=element_text(angle=60,hjust=1),
        text = element_text(size=24))
boxplot_top_ten_cuisines_by_count_scores

cuisine_type_linear_model <- lm(median_violation_score ~ cuisine_description,
                   data = top_ten_cuisines_by_count_scores)
summary(cuisine_type_linear_model)

### Part 2
# Changes over time
restaurant_violations_enriched <- restaurant_violations %>%
  dplyr::mutate(grade_date = mdy(grade_date)) %>%
  dplyr::mutate(year_month = format(grade_date, '%Y-%m'),
                year_quarter = paste0(format(grade_date, '%Y'), '_Q', lubridate::quarter(grade_date)),
                grade_month = lubridate::month(grade_date, label = TRUE)) %>%
  dplyr::arrange(grade_date)

boxplot_violations_by_month <- ggplot(restaurant_violations_enriched,
                                        aes(x = grade_month, y = score, group = grade_month)) +
  geom_boxplot() +
  theme(axis.text.x=element_text(angle=60,hjust=1),
        text = element_text(size=24))
  
boxplot_violations_by_month

violations_by_month_lm <- lm(score ~ grade_month, data = restaurant_violations_enriched)
summary(violations_by_month_lm)

boxplot_violations_by_quarter <- ggplot(restaurant_violations_enriched,
                                        aes(x = year_quarter, y = score)) +
  geom_boxplot()
boxplot_violations_by_quarter +
  theme(axis.text.x=element_text(angle=60,hjust=1),
        text = element_text(size=24))