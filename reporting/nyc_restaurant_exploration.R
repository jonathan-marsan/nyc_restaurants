###
# Exploratory analysis of NYC restaurant inspection data
###

pacman::p_load(dplyr)
pacman::p_load(ggplot2)
pacman::p_load(lubridate)
pacman::p_load(aws.s3)
pacman::p_load(rmarkdown)
pacman::p_load(scales)

source('reporting/utilities.R')


## Constants
file_output_folder <- "output/"
exploration_markdown_file <- "reporting/nyc_restaurant_exploration.Rmd"
bucket_name <- 'nyc-restaurants-20180203'
nyc_inspection_data_s3_path <- "output/nyc_restaurant_inspection_data.csv"

## Read in NYC Inspection dataset from S3
nyc_restaurants <- convert_s3_csv_to_df(bucket_name, nyc_inspection_data_s3_path)

## Read Yelp data from S3
all_files <- aws.s3::get_bucket(bucket_name, prefix="output/yelp_business_data/")
specific_keys <- get_my_s3_keys(s3_bucket_objs=all_files, exclusion_pattern='/manifest/')
yelp_data <- convert_many_s3_csv_to_df(bucket_name, specific_keys)

## Clean data
nyc_rest_cleaned <- nyc_restaurants %>%
  dplyr:: mutate(inspection_date=lubridate::ymd_hms(inspection_date),
                 inspection_month=as.Date(format(inspection_date, format="%Y-%m-01")),
                 grade_date = lubridate::ymd_hms(grade_date),
                 record_date = lubridate::ymd_hms(record_date),
                 critical = ifelse(critical_flag == "Not Applicable",
                                   NA,
                                   critical_flag),
                 cuisine_type =  clean_cuisine_name(as.character(cuisine_description))) %>%
  dplyr::select(restaurant_id = camis,
                restaurant_name = dba,
                borough = boro,
                building_nb = building,
                street,
                zipcode,
                phone,
                cuisine_type,
                action,
                viol_code = violation_code,
                viol_desc = violation_description,
                score,
                grade,
                inspection_type,
                critical,
                inspection_date,
                inspection_month,
                grade_date,
                record_date)

## Explore dataset


# Summarize inspections ---------------------------------------------------

first_inspection <- nyc_rest_cleaned %>%
  dplyr::filter(inspection_date > as.Date('1900-01-01')) %>% #removing inspect dates that appear to be omitted
  .$inspection_date %>%
  min()
last_inspection <- max(nyc_rest_cleaned$inspection_date)
nb_restaurants <- format(length(unique(nyc_rest_cleaned$restaurant_id)), big.mark = ",")
nb_cuisine_types <- length(unique(nyc_rest_cleaned$cuisine_type))

inspections_by_month_plot <-  ggplot(data = nyc_rest_cleaned %>% filter(inspection_date > as.Date('2010-01-01')),
                                     aes(x = inspection_month )) +
  geom_bar(fill="#FF4A00") +
  scale_x_date(date_breaks = "3 month") +
  theme(axis.text.x = element_text(angle = 40, hjust = 1, vjust = 1)) +
  labs(x = 'Inspection Month', y = 'Number of Inspections')

inspections_by_month_ts <- nyc_rest_cleaned %>%
  dplyr::filter(inspection_date > as.Date('2015-01-01'),
                inspection_date < as.Date('2018-02-01')) %>%
  dplyr::group_by(inspection_month) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::arrange(inspection_month) %>%
  .$count %>%
  ts(frequency = 12)

fit_inspections <- stl(inspections_by_month_ts, s.window="periodic")


# Summarize grades --------------------------------------------------------

grade_count <- nyc_rest_cleaned %>%
  dplyr::filter(grade %in% c('A', 'B', 'C')) %>%
  dplyr::group_by(grade) %>%
  dplyr::summarise(count = n()) %>%
  dplyr::mutate(percent = scales::percent(count/sum(.$count)),
                count = format(count, big.mark = ",")) %>%
  dplyr::arrange(grade)

overall_prob_gradeA <- nyc_rest_cleaned %>%
  dplyr::filter(grade %in% c('A', 'B', 'C')) %>%
  dplyr::mutate(is_grade_A = grade == 'A') %>%
  dplyr::group_by(restaurant_id) %>%
  dplyr::summarise(only_grade_A = mean(is_grade_A)==1) %>%
  dplyr::ungroup() %>%
  dplyr::summarise(nb_gradeABC_restaurants = n(),
                   probability_only_received_grade_A = scales::percent(mean(only_grade_A)))

rest_count_by_cuisine <- nyc_rest_cleaned %>%
  dplyr::select(restaurant_id, cuisine_type) %>%
  unique() %>%
  dplyr::group_by(cuisine_type) %>%
  dplyr::summarise(nb_restaurants = n()) %>%
  dplyr::mutate(overall_proportion = scales::percent(nb_restaurants/sum(.$nb_restaurants))) %>%
  dplyr::arrange(desc(nb_restaurants))

grade_A_inspections <- nyc_rest_cleaned %>%
  dplyr::filter(grade %in% c('A', 'B', 'C')) %>% # remove empty or pending grades
  dplyr::mutate(is_grade_A = grade == 'A') %>%
  dplyr::group_by(restaurant_id, cuisine_type) %>%
  dplyr::summarise(only_grade_A = mean(is_grade_A)==1) %>%
  dplyr::group_by(cuisine_type) %>%
  dplyr::summarise(nb_gradeABC_restaurants = n(),
                   probability_only_received_grade_A = scales::percent(mean(only_grade_A)))

rest_by_cuisine_and_prob_grade_A_only <- rest_count_by_cuisine %>%
  dplyr::left_join(grade_A_inspections)



# Is borough correlated with violation score? -----------------------------

borough_score <- nyc_rest_cleaned %>%
  dplyr::select(restaurant_id, borough, score) %>%
  dplyr::mutate(borough = as.factor(borough)) %>%
  dplyr::filter(borough != 'Missing' & !is.na(score)) %>%
  dplyr::group_by(restaurant_id, borough) %>%
  dplyr::summarise(mean_score = mean(score, na.rm=TRUE))

borough_score_plot <- ggplot(data=borough_score,
                             aes(x=borough, y= mean_score)) +
  geom_boxplot()

borough_score_aov <- summary(aov(mean_score ~ borough, data = borough_score))
borough_score_tukey <- TukeyHSD(aov(mean_score ~ borough, data = borough_score))


# Are violation scores inversely restaurant price? ---------------------

yelp_price_data <- yelp_data %>%
  dplyr::select(phone, price) %>%
  dplyr::mutate(phone = as.character(phone),
                price = as.factor(price))

avg_viol_score_with_yelp_price <- nyc_rest_cleaned %>%
  dplyr::mutate(phone = paste0("1", as.character(phone))) %>%
  dplyr::group_by(phone) %>%
  dplyr::summarise(mean_violation_score = mean(score, na.rm = TRUE)) %>%
  dplyr::inner_join(yelp_price_data) %>%
  dplyr::filter(!is.na(mean_violation_score) & !is.na(price))

avg_viol_score_with_yelp_price_plot <- ggplot(data=avg_viol_score_with_yelp_price,
                                              aes(x=price, y=mean_violation_score)) +
  geom_boxplot()

price_score_aov <- summary(aov(mean_violation_score ~ price,
                                 data = avg_viol_score_with_yelp_price))
price_score_tukey <- TukeyHSD(aov(mean_violation_score ~ price,
                                    data = avg_viol_score_with_yelp_price))

rmarkdown::render(input = exploration_markdown_file,
                  output_dir = file_output_folder,
                  params = list(
                    first_inspection = first_inspection,
                    last_inspection = last_inspection,
                    nb_restaurants = nb_restaurants,
                    nb_cuisine_types = nb_cuisine_types,
                    rest_by_cuisine_and_prob_grade_A_only = rest_by_cuisine_and_prob_grade_A_only,
                    overall_prob_gradeA = overall_prob_gradeA,
                    grade_count = grade_count,
                    inspections_by_month_plot = inspections_by_month_plot,
                    fit_inspections = fit_inspections,
                    borough_score_plot = borough_score_plot,
                    borough_score_aov = borough_score_aov,
                    avg_viol_score_with_yelp_price_plot = avg_viol_score_with_yelp_price_plot,
                    price_score_aov = price_score_aov))
