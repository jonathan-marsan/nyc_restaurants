pacman::p_load(dplyr)
pacman::p_load(ggplot2)
pacman::p_load(lubridate)
pacman::p_load(aws.s3)
pacman::p_load(rmarkdown)
pacman::p_load(scales)
pacman::p_load(forecast)

source('utilities.R')

## Constants
file_output_folder <- "output/"
exploration_markdown_file <- "nyc_restaurant_exploration.Rmd"

## Read in file from S3
raw_object <- aws.s3::get_object("s3://nyc-restaurants-20180203/output/nyc_restaurant_inspection_data.csv")
char_string <- base::rawToChar(raw_object)
con <- base::textConnection(char_string)
nyc_restaurants <- utils::read.csv(con)
close(con)


## Clean data
nyc_rest_cleaned <- nyc_restaurants %>%
  dplyr:: mutate(inspection_date=lubridate::mdy(INSPECTION.DATE),
                 inspection_month=as.Date(format(inspection_date, format="%Y-%m-01")),
                 grade_date = lubridate::mdy(GRADE.DATE),
                 record_date = lubridate::mdy(RECORD.DATE),
                 critical = ifelse(CRITICAL.FLAG == "Not Applicable",
                                   NA,
                                   CRITICAL.FLAG),
                 cuisine_type =  clean_cuisine_name(as.character(CUISINE.DESCRIPTION))) %>%
  dplyr::select(restaurant_id = CAMIS,
                restaurant_name = DBA,
                borough = BORO,
                building_nb = BUILDING,
                street = STREET,
                zipcode = ZIPCODE,
                phone = PHONE,
                cuisine_type,
                action = ACTION,
                viol_code = VIOLATION.CODE,
                viol_desc = VIOLATION.DESCRIPTION,
                score = SCORE,
                grade = GRADE,
                inspection_type = INSPECTION.TYPE,
                critical,
                inspection_date,
                inspection_month,
                grade_date,
                record_date)

## Explore dataset
first_inspection <- nyc_rest_cleaned %>%
  filter(inspection_date > as.Date('1900-01-01')) %>% #removing inspect dates that appear to be omitted
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
  filter(inspection_date > as.Date('2015-01-01'),
         inspection_date < as.Date('2018-02-01')) %>%
  group_by(inspection_month) %>%
  summarise(count = n()) %>%
  arrange(inspection_month) %>%
  .$count %>%
  ts(frequency = 12)

fit_inspections <- stl(inspections_by_month_ts, s.window="periodic")

grade_count <- nyc_rest_cleaned %>%
  filter(grade %in% c('A', 'B', 'C')) %>%
  group_by(grade) %>%
  summarise(count = n()) %>%
  mutate(percent = scales::percent(count/sum(.$count)),
         count = format(count, big.mark = ",")) %>%
  arrange(grade)

rest_count_by_cuisine <- nyc_rest_cleaned %>%
  select(restaurant_id, cuisine_type) %>%
  unique() %>%
  group_by(cuisine_type) %>%
  summarise(nb_restaurants = n()) %>%
  mutate(overall_proportion = scales::percent(nb_restaurants/sum(.$nb_restaurants))) %>%
  arrange(desc(nb_restaurants))

grade_A_inspections <- nyc_rest_cleaned %>%
  filter(grade %in% c('A', 'B', 'C')) %>% # remove empty or pending grades
  mutate(is_grade_A = grade == 'A') %>%
  group_by(restaurant_id, cuisine_type) %>%
  summarise(only_grade_A = mean(is_grade_A)==1) %>%
  group_by(cuisine_type) %>%
  summarise(nb_inspected_restaurants = n(),
            probability_only_received_grade_A = scales::percent(mean(only_grade_A)))

rest_by_cuisine_and_prob_grade_A_only <- rest_count_by_cuisine %>%
  left_join(grade_A_inspections)

#note join with average $$$ value ?

rmarkdown::render(input = exploration_markdown_file,
                  output_dir = file_output_folder,
                  params = list(
                    first_inspection = first_inspection,
                    last_inspection = last_inspection,
                    nb_restaurants = nb_restaurants,
                    nb_cuisine_types = nb_cuisine_types,
                    rest_by_cuisine_and_prob_grade_A_only = rest_by_cuisine_and_prob_grade_A_only,
                    grade_count = grade_count,
                    inspections_by_month_plot = inspections_by_month_plot,
                    fit_inspections = fit_inspections))
