###
# Yelp dataset functions
###

require(aws.s3)
require(dplyr)


# Yelp functions ----------------------------------------------------------

# Shorten and group a few cuisine names for readability
clean_cuisine_name <- function(x){
  ifelse(grepl('Latin', x), 'Latin',
         ifelse(grepl('Bottled', x), 'BottledBev',
                ifelse(grepl('Coffee', x), 'Coffee',
                       ifelse(grepl('Ice Cream', x), 'Ice Cream',
                              ifelse(grepl('Juice', x), 'Juice',
                                     ifelse(grepl('Not Applicable', x), NA,
                                            ifelse(grepl('Sandwiches', x), 'Sandwiches',
                                                   ifelse(grepl('Vietnamese', x), 'SE_Asian', x))))))))
}


# AWS functions -----------------------------------------------------------

get_my_s3_keys <- function(s3_bucket_objs, exclusion_pattern){
  list_objs_to_keep = list()
  j <- 1
  for(i in 1:length(s3_bucket_objs)){
    if(!grepl(exclusion_pattern, s3_bucket_objs[i]$Contents$Key)){
      list_objs_to_keep[j] <- s3_bucket_objs[i]$Contents$Key
      j <- j+1
    }
  }
  list_objs_to_keep
}

convert_s3_csv_to_df <- function(bucket_name, csv_link){
  s3_csv_link <- paste0('s3://', bucket_name, '/', csv_link)
  raw_object <- aws.s3::get_object(s3_csv_link)
  char_string <- base::rawToChar(raw_object)
  con <- base::textConnection(char_string)
  df <- utils::read.csv(con, stringsAsFactors=FALSE)
  close(con)
  df
}

convert_many_s3_csv_to_df <- function(bucket_name, s3_csv_links){
  list_of_dfs = list()
  for(i in 1:length(s3_csv_links)){
    tryCatch({
    list_of_dfs[[i]] <- convert_s3_csv_to_df(bucket_name, s3_csv_links[[i]])
    },
    error=function(e){cat("ERROR :", s3_csv_links[[i]], conditionMessage(e), "\n")})
  }
  dplyr::bind_rows(list_of_dfs)
}
