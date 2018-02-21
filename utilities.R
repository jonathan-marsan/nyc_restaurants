###
# Yelp dataset functions
###

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
