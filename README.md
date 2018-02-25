## Exploratory Analysis of DOHMH New York City Restaurant Results

### Datasets
* DOHMH New York City Restaurant Inspection Results
https://catalog.data.gov/dataset/dohmh-new-york-city-restaurant-inspection-results

* Yelp Dataset
https://www.yelp.com/developers/documentation/v3/get_started

Note: For a Yelp API key, see: https://www.yelp.com/developers/

## R Code

### R Package Management
This repo uses a combination of Packrat and Pacman R libraries for package management.
* Packrat: https://rstudio.github.io/packrat/
* Pacman: https://cran.r-project.org/web/packages/pacman/vignettes/Introduction_to_pacman.html

## Python Code

### Python Prerequisites

* Install `virtualenv` https://virtualenv.pypa.io/en/stable/installation/
* Create a Python 3 virtual environment called `venv` in project root directory
* Run: `pip install -r requirements.txt` within the activated virtual environment
* If modifying a script and adding a new package, make sure to update the `requirements.txt` file: `pip freeze > requirements.txt`

## Pipeline

Tasks are managed using Luigi (https://luigi.readthedocs.io/en/stable/)
* In your virtual environment type `luigid`
* Then, in a separate command line window, run tasks found in `pipeline.py`, using `python3 -m luigi --module pipeline [TASK_NAME]`
