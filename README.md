## Exploratory Analysis of DOHMH New York City Restaurant Results

### Datasets
* DOHMH New York City Restaurant Inspection Results
https://catalog.data.gov/dataset/dohmh-new-york-city-restaurant-inspection-results

* Yelp Dataset
https://www.yelp.com/developers/documentation/v3/get_started

Note: For a Yelp API key, see: https://www.yelp.com/developers/


## Python Code

### Python Setup

* Install `virtualenv` https://virtualenv.pypa.io/en/stable/installation/
* Create a Python 3 virtual environment called `venv` in project root directory
* Run: `pip install -r requirements.txt` within the activated virtual environment
* If modifying a script and adding a new package, make sure to update the `requirements.txt` file: `pip freeze > requirements.txt`
* Environment variables can be accessed several ways, on way is to use a `.env` file, then `source .env`. Format:

```
export VARIABLE_ONE_NAME=VARIABLE_ONE_VALUE
export VARIABLE_TWO_NAME=VARIABLE_TWO_VALUE
```

## R Code

### R Setup
This repo uses a combination of Packrat and Pacman R libraries for package management.
* Packrat: https://rstudio.github.io/packrat/
* Pacman: https://cran.r-project.org/web/packages/pacman/vignettes/Introduction_to_pacman.html

## Pipeline

Tasks are managed using the command line and Luigi (https://luigi.readthedocs.io/en/stable/)
* In your virtual environment type `luigid`
* Then, in a separate command line window, run tasks found in `pipeline.py`, using `python3 -m luigi --module pipeline [TASK_NAME]`
