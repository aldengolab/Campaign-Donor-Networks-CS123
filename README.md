# Campaign-Donor-Networks-CS123
### _Alden Golab, Paul Mack_
Code for measuring US campaign donations for F500 companies. Class project for CS 123 (Spring 2016) at the University of Chicago, utilizing parallelization through MapReduce to enhance performance on Elastic Map Reduce (EMR), S3, and EC2 on Amazon Web Services. 

Data provided by the Center for Responsive Politics at OpenSecrets.org via Sunlight’s Influence Explorer tool, with 15.1 GB of data amounting to about 5.1 million contribution records from 1990-2012.

See Data Dictionary:
http://data.influenceexplorer.com/docs/contributions/

Get the full State & Federal Campaign Contributions data: 
http://data.influenceexplorer.com/bulk/

## What's Here

There are a few files in this directory worth pointing out: 

+ `Golab&Mack_FinalReport.pdf` is the final report of findings.
+ `Golab&Mack_Presentation.pdf` is our final presentation, which includes visuals instead of just dry text.
+ `requirements.txt` is the requirements file for running; all code is in Python 2.7
+ `aws1.json` is the JSON used for entity resolution in the large dataset
+ `ftest.csv` is the list of Fortune 500 companies
+ `count_json.py` provides summary statistics for the entity resolution JSON

You'll find three relevant folders with scripts in each for the following:

1. `explore` 
  + `explore_1.py` - sums unique contributors' contributions
  + `explore_3.py` - pulls unique donor/recipient pairs from dataset
  + `sample_records.py` - randomly samples from the full dataset
2. `entity_reduction_scripts`
  + `F500_json_MR.py` - bulids a JSON of {nameInstance: authoritativeName} for all Fortune 500 companies
3. `data_crunching` 
  + `corporate_donations_MRJob.py` - aggregates F500 corporate donations into a csv
  + `individual_donations_MRJob.py`- aggregates individual F500 employee donations into a csv
