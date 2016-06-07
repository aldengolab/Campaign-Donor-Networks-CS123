# Campaign-Donor-Networks-CS123
### _Alden Golab, Paul Mack_
Code for measuring US campaign donor networks. Class project for CS 123 at the University of Chicago, utilizing parallelization through MapReduce to enhance performance. 

Data provided by the Center for Responsive Politics at OpenSecrets.org via Sunlight’s Influence Explorer tool.

See Data Dictionary:
http://data.influenceexplorer.com/docs/contributions/

## What's Here

You'll find three relevant folders with scripts in each for the following:

1. `explore` 

..* `explore_1.py` - sums unique contributors' contributions
..* `explore_3.py` - pulls unique donor/recipient pairs from dataset
..* `sample_records.py` - randomly samples from the full dataset

2. `entity_reduction_scripts`

..* `F500_json_MR.py` - bulids a JSON of {nameInstance: authoritativeName} for all Fortune 500 companies

3. `data_crunching` contains three scripts: 

..* `corporate_donations_MRJob.py` - aggregates F500 corporate donations into a csv
..* `individual_donations_MRJob.py`- aggregates individual F500 employee donations into a csv
