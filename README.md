HMIS and Connecting Point cleaning and analysis
===

**Compiled 20 November 2014 for the City of San Francisco Mayor's Office**

*Carl Shan, Isaac Hollander McCreery, Brent Gaisford, Swati Jain, Bayes Impact*

Overview
---

This package was created by the team at Bayes Impact for the San Francisco Mayor's Office as a final deliverable for the Fall 2014 data exploration conducted in pursuit of a Pay for Success (PFS) model to be implemented in San Francisco's homelessness services system.

Over the course of 6 weeks, the Bayes Impact team assessed, cleaned, and analyzed the data provided by the City's HMIS and Compass Connecting Point systems.  In this package are the scripts we wrote to clean the data, as well as notebooks we used for exploration and data analysis.

The files included in this package (in `./exploration/`) are listed below.

- **clean.py** is the script we wrote to clean the data.  It is explained more below, in the section *Data Cleaning*, and is also annotated inline.
- **util.py** is a package of a few utilities we wrote to assist with analysis.  It is annotated inline.
- **data-exports.py** is a simple notebook for exporting data.
- **2014-11-04-hmis-metrics.ipynb** - This is an [iPython Notebook](www.ipython.org) that contains the Python code detailing how metrics for the HMIS data was calculated, and then visualized. The metrics this Notebook covers include:
	- How many families are there currently in shelter?
	- How many families entered the shelter system each year since 2009?
	- How many families, and how many children have entered the shelter program since Jan 15 of 2014?
	- What is the average and median homeless family size? How has that changed over time?
	- How long did the average family stay in a shelter in each quarter over the past four years?
	- What is the distribution of times families are spending in a shelter, and how has that changed over time?
	- How many homeless families, and individuals have entered the shelter system as broken down by the specific shelter?
	- How many families were in shelters on an average night in each quarter over the past four years
- **2014-11-20-CP-metrics.ipynb** - This is an iPython Notebook detailing the calculation and visualization of metrics for the Connecting Point data. The metrics this Notebook covers include:
	- How many families were on waitlist on an average night in each quarter over the past two years?
	- What is the distribution of time spent by families on the waitlist, over the past few years?
	- What is the average and minimum number of days spent by families on the waitlist, for each quarter in the past few years?

Data Generation
---

### Overview

Put raw data in `./data/`.  Open a python shell or notebook in `./exploration/`:

```
import pandas as pd
import clean

(hmis, cp) = clean.get_hmis_cp()
```

### Important columns in the data

Most columns in the `hmis` and `cp` dataframes are self-explanatory.  However, there are a few worth mentioning.

#### HMIS

- `Subject Unique Identifier` is the global individual identifier in HMIS, (it links to `Clientid` in Connecting Point).
- `Family Identifier` is the global family identifier in HMIS, (it links to `Familyid` in Connecting Point).
- `Raw Subject Unique Identifier` is the raw individual identifier.
- `Family Site Identifier` is the raw family identifier.
- `Family?` is a boolean that indicates whether this person has ever or will ever enter with a family; this allows us to track individuals within families before and/or after they actually enter with a full family.

#### Connecting Point

- `Clientid` is the global individual identifier in Connecting Point, (it links to `Subject Unique Identifier` in HMIS).
- `Familyid` is the global family identifier in Connecting Point, (it links to `Family Identifier` in HMIS).
- `Raw Clientid` is the raw individual identifier.
- `Caseid` is the raw family identifier.
- `Family?` (same as in HMIS) is a boolean that indicates whether this person has ever or will ever enter with a family; this allows us to track individuals within families before and/or after they actually enter with a full family.

### Cleaning

The bulk of the work done on this project was in data cleaning.  **clean.py** is the authoritative source on what cleaning was done, but a brief overview is provided here.

`get_hmis_cp` is the main function to call in this script.  It pulls in relevant CSVs from `./data/`, merges them, cleans them, and returns a tuple containing the cleaned HMIS data and the cleaned Connecting Point data.  All other functions in **clean.py** are in service of `get_hmis_cp`.

Cleaning has a few steps:

1. `get_raw_hmis` and `get_raw_cp` import the raw data and merge it appropriately;
- `hmis_convert_dates` and `cp_convert_dates` parse the date columns in each dataframe;
- `get_client_family_ids` uses both dataframes, pulls in other files, and determines which people in the dataset are
	a. the same person, and
	b. in the same family
and returns the dataframes with the raw identifiers moved to `Raw ...` columns, and the new identifiers in the proper columns, (more explanation provided below);
- `hmis_child_status` and `cp_child_status` determine whether each person is a child or adult; and
- `hmis_generate_family_characteristics` and `cp_generate_family_characteristics` determine other family characteristics, such as if the person is in a family for that record, and if they are ever in a family anywhere in the dataset.

#### De-duplicating individuals and determining families across time

De-duplicating individuals and determining families across time proved the hardest part of cleaning this dataset.

In Connecting Point, if the same person or family enters the waitlist as before, there is no record that they are the same family: one must instead compare personally identifying fields, (e.g. name, birth date,) to determine who is the same individual, and from there, extrapolate who is in the same family.

Similarly in HMIS, if the same person or family enters a different housing program as before, they may or may not be given the same `Family Site Identifier`, depending on if the programs are managed by the same organization.  So, like in Connecting Point, one must instead compare personally identifying fields, (e.g. name, birth date,) to determine who is the same individual, and from there, extrapolate who is in the same family.

The City of San Francisco provided us, (Bayes Impact,) with fuzzy matchings across time within and between the two datasets, created using the *RecLink* and *Link Plus* probabilistic matching software, and we used those to generate global individual and family identifiers across the two datasets.

We devised the following methodology, relying on the concept of [connected components](http://en.wikipedia.org/wiki/Connected_component_(graph_theory)) in graph theory.

1. Create a graph where each vertex represents an individual identifier, either in HMIS or in Connecting Point.
- Connect every pair of vertices in the graph that are said to be the same person by the fuzzy matchings provided by the City; this gives us a graph where each connected component represents exactly one person.
- Duplicate the graph, and connect every pair of vertices in the graph that ever showed up together, either in Connecting Point, (with the same `Caseid`,) or in HMIS, (with the same `Family Site Identifier` issued on the same date); this gives us a graph where each connected component represents exactly one family.
- For each graph, enumerate all the connected components, and assign the same global individual and family identifier to each person in the individual and family connected components, respectively.

This methodology assigns every record of the same person the same global individual identifier, (across datasets,) and assigns every record of a person in the same family the same global family identifier, (also across datasets).  This allows us to accurately see the unique families within and across datasets, (by avoiding double-counting families,) and allows us to connect families across datasets, (to see, for example, if the same family that left the waitlist entered shelter right after).

### Data files

Data files that will be processed in the cleaning step should live in `./data/`.  The files that are required for cleaning to complete properly are below.

- `./data/hmis/program with family.csv` is the `program` CSV that the City sent us, including the column `Family Site Identifier`.
- `./data/hmis/client de-identified.csv` is the `client` CSV that the City sent us; this file was not updated when we got the new `program` file, so we use this subset of clients.
- `./data/connecting_point/case.csv` is the `case` CSV exported from the Connecting Point spreadsheet document sheet `Case1`.
- `./data/connecting_point/client.csv` is the `client` CSV exported from the Connecting Point spreadsheet document sheet `Clients`.
- `./data/hmis/hmis_client_duplicates_link_plus.csv` is the *Link Plus* de-duplication CSV for HMIS the City sent us.
- `./data/connecting_point/cp_client_duplicates_link_plus.csv` is the *Link Plus* de-duplication CSV for Connecting Point the City sent us.
- `./data/matching/cp_hmis_match_results.csv` is the *RecLink* de-duplication CSV (across the two datasets) the City sent us.

We are including in the package other, unused data files as well, but the files above are the only ones used for this project.
