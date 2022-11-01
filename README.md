# Arine Data Engineer Assignment

This project contains the necessary models to load and create patient medicine summaries.

# File Structure

```
├── src                     # source code folder
│   ├── models              # file type data models (e.g. Enrollment)
│   ├── tasks               # tasks to run jobs using models
│   ├── utils               # helper classes and functions
├── tests                   # unit test files
│   ├── models              # model unit tests
├── sample - data           # files provided for this assignment
├── output - data           # output files for this assignment
│   ├── patient_records     # patient summary output files
├── environment.yml         # python environment requirements file
├── setup.cfg               # lint & other configurations
├── setup.py                # python project setup file
```

# Installation

```bash
# conda example
$ conda env create -f environment.yml
$ conda activate de-assignment
$ pip install -e .
```

# Usage

```bash
# run unit tests
$ pytest
```
```bash
# run tasks
$ python src/tasks/run.py
```

## Question & Answers
1. How many patients were enrolled in the program as of July 1st, 2020? 1726
2. How many rows are there in the initial pharmacy claims data set? 11524
3. How many prepared claims do you have at the end of step 3? 9061
4. What is the highest amount_allowed? Which patient and generic drug does it correspond to? amount: 35.6, Elliot Ness, rosuvastatin calcium
5. How many unique generic names for the patient Abe Lincoln? 4

## Walkthrough of steps above
https://user-images.githubusercontent.com/117159158/199203150-60844a06-f1df-4528-92df-6ff917f16055.mp4
