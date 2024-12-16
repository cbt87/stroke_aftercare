# Capstone Project Summary December 2024

## Instructions to create the cohort, model target vector, and model feature matrix in python or directly in a dbms

First, acquire database credentials through OHDSI Lab, and save them in a config file. The config file is imported to all scripts that connect to the database, but it is included in the .gitignore file for security reasons.

Run [create_stroke_cohort_and_model_1.py](src/create_stroke_cohort_and_model_1.py)

This script contains a series of functions that create the necessary tables to create a full stroke cohort and to fit the logistic regression models. At the end of the file, you will need to change the names of the following tables so that you are writing to your own personal schema in the OHDSI Lab database rather than work_tilton_ca204.

```
in_patient_stroke_table = "work_tilton_ca204.inpatient_stroke_demo" 
cohort_table = "work_tilton_ca204.stroke_cohort_w_conditions_demo" 
model_target_table = "work_tilton_ca204.model_1_target_demo" 
full_model_table = "work_tilton_ca204.model_1_full_demo" 
insurance_table = "work_tilton_ca204.model_1_insurance_demo" 
```

Alternatively, you can create the tables directly in a database management system tool like DBeaver.
Run the sql files in [src/sql_for_modeling](src/sql_for_modeling) in the following order:

1. stroke_cohort.sql
2. model_1_target.sql
3. model_1_full.sql
4. model_1_insurance.sql

**Note**: you will need to specify the table names so the sql queries write the data to your personal schema. All the table names are in {curly brackets}. For example, you will need to find and replace all instances of {stroke_cohort} into the exact table name you want to write to. 

