import redshift_connector
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import sys
import os

# Add the root directory (one level above 'src') to the system path
sys.path.append(os.path.abspath(os.path.join('..')))

# Now you can import the config module
import config

#Connect to the cluster

conn = redshift_connector.connect(
     host=config.DB_HOST,
     port=5439,
     database=config.DB_NAME,
     user=config.DB_USER,
     password=config.DB_PASSWORD
  )
  
# Create a Cursor object
cursor = conn.cursor()

# Query and receive result set
cursor.execute("SELECT sc.*, p.gender_source_value,p.location_id,p.year_of_birth FROM work_tilton_ca204.stroke_cohort_w_aphasia sc inner join omop_cdm_53_pmtx_202203.person p on sc.person_id = p.person_id;")

result = cursor.fetch_dataframe()

# Fig 1: Stroke Counts by Year
# Step 1: Aggregate the data
grouped_df = result.groupby(['year_of_birth', 'has_aphasia']).size().unstack(fill_value=0)

# Step 2: Create the plot with expanded x-axis
plt.rcParams['figure.figsize'] = [15, 7]  # Adjust the width (14) and height (6)
grouped_df.plot(kind='bar', stacked=True, width=0.8)

# Step 3: Adding Seaborn style
sns.set_style("whitegrid")
plt.title("Incidence of Stroke and Aphasia by Year of Birth")
plt.xlabel("Year of Birth")
plt.ylabel("Count")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed

# Step 4: Customize the legend
plt.legend(labels=["Stroke with no aphasia", "Stroke with aphasia"], title=None)

# Show the plot
plt.grid(False)
plt.tight_layout()  # Ensure everything fits within the plot
plt.savefig("../figs/stroke_cts_by_yob.jpg")



# Fig 2: Stroke counts by YOB and Gender

grouped_df = result.groupby(['year_of_birth', 'gender_source_value']).size().unstack(fill_value=0)

# Step 2: Create the plot with expanded x-axis
plt.rcParams['figure.figsize'] = [15, 7]  # Adjust the width (14) and height (6)
grouped_df.plot(kind='bar', stacked=True, width=0.8)

# Step 3: Adding Seaborn style
sns.set_style("whitegrid")
plt.title("Incident of Stroke by Year of Birth and Gender")
plt.xlabel("Year of Birth")
plt.ylabel("Count")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed

plt.legend(labels=["Female", "Male"], title=None)

# Show the plot
plt.grid(False)
plt.tight_layout()  # Ensure everything fits within the plot
plt.savefig("../figs/stroke_ct_by_yob_gender.jpg")


#Fig 3: Stroke counts by state


cursor.execute("SELECT sc.*, p.gender_source_value,p.location_id,p.year_of_birth, l.* FROM work_tilton_ca204.stroke_cohort_w_aphasia sc \
inner join omop_cdm_53_pmtx_202203.person p on sc.person_id = p.person_id \
inner join omop_cdm_53_pmtx_202203.location l on \
p.location_id = l.location_id;")

result = cursor.fetch_dataframe()
# Step 1: Aggregate the data
grouped_df = result.groupby(['state', 'has_aphasia']).size().unstack(fill_value=0)

# Step 2: Sort the DataFrame by the total count (sum of each row)
grouped_df['Total'] = grouped_df.sum(axis=1)  # Create a 'Total' column
grouped_df = grouped_df.sort_values(by='Total', ascending=False)  # Sort by 'Total'
grouped_df = grouped_df.drop(columns='Total')  # Remove the 'Total' column after sorting

# Step 3: Create the plot with expanded x-axis
#plt.figure(figsize=(14, 6))  # Adjust the width and height as needed
grouped_df.plot(kind='bar', stacked=True, width=0.8)

# Step 4: Adding Seaborn style
sns.set_style("whitegrid")
plt.title("Count of stroke cohort members by state")
plt.xlabel("State")
plt.ylabel("Count")
plt.xticks(rotation=45)  # Rotate x-axis labels if needed
plt.legend(labels=["Stroke with no aphasia", "Stroke with aphasia"], title=None)
# Show the plot
plt.rcParams['figure.figsize'] = [15, 7]
plt.grid(False)
plt.tight_layout()  # Ensure everything fits within the plot
plt.savefig("../figs/stroke_ct_by_state.jpg")



#fig 4: count of visits per specialty

cursor.execute("select p.specialty_source_value, count(*) from \
(   \
select * from work_tilton_ca204.stroke_cohort_w_aphasia s \
inner join omop_cdm_53_pmtx_202203.visit_occurrence vo \
on s.person_id = vo.person_id and s.condition_start_date <= vo.visit_start_date \
where vo.provider_id is not null \
) a \
inner join omop_cdm_53_pmtx_202203.provider p \
on a.provider_id = p.provider_id \
group by p. specialty_source_value \
order by count desc;")

result = cursor.fetch_dataframe()
result['specialty_source_value'] = result["specialty_source_value"].fillna('No Specialty')
top_specialities = result.iloc[:20,:]

plt.figure(figsize=(10, 6))
sns.barplot(x='specialty_source_value', y='count', data=top_specialities)
plt.xticks(rotation=90)  # Rotate x labels if needed
plt.title("Top 20 Provider specialties for all cohort visits after entry date")
plt.xlabel("Specialty")
plt.ylabel("Count")
plt.tight_layout()

plt.savefig("../figs/visit_ct_specialty.jpg")

# fig 5: count of patients who visited SLPs after cohort entry date by aphasia status
cursor.execute("""WITH filtered_procedures AS (
    SELECT person_id, procedure_concept_id, procedure_date
    FROM omop_cdm_53_pmtx_202203.procedure_occurrence
    WHERE procedure_concept_id in (44816446, 2313701, 2314188)
),
person_classification AS (
    SELECT 
        sc.person_id,
        sc.has_aphasia,
        CASE  
            WHEN COUNT(CASE WHEN sc.condition_start_date <= po.procedure_date THEN 1 END) > 0 THEN 'not_null'
            ELSE 'null' 
        END AS person_status
    FROM work_tilton_ca204.stroke_cohort_w_aphasia sc
    LEFT JOIN filtered_procedures po 
        ON sc.person_id = po.person_id
    GROUP BY sc.person_id, sc.has_aphasia
)
SELECT 
    COUNT(CASE WHEN person_status = 'null' THEN 1 END) AS count_null,
    COUNT(CASE WHEN person_status = 'not_null' THEN 1 END) AS count_not_null,
    
    COUNT(CASE WHEN person_status = 'null' AND has_aphasia = 1 THEN 1 END) AS count_null_has_aphasia_1,
    COUNT(CASE WHEN person_status = 'not_null' AND has_aphasia = 1 THEN 1 END) AS count_not_null_has_aphasia_1,
    
    COUNT(CASE WHEN person_status = 'null' AND has_aphasia = 0 THEN 1 END) AS count_null_has_aphasia_0,
    COUNT(CASE WHEN person_status = 'not_null' AND has_aphasia = 0 THEN 1 END) AS count_not_null_has_aphasia_0
FROM person_classification;
""")

result = cursor.fetch_dataframe()

# Prepare the data for melting
df_melted = pd.DataFrame({
    'status': ['No SLP visits', 'No SLP visits', 'One or more SLP visits', 'One or more SLP visits'],
    'has_aphasia': [1, 0, 1, 0],
    'count': [
        result['count_null_has_aphasia_1'][0],  # Null and has_aphasia = 1
        result['count_null_has_aphasia_0'][0],  # Null and has_aphasia = 0
        result['count_not_null_has_aphasia_1'][0],  # Not Null and has_aphasia = 1
        result['count_not_null_has_aphasia_0'][0]   # Not Null and has_aphasia = 0
    ]
})


plt.close('all')
# Set up the figure and the positions for the bars
fig, ax = plt.subplots()

# X-axis positions for 'Null' and 'Not Null'
x = np.arange(2)

# Heights for has_aphasia = 1 and has_aphasia = 0 for each category
null_has_aphasia_1 = result['count_null_has_aphasia_1'][0]
null_has_aphasia_0 = result['count_null_has_aphasia_0'][0]
not_null_has_aphasia_1 = result['count_not_null_has_aphasia_1'][0]
not_null_has_aphasia_0 = result['count_not_null_has_aphasia_0'][0]

# Plot has_aphasia = 0 first, then stack has_aphasia = 1 on top
ax.bar(x[0], null_has_aphasia_0, label='Not diagnosed with Aphasia', color = "#1f77b4")
ax.bar(x[0], null_has_aphasia_1, bottom=null_has_aphasia_0, label='Diagnosed with Aphasia', color = "C1")

ax.bar(x[1], not_null_has_aphasia_0, color = "#1f77b4")
ax.bar(x[1], not_null_has_aphasia_1, bottom=not_null_has_aphasia_0, color = "C1")

# X-axis labels
ax.set_xticks(x)
ax.set_xticklabels(['No SLP visits', 'One or more SLP visits'], fontsize = 14)

# Add labels, title, and legend
ax.set_xlabel('')
ax.set_ylabel('Count of Patients', fontsize = 14)
ax.set_title('Count of patients who visited SLPs* after cohort entry date', fontsize = 16)
ax.legend(title='Aphasia Status**')

fig.text(0.5, .02, '*SLP visit defined as an occurrence of procedure concept id in (44816446, 2313701, 2314188) \n **Diagnosed with Aphasia = multiple occurrences of the condition concept across 2 or more days', 
         ha='center', fontsize=10)


plt.savefig("../figs/SLP_visits.jpg", bbox_inches ="tight")


# Fig 6: Histogram showing counts of patients by the number of SLP visits
cursor.execute("""select count(*), sc.person_id from work_tilton_ca204.stroke_cohort_w_aphasia sc
inner join omop_cdm_53_pmtx_202203.procedure_occurrence po on 
sc.person_id = po.person_id 
where procedure_concept_id in (44816446, 2313701, 2314188)
and condition_start_date <= procedure_date 
group by sc.person_id 
order by count desc;
;""")
result = cursor.fetch_dataframe()

plt.close('all')

sns.histplot(result["count"], binwidth=5)
plt.title('Distribution of SLP visits for stroke cohort members', fontsize = 16)
plt.xlabel('Count of SLP visits per patient after entry date, binwidth = 5', fontsize = 14)
plt.ylabel('Count of patients per bin', fontsize = 14)

tick_interval = 20  # Adjust this based on your data
plt.xticks(range(int(result['count'].min()), int(result['count'].max()), tick_interval), rotation=45)

plt.savefig("../figs/SLP_hist.jpg")
plt.close('all')

# Fig 7: Histogram of SLP visits, log scale y axis
sns.histplot(result["count"], binwidth=5)
plt.title('Distribution of SLP visits for stroke cohort members', fontsize = 16)
plt.xlabel('Count of SLP visits per patient after entry date, binwidth = 5', fontsize = 14)
plt.ylabel('Count of patients per bin (log scale)', fontsize = 14)

tick_interval = 20  # Adjust this based on your data
#plt.xticks(range(int(result['count'].min()), int(result['count'].max()), tick_interval), rotation=45)
plt.yscale('log')
plt.savefig("../figs/SLP_hist_y_log_scale.jpg")


#Fig 8: count of discharge codes

cursor.execute ("""select a.*,c.concept_name  from 
(select count(*), vo.discharge_to_concept_id from work_tilton_ca204.stroke_cohort_w_aphasia scwa inner join
omop_cdm_53_pmtx_202203.condition_occurrence co  on
scwa.condition_occurrence_id = co.condition_occurrence_id  inner join 
omop_cdm_53_pmtx_202203.visit_occurrence vo on
vo.visit_occurrence_id = co.visit_occurrence_id 
group by vo.discharge_to_concept_id 
) a inner join
omop_cdm_53_pmtx_202203.concept c on
a.discharge_to_concept_id = c.concept_id
order by count desc
;""")

result = cursor.fetch_dataframe()

result["concept_name"] = result["concept_name"].replace("No matching concept", "Discharged to Home")

plt.close('all')
sns.barplot(y = "concept_name", x = "count", data = result)
plt.title('Count of patients by the facility they were discharged to after first stroke occurrence')
plt.xlabel('Count of patients')
plt.ylabel('Type of care facility')
plt.tight_layout()
plt.savefig("../figs/discharge_location.jpg")