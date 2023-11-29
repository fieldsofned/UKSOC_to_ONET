import pandas as pd

#read csv lookup and remove rows with }}}} string
df = pd.read_csv("SOCUK_to_ISOC_2.csv")
df = df[df["SOC_2020"].str.contains(r"}}}}") == False]

#get a list of unique SOC codes
uk_soc_unique_list = list(df.SOC_2020.unique())

#for each unique combination, calculate number of occurences and add in new column
df['Group_Size'] = df.groupby(['SOC_2020', 'ISCO-08'])['SOC_2020'].transform('size')

final_UK_SOC_to_ISCO_df = pd.DataFrame(columns = ("SOC_2020", "ISCO-08", "Percentage", "SOC2020_job_title", "ISCO08_job_title"))

#for each UK SOC code, create a new dataframe that just has the SOC_2020 code, ISOC-08 code
for i in range(0, len(uk_soc_unique_list)):
    #get the UK SOC code
    uk_soc_code_to_filter = uk_soc_unique_list[i]
    #Filter main dataframe to just that code
    filtered_df = df[df["SOC_2020"] == uk_soc_code_to_filter]
    #remove any duplicates as the groupby column takes that into account when adding group size
    filtered_df = filtered_df.drop_duplicates(subset='ISCO-08')
    #Create a new column called percentage that works out how much each combination makes up of the UK SOC
    filtered_df['Percentage'] = filtered_df.apply(lambda row: row['Group_Size'] / filtered_df['Group_Size'].sum(), axis=1)
    #Filter to remove jobs where percentage is less that 5%
    filtered_df = filtered_df[filtered_df["Percentage"] >= 0.05]
    #Re calculate the percentage after removing minor jobs
    filtered_df['Percentage'] = filtered_df.apply(lambda row: row['Percentage'] / filtered_df['Percentage'].sum(), axis=1)
    #Filter to fit into main data frame
    filtered_df = filtered_df[["SOC_2020", "ISCO-08", "Percentage","SOC2020_job_title", "ISCO08_job_title" ]]
    #add to main dataframe
    final_UK_SOC_to_ISCO_df = pd.concat([final_UK_SOC_to_ISCO_df, filtered_df], ignore_index=True)

#I've already pre filtered this to only include unique rows as there were duplicates as the orignal went into more than 4 digit detail
#Second part is to join up existing table with ISOC to ONET
df = pd.read_csv("ISOC_to_ONET.csv")

#update column types so that dataframes merge, then merge
final_UK_SOC_to_ISCO_df['ISCO-08'] = final_UK_SOC_to_ISCO_df['ISCO-08'].astype("int")
df['ISCO-08'] = df['ISCO-08'].astype("int")
combined_df = pd.merge(df, final_UK_SOC_to_ISCO_df, on='ISCO-08', how='left')

#get another list of unique soc codes in the combined dataframe
uk_soc_unique_list_2 = list(combined_df.SOC_2020.unique())

#set up dataframe to save into later
final_df = pd.DataFrame(columns = ("ISCO-08", "ONET-SOC", "Job title", "SOC_2020","Percentage","SOC2020_job_title","ISCO08_job_title" ))

#For each unique SOC code
for i in range(0, len(uk_soc_unique_list_2)):
    #filter to soc code
    soc_code_to_filter = uk_soc_unique_list_2[i]
    filtered_df_2 = combined_df[combined_df["SOC_2020"] == soc_code_to_filter]
  
    #get isco-08 codes for filtered df
    unique_ISCO_08_codes = list(filtered_df_2['ISCO-08'].unique())
  
    #for isco codes for SOC code, filter so that it's just that in the dataframe
    for i in unique_ISCO_08_codes:
        filtered_df_3 = filtered_df_2[filtered_df_2["ISCO-08"] == i]
 
        #Divide the percentage by the number of times it occurs
        filtered_df_3 ['Percentage'] = filtered_df_3.apply(lambda row: row['Percentage'] / filtered_df_3['Percentage'].count(), axis=1)
        final_df = pd.concat([final_df, filtered_df_3], ignore_index=True)

final_df.to_csv("final_lookup_v2.csv", index = False)