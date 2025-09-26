import pandas as pd # type: ignore

# Load both CSVs and xlsx 5019, 4602 ==9553
sea_web_data = pd.read_excel('/home/abhishekyadav/stig_project/src/data/Seaweb data sample 18 09 25.xlsx')

sers_fleet_share_data = pd.read_csv('/home/abhishekyadav/stig_project/src/data/SERS_Fleet_sharepoint.csv')
# sers_fleet_share_data=sers_fleet_share_data.rename(columns={'LR No': 'IMO No'})
case_summary_data=pd.read_csv('/home/abhishekyadav/stig_project/src/data/CASE SUMMARY(Export).csv')

regions_data=pd.read_csv('/home/abhishekyadav/stig_project/src/data/Regions Countries for STIG 22 09 25(Region Country correct for STIG).csv', encoding='ISO-8859-1')
auto_mobile_data=pd.read_csv('/home/abhishekyadav/stig_project/src/data/Auto-ModelListTab.csv')
ship_type_data=pd.read_csv('/home/abhishekyadav/stig_project/src/data/ship_type_details.csv')

if 'LR No' in sers_fleet_share_data.columns:
    sers_fleet_share_data = sers_fleet_share_data.rename(columns={'LR No': 'IMO No'})
if 'LRNo' in auto_mobile_data.columns:
    auto_mobile_data = auto_mobile_data.rename(columns={'LRNo': 'IMO No'})



columns1 = set(sea_web_data.columns)

# {'DOB', 'Class1', 'Technical Manager Name', 'Yard No', 'GT', 'Ship Status', 
#  'COB', 'IMO No', 'Technical Manager Country of Domicile', 'Type detail', 'LBP', 
#  'GB Owner', 'Built to LR Class', 'DWT', 'Ship Builder', 'GBO Country', 'Class 2',
#    'SSD', 'GLIMO', 'Ship Name'}
columns2 = set(sers_fleet_share_data.columns)

# {'LR No', 'SERS Comments', 'Status', 'Ship Name', 'Contract Signed Date', 'Client Name', 
#  'Project Type', 'Country', 'Client Report', 'Documents', 'Last Updated', 'Annual Sub Month'}

# Find common columns
common_columns = columns1.intersection(columns2)
get_region = pd.merge(sea_web_data, regions_data, on='Technical Manager Country of Domicile', how='inner')
combined = pd.merge(sers_fleet_share_data, sea_web_data, on='IMO No', how='inner')

combined['IMO No'] = combined['IMO No'].astype(str)
case_summary_data['IMO No'] = case_summary_data['IMO No'].astype(str)
get_region['IMO No'] = get_region['IMO No'].astype(str)
auto_mobile_data['IMO No'] = auto_mobile_data['IMO No'].astype(str)

# Now merge
combined = pd.merge(combined, case_summary_data, on='IMO No', how='inner')

combined=pd.merge(combined, get_region, on='IMO No', how='inner')
combined.columns= [col.replace('_x', '').replace('_y', '') for col in combined.columns]

# Merge the DataFrames to check if the 'IMO No' exists in both files
combined = pd.merge(combined, auto_mobile_data, on='IMO No', how='left')


# Create a new column to indicate "Yes" or "No" based on the match
combined['transfer'] = combined['IMO No'].apply(lambda x: 'Y' if x == 'both' else 'N')
valid_imo_numbers = set(combined['IMO No'])
# Step 3: Check if 'IMO No' from file1 exists in the set of valid IMO numbers
combined['sers_sister'] = combined['IMO No'].apply(lambda x: 'Y' if x in valid_imo_numbers else 'N')


#################################-----------------ship type get -------------############
combined.columns= [col.replace('_x', '').replace('_y', '') for col in combined.columns]
combined = combined.loc[:, ~combined.columns.duplicated()]

combined = pd.merge(combined, ship_type_data, on='Type detail', how='inner')

################################---------------------------------###################

#####_____________________________________############

sea_web_data['IMO Count'] = sea_web_data['GLIMO'].map(sea_web_data['GLIMO'].value_counts())
def assign_role_group(group):
    count = len(group)
    if count == 1:
        group['Client lead / sister'] = 'Lead'
    elif count == 2:
        earliest_dob = group['DOB'].min()
        group['Client lead / sister'] = group['DOB'].apply(lambda x: 'Lead' if x == earliest_dob else 'Sister')
    else:
        latest_dob = group['DOB'].max()
        group['Client lead / sister'] = group['DOB'].apply(lambda x: 'Sister' if x == latest_dob else 'Lead')
    
    return group

client_ls = sea_web_data.groupby('GLIMO', group_keys=False).apply(assign_role_group)
client_ls['IMO No']=client_ls['IMO No'].astype(str)

combined=pd.merge(combined, client_ls, on='IMO No', how='left')

#################------------------------------------#####################
def get_fee_column(row):
    
    for col in ['Status', 'transfer', 'sers_sister']: 
            return col
    return row['Client lead / sister']  
combined['Enrol type'] = combined.apply(get_fee_column, axis=1)

######################### Enrol Fee ###############
fee_mapping = {

    'transfer': 0,
    'sers_sister': 1000,
    'Lead': 5000,
    'Sister': 1000,
    'Passenger Ship': 10300,
    'Ferry': 10300,
    'Container': 7700,
    'Tanker': 5750,
    'Bulk Carrier':	6400,
    'Ro/Ro Cargo': 7700,
    'vehicle carrier': 7700,
    'gas':7050,
    'General Cargo'	:7050,
    'OSV': 7050,
    'Yacht'	:9500,
    'Other'	:7050,
}
combined['Enrol Fee'] = combined['Enrol type'].map(fee_mapping).fillna(0)



#########################-----------------------------####################
combined.columns= [col.replace('_x', '').replace('_y', '') for col in combined.columns]

combined = combined.loc[:, ~combined.columns.duplicated()]

select_col=['Technical Manager Name', 'Technical Manager Country of Domicile', 'Area', 'Client Name','Ship Name', 
            'Ship Type',  'Type detail', 'IMO No', 'Class1', 'Class 2', 'GB Owner', 'GBO Country',
              'LBP', 'GT', 'DWT', 'Ship Status', 'DOB', 'COB',
            'Ship Builder', 'Yard No', 'Built to LR Class' , 'SERS DESCRP.NOTE FLAG',
              'ECO NOTATION FLAG', 'SSD', 'GLIMO', 'Contract Signed Date', 'Status' ,
              'transfer', 'sers_sister', 'Client lead / sister', 'Enrol type', 'Enrol Fee']
final_data=combined[select_col]
final_data.to_csv('stig_detail_table.csv', index=False)

