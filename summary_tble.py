import pandas as pd # type: ignore
import numpy as np # type: ignore
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

df = pd.read_csv('/home/abhishekyadav/stig_project/src/stig/stig_detail_table_26.csv')


# Index(['Technical Manager Name', 'Technical Manager Country of Domicile',
#        'Area', 'Client Name', 'Ship Name', 'Type detail', 'IMO No', 'Class1',
#        'Class 2', 'GB Owner', 'GBO Country', 'LBP', 'GT', 'DWT', 'Ship Status',
#        'DOB', 'COB', 'Ship Builder', 'Yard No', 'Built to LR Class',
#        'SERS DESCRP.NOTE FLAG', 'ECO NOTATION FLAG', 'SSD', 'GLIMO',
#        'Contract Signed Date', 'Status', 'transfer', 'sers_sister',
#        'Client lead / sister', 'Enrol type', 'Enrol Fee'],
#       dtype='object')
 # Clean and preprocess the data
for col in ['Technical Manager Name', 'Type detail', 'Class1', 'Enrol Fee', 'Ship Type']:
    if col in df.columns:
        df[col] = df[col]

# Pivot and aggregate data
expected_ship_type=['Bulk carrier',	'Container ship', 'Ferry',	'Gas',	'General cargo',
                    	'Oil tanker',  'OSV',	'Other type',	
                    'Passenger'	,'Roro cargo',	'Vehicle carrier'	,'Yacht'
] 
ship_type_counts = df.groupby(['Technical Manager Name', 'Technical Manager Country of Domicile', 'Area', 'Ship Type']).size().unstack(fill_value=0)
ship_type_counts = ship_type_counts.reindex(columns=expected_ship_type, fill_value=0)
# fee_sum = df.groupby(['Technical Manager Name', 'Ship Type'])['Enrol Fee'].sum().reset_index(name='fee_sum')

# # Filter for Ship Type 'bb'
# fee_sum_bb = fee_sum[fee_sum['Ship Type'] == 'Bulk carrier']
# print(fee_sum_bb)
# exit()

ship_type_counts.to_csv('ship_type_counts.csv', index=False)
expected_class1 = ['AB', 'BV', 'CC', 'IR', 'KR', 'NK', 'OT', 'RI', 'VL', 'LR'] 
Class1_counts = df.groupby(['Technical Manager Name',  'Technical Manager Country of Domicile' , 'Area', 'Class1']).size().unstack(fill_value=0)
Class1_counts = Class1_counts.reindex(columns=expected_class1, fill_value=0)
Class1_counts.to_csv('Class1_counts.csv', index=False)
# Combine the pivot tables and add additional columns

combined = ship_type_counts.join(Class1_counts, how='outer', lsuffix='_ship', rsuffix='_class').fillna(0)

combined['fleet_total'] = ship_type_counts.sum(axis=1)
combined.to_csv('Class1_counts.csv', index=False)



group_cols=['Technical Manager Name',  'Technical Manager Country of Domicile', 'Area']

#------------------- csi value-------------------------#
in_csi_df = df[df['Status'] != 'Contract sent']
csi_counts = in_csi_df.groupby(group_cols).size().reset_index(name='csi')
final_df = combined.reset_index()
final_df = final_df.merge(csi_counts, on=group_cols, how='left')
final_df['csi'] = final_df['csi'].fillna(0).astype(int)


#------------------cse value----------------------#
in_cse_df = df[df['Status'] == 'Contract sent']
cse_counts = in_cse_df.groupby(group_cols).size().reset_index(name='cse')
final_df = final_df.merge(cse_counts, on=group_cols, how='left')
final_df['cse'] = final_df['cse'].fillna(0).astype(int)

#------------------------------SERS client: LR ships missing---------------------------------#

#-----------------------------CSI LR -----------------------------#
final_df['CSI LR']=final_df[['LR', 'csi']].min(axis=1)

#---------------------------end ---------------------------#

#--------------------Not CSI LR --------------#
final_df['Not CSI LR'] = final_df['LR'] - final_df['CSI LR']

#----------------------------end------------------------------#

final_df['enrol_lr_missing']=1000 # fill count





#--------------------------------------------------------------------#
final_df['cpe_lr_missing'] = (final_df['enrol_lr_missing'] / final_df['Not CSI LR'].replace(0, pd.NA)).fillna(0).round(2)


#---------------------jcob ---------------------#

merged_df_cob = pd.merge(final_df, df[['Technical Manager Name', 'Technical Manager Country of Domicile', 'Area', 'COB']],
                     on=group_cols, how='left')

grouped = merged_df_cob[(final_df['Not CSI LR'].notna()) & (merged_df_cob['COB'] == 'Japan')] \
            .groupby(group_cols).size().reset_index(name='jcob_ship_missing')
final_df = final_df.merge(grouped, on=group_cols, how='left')
final_df['jcob_ship_missing'] = final_df['jcob_ship_missing'].fillna(0).astype(int)

#------------------------------end -----------------------------#



#-----------------------  Fleet: Class v SERS--------------------#
final_df['All SERS CSI'] = np.where(final_df['csi'] > final_df['fleet_total'], 'y', 'n')

lr_count = Class1_counts["LR"].reset_index(drop=True)# change value LR class

final_df['All LR']=np.where(lr_count > final_df['fleet_total'], 'y', 'n')

#_______________________end_____________________________#



#---------------------cpe SERS client: LR ships --------------------------#

# Step 1: Managers who have at least one ship that is CSI (Status != 'Contract sent')
managers_with_csi = df[df['Status'] != 'Contract sent'][group_cols[0]].dropna().unique()

# Step 2: Filter full data for those managers only
df_with_csi_managers = df[df[group_cols[0]].isin(managers_with_csi)]


# Step 3: Find their ships which are Class1 == 'LR' AND Status == 'Contract sent'
lr_not_csi = df_with_csi_managers[
    (df_with_csi_managers['Class1'] == 'LR') &
    (df_with_csi_managers['Status'] == 'Contract sent')
]

# Step 4: Group and count per manager
grouped_no = lr_not_csi.groupby(group_cols).size().reset_index(name='no_lr_ship')

# Step 5: Merge into final_df
final_df = pd.merge(final_df, grouped_no, on=group_cols, how='left')
final_df['no_lr_ship'] = final_df['no_lr_ship'].fillna(0).astype(int)


final_df['enrol_lr_ship']=1000 # fill count
final_df['cpe_lr_ship'] = (final_df['enrol_lr_ship'] / final_df['no_lr_ship'].replace(0, pd.NA)).fillna(0).round(2)


lr_ships_japan = lr_not_csi[lr_not_csi['COB'] == 'Japan']

grouped_japan = lr_ships_japan.groupby(group_cols).size().reset_index(name='jcob_lr_ship')
final_df = pd.merge(final_df, grouped_japan, on=group_cols, how='left')
final_df['jcob_lr_ship'] = final_df['jcob_lr_ship'].fillna(0).astype(int)



#---------------------cpe SERS client: LR client --------------------------#


# Step 1: Tech Managers with SERS status CSI
tech_mgrs_with_csi = set(df[df['Status'] != 'Contract sent'][group_cols[0]].unique())

# Step 2: Tech Managers with LR class ships
tech_mgrs_with_lr = set(df[df['Class1'] == 'LR'][group_cols[0]].unique())

# Step 3: Tech Managers without CSI and LR ships
tech_mgrs_no_csi_no_lr = set(df[group_cols[0]].unique()) - tech_mgrs_with_csi - tech_mgrs_with_lr

# Step 4: Filter ships belonging to these Tech Managers with SERS status CSE
filtered_df = df[(df[group_cols[0]].isin(tech_mgrs_no_csi_no_lr)) & (df['Status'] != 'Contract sent')]
grouped_no = filtered_df.groupby(group_cols).size().reset_index(name='no_lr_client')

final_df = pd.merge(final_df, grouped_no, on=group_cols, how='left')

final_df['no_lr_client'] = final_df['no_lr_client'].fillna(0).astype(int)


final_df['enrol_lr_client']=1000 # fill count
final_df['cpe_lr_client'] = (final_df['enrol_lr_client'] / final_df['no_lr_client'].replace(0, pd.NA)).fillna(0).round(2)


tech_mgrs_with_csi = set(df[df['Status'] != 'Contract sent'][group_cols[0]].unique())

tech_mgrs_with_lr = set(df[df['Class1'] == 'LR'][group_cols[0]].unique())

# Step 3: Tech Managers without CSI and LR ships
tech_mgrs_no_csi_no_lr = set(df[group_cols[0]].unique()) - tech_mgrs_with_csi - tech_mgrs_with_lr
filtered_df = df[
    (df[group_cols[0]].isin(tech_mgrs_no_csi_no_lr)) & (df['COB'] == 'Japan')
]

# Step 5: Group by group_cols and count ships
grouped_no = filtered_df.groupby(group_cols).size().reset_index(name='jcob_lr_client')

# Step 6: Merge with final_df
final_df = pd.merge(final_df, grouped_no, on=group_cols, how='left')

# Step 7: Fill NaNs with 0 and convert to int
final_df['jcob_lr_client'] = final_df['jcob_lr_client'].fillna(0).astype(int)


#-------------------------------------#

final_df.to_csv('summary_data_26.csv', index=False)
