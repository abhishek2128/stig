import pandas as pd # type: ignore
import numpy as np # type: ignore
# Load the data
df = pd.read_csv('/home/abhishekyadav/stig_project/src/stig/stig_detail_table.csv')
# Index(['Technical Manager Name', 'Technical Manager Country of Domicile',
#        'Area', 'Client Name', 'Ship Name', 'Type detail', 'IMO No', 'Class1',
#        'Class 2', 'GB Owner', 'GBO Country', 'LBP', 'GT', 'DWT', 'Ship Status',
#        'DOB', 'COB', 'Ship Builder', 'Yard No', 'Built to LR Class',
#        'SERS DESCRP.NOTE FLAG', 'ECO NOTATION FLAG', 'SSD', 'GLIMO',
#        'Contract Signed Date', 'Status', 'transfer', 'sers_sister',
#        'Client lead / sister', 'Enrol type', 'Enrol Fee'],
#       dtype='object')
 # Clean and preprocess the data
for col in ['Technical Manager Name', 'Type detail', 'Class1', 'Enrol Fee']:
    if col in df.columns:
        df[col] = df[col]

# Pivot and aggregate data
ship_type_counts = df.groupby(['Technical Manager Name', 'Technical Manager Country of Domicile', 'Area', 'Type detail']).size().unstack(fill_value=0)

Class1_counts = df.groupby(['Technical Manager Name',  'Technical Manager Country of Domicile' , 'Area', 'Class1']).size().unstack(fill_value=0)

# Process the enrolment fee and calculate sums and averages
df['Enrol Fee'] = pd.to_numeric(df['Enrol Fee'], errors='coerce')

total_enroll_count = df.groupby(['Technical Manager Name',  'Technical Manager Country of Domicile', 'Area'])['Enrol Fee'].sum()
# Combine the pivot tables and add additional columns
combined = ship_type_counts.join(Class1_counts, how='outer', lsuffix='_ship', rsuffix='_class').fillna(0)


combined['fleet_total'] = ship_type_counts.sum(axis=1)




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
final_df['CSI LR']=1

#-----------------------------CSI LR -----------------------------#
final_df['CSI LR'] = df[(df['Status'] != 'CSI') & (df['Class1'] == 'LR')].shape[0]
final_df['CSI LR'] = final_df[['fleet_total', 'csi']].min().min()

#--------------------Not CSI LR --------------#
final_df['Not CSI LR'] = final_df['fleet_total'] - final_df['CSI LR']


#------------------------------JCOB SERS client: LR ships missing---------------------------------#
# final_df['jcob_ship_missing']=df[df['COB'] == 'japan']
# # Get the count of such ships
# final_df['jcob_ship_missing'] = final_df['jcob_ship_missing'].shape[0]

#---------------------cpe SERS client: LR ships missing--------------------------#
final_df['no_lr_missing']=0 # fill count
final_df['enrol_lr_missing']=0 # fill count
final_df['cpe_missing'] = (final_df['enrol_lr_missing'] / final_df['Not CSI LR'].replace(0, pd.NA)).fillna(0).round(2)
final_df['jcob_lr_missing']=0

#-----------------------  Fleet: Class v SERS--------------------#
# Assign 'y' if A > B, else 'n'
# filtered = df[df['class_type'] == 'lr']
# grouped = filtered.groupby(group_cols).agg(
#     count=('score', 'count'),
# ).reset_index()

# print(grouped)
final_df['All SERS CSI'] = np.where(final_df['csi'] > final_df['fleet_total'], 'y', 'n')


lr_count = Class1_counts["LR"].reset_index(drop=True)# change value LR class

final_df['All LR']=np.where(lr_count > final_df['fleet_total'], 'y', 'n')


#---------------------cpe SERS client: LR ships --------------------------#
final_df['no_lr_ship']=0 # fill count
final_df['enrol_lr_ship']=0 # fill count
final_df['cpe_lr_ship'] = (final_df['enrol_lr_ship'] / final_df['no_lr_ship'].replace(0, pd.NA)).fillna(0).round(2)
final_df['jcob_lr_ship']=0

#---------------------cpe SERS client: LR client --------------------------#
final_df['no_lr_client']=5 # fill count
final_df['enrol_lr_client']=0 # fill count
final_df['cpe_lr_client'] = (final_df['enrol_lr_client'] / final_df['no_lr_client'].replace(0, pd.NA)).fillna(0).round(2)

#---------------------------------------------------------#

merged_df_cob = pd.merge(final_df, df[['Technical Manager Name', 'Technical Manager Country of Domicile', 'Area', 'COB']],
                     on=group_cols, how='left')

grouped = merged_df_cob[(merged_df_cob['no_lr_client'].notna()) & (merged_df_cob['COB'] == 'Netherlands')] \
            .groupby(group_cols).size().reset_index(name='jcob_lr_client')


final_df = final_df.merge(grouped, on=group_cols, how='left')

final_df['jcob_lr_client'] = final_df['jcob_lr_client'].fillna(0).astype(int)

final_df.to_csv('summary_data.csv', index=False)
