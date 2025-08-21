import csv
import msal
import requests
import json
import time
import urllib3
import sys
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration

tenant_id = ""
client_id = ""
client_secret = ""
scopes=["https://graph.microsoft.com/.default"]
authority = f"https://login.microsoftonline.com/{tenant_id}"
sharepoint_site_domain = "lloydsregistergroup.sharepoint.com"
sharepoint_site_name = "LRSERS"  
local_file_name = "downloaded_data.csv"
list_name = "AutoModelListTab" #SERS Fleet, AutoModelListTab


# user_input_list=sys.argv[1]
# user_file_name=sys.argv[2]
# print(user_input_list, user_file_name)
# exit()


# Authentication using MSAL
def authenticate():
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )

    # Acquire access token
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception(f"❌ Token acquisition failed: {result.get('error_description')}")
    
    result["access_token"]="please enter you token"
    return result["access_token"]



# Get Site ID for SharePoint site
def get_site_id(access_token):
    site_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_domain}:/teams/{sharepoint_site_name}"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.get(site_url, headers=headers, verify=False)
    if response.status_code==200:
        site_id = response.json().get("id")
        if not site_id:
            raise Exception("❌ Failed to retrieve site ID")

        print(f"✅ Site ID: {site_id}")
        return site_id
    else:
       print("Failed to retrieve the SharePoint site URL. Please verify your credentials and permissions.")
       exit()



# Get the list ID by name
def get_list_id(site_id, access_token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers, verify=False)
    if response.status_code==200:
        response.raise_for_status()

        lists = response.json().get("value", [])
        for lst in lists:
            print(f"List Name: {lst['name']} | ID: {lst['id']}")
            if lst["name"] == list_name:
                return lst['id']

        print(f"❌ List '{list_name}' not found. Please verify that the list name or list ID is correct.")
      
    else:
       print("Error retrieving SharePoint list. Please check your credentials and request parameters.")
 




# Fetch all items from a SharePoint list (handling pagination)
def fetch_all_items(site_id, lst_id, access_token):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{lst_id}/items?expand=fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    all_items = []

    while url:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code != 200:
            #raise Exception(f"❌ Error fetching data: {response.status_code} - {response.text}")
            print((f"❌ Error fetching data: {response.status_code} - {response.text}"))
        
        
        data = response.json()
        items = data.get("value", [])
        all_items.extend(items)

        # Check for pagination
        url = data.get("@odata.nextLink", None)

        if url:
            print(f"Fetched {len(items)} items. Fetching more...")
            time.sleep(1)  # Optional delay to avoid hitting rate limits

    return all_items



# Save items to CSV
def save_to_csv(items, filename):
    if not items:
        print("❌ No items to save.")
        return

    # Initialize fieldnames from the first item
    fieldnames = list(items[0].get("fields", {}).keys())
    fieldnames.insert(0, 'id')  # Add 'id' as the first column

     # Check if items are available
    if items:
       
        all_fieldnames = set()

        # Iterate through items and gather all field names
        for item in items:
            fields = item.get("fields", {})
            for field in fields.keys():
                all_fieldnames.add(field.replace("_x0020_", "_"))
        print(all_fieldnames)

        # Open CSV file and write data
    with open(filename, "w", newline='', encoding="utf-8") as csvfile:
       
        fields_to_remove = ['FolderChildCount', 'Edit', '_ComplianceFlags']
        all_fieldnames = list(items[0].get("fields", {}).keys())  # Get fieldnames from the first item
        all_fieldnames = [field.replace("_x0020_", "_") for field in all_fieldnames]  # Clean field names
        all_fieldnames = [field for field in all_fieldnames if field not in fields_to_remove]
        all_fieldnames.insert(0, 'id')

        # Create CSV DictWriter object
        writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
        writer.writeheader()

        for item in items:
            fields = item.get("fields", {})
            fields = {key.replace("_x0020_", "_"): value for key, value in fields.items()}

            # Remove unwanted fields
            for field in fields_to_remove:
                if field in fields:
                    del fields[field] 
            
            fields['id'] = item['id']
            writer.writerow(fields)

    print(f"✅ Data has been saved to '{filename}'")




# Main workflow
def main():
    # Step 1: Authenticate and get access token
    access_token = authenticate()

    # Step 2: Get SharePoint Site ID
    site_id = get_site_id(access_token)

    # Step 3: Get List ID
    lst_id = get_list_id(site_id, access_token)

    # Step 4: Fetch all items from the SharePoint list
    items = fetch_all_items(site_id, lst_id, access_token)

    # Step 5: Save the items to CSV
    save_to_csv(items, local_file_name)



if __name__ == "__main__":
    main()
    print("-----------completed------------")
