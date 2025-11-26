# Databricks One Calgary Guided Tour - November 27 2025

In this workshop, we will use the [Current Year Property Assessments](https://data.calgary.ca/Government/Current-Year-Property-Assessments-Parcel-/4bsw-nn7w/about_data) to create AI/BI Dashboards and Genie spaces in Databricks. This data contains the assessed values of residential, non-residential and farm land properties in Calgary. The properties in this dataset consist of Calgary lands that have a registered parcel at Alberta’s Land Titles Office. 

### Getting Started
1. [Register for your Databricks Express Workspace](https://login.databricks.com/signup?tuuid=cf10084b-95a3-4ef7-aaa2-08052ee71cec&intent=SIGN_UP&dbx_source=direct&sisu_state=eyJsZWdhbFRleHRTZWVuIjp7Ii9zaWdudXAiOnsicHJpdmFjeSI6dHJ1ZSwiY29ycG9yYXRlRW1haWxTaGFyaW5nIjp0cnVlfX19). Ensure you pick a US region (e.g., US East, US West)
2. Open your Databricks workspace
3. From the Workspace tab, select Create and choose Git folder

   
   <img width="1728" height="192" alt="git-folder" src="https://github.com/user-attachments/assets/db7cfc80-5bbf-456d-bc64-900320d441c4" />

   
4. Enter `https://github.com/harrison-chen87/databricks-one-workshop.git` in the Git repository URL box and click Create Git Folder
5. You should now have a directory called `databricks-one-workshop`
6. Navigate into the `setup` (`dbx-one-data-gen` then to `setup`) directory and open the `001-setup` notebook and select `Run all`, the play button in the top right - DO NOT MODIFY THIS NOTEBOOK

   <img width="1141" height="317" alt="run-all" src="https://github.com/user-attachments/assets/bfee9cb8-2f86-4e94-9393-a159cfb54984" />


7. When the notebook is finished running, navigate to the Catalog tab and expand the `calgary_real_estate` catalog and `property_assessments` schema. You should see the newly created table `2025_data`.
   
  <img width="633" height="425" alt="catalog" src="https://github.com/user-attachments/assets/85c2fab7-8990-4905-adb5-2da0033b78b4" />
   


### Data Terms of Use

**Terms of use**

**Non-commercial reproduction**

Unless indicated otherwise, the data contained in the repository may be copied and distributed for non-commercial use without charge or further permission from the City of Calgary. We ask that: the data will not be modified, users exercise due diligence in ensuring the accuracy of the data, the City of Calgary be identified as the source of the data, and the reproduction is not represented as an official version of the data reproduced, nor as having been made in affiliation with or with the endorsement of the City of Calgary.

**Commercial reproduction**

The data contained in the repository may not be copied and/or distributed for commercial purposes without prior written permission from the City of Calgary (Calgary Open Data Office).
