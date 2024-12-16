
import numpy as np
import pandas as pd
import re
import os
# Step 1: Calculate REMAINING_BUDGET
def calculate_remaining_budget(row):
    if pd.isna(row['APPROVED_AMOUNT']) or pd.isna(row['TOTAL_PAID']):
        return "Unknown"
    elif row['TOTAL_PAID'] < row['APPROVED_AMOUNT']:
        return "Yes"
    elif row['TOTAL_PAID'] == row['APPROVED_AMOUNT']:
        return "No"
    elif row['TOTAL_PAID'] > row['APPROVED_AMOUNT']:
        return "Error"
    
def normalize_str(a_string: str):
    return str(a_string).strip().title()
  
def parse_country(data_dir, df):
    # Step 1: Create COUNTRY column and select relevant columns
    df['COUNTRY'] = df['COUNTRIESOFSTUDY']
    df = df[["NAME", "COUNTRIESOFSTUDY", "COUNTRY", "UNITEDSTATES", "STATUS", "STUDYSOP"]]

    # Step 2: Separate entries by delimiter "|"
    df = df.assign(COUNTRY=df['COUNTRY'].str.split('|')).explode('COUNTRY')
    df = df[df['COUNTRY'] != ""].drop_duplicates()
    df = df.dropna(subset=['COUNTRY'])
    df['COUNTRY'] = df['COUNTRY'].str.upper()

    # Define the patterns and their replacements
    replacements = {
      r"VENEZUELA, BOLIVARIAN REPUBLIC OF": "VENEZUELA (BOLIVARIAN REPUBLIC OF)",
      r"KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF": "KOREA THE DEMOCRATIC PEOPLE'S REPUBLIC OF",
      r"KOREA, REPUBLIC OF": "KOREA",
      r"VIRGIN ISLANDS, U.S.": "VIRGIN ISLANDS (U.S.)",
      r"IRAN, ISLAMIC REPUBLIC OF": "IRAN",
      r"PALESTINIAN TERRITORY, OCCUPIED": "ISRAEL",
      r"TANZANIA, UNITED REPUBLIC OF": "TANZANIA",
      r"ZAIRE": "CONGO DEMOCRATIC",
      r"CONGO, THE DEMOCRATIC REPUBLIC OF THE": "CONGO DEMOCRATIC",
      r"THE FORMER YUGOSLAV REPUBLIC OF": "REPUBLIC OF NORTH MACEDONIA",
      r"MOLDOVA, REPUBLIC OF": "Moldova (THE REPUBLIC OF)"
      }
    
    def replace_country_names(column, replacements):
      for pattern, replacement in replacements.items():
        column = column.str.replace(pattern, replacement, regex=True)
      return column
    
    df['COUNTRY'] = replace_country_names(df['COUNTRY'], replacements)

    # Step 4: Further split by delimiter "," and clean
    df = df.assign(COUNTRY=df['COUNTRY'].str.split(',')).explode('COUNTRY')
    df['COUNTRY'] = df['COUNTRY'].str.strip()
    df = df[df['COUNTRY'] != ""].drop_duplicates()
    df = df.dropna(subset=['COUNTRY'])
    df['COUNTRY'] = df['COUNTRY'].str.upper()

    # Step 5: Replace COUNTRY values based on specific mappings
    def final_country_mapping(country, united_states):
        mapping = {
            "VIETNAM": "VIET NAM",
            "SLOVAKIA (SLOVAK REPUBLIC)": "SLOVAKIA",
            "CROATIA (LOCAL NAME: HRVATSKA)": "CROATIA",
            "CZECH REPUBLIC": "CZECHIA",
            "USA": "UNITED STATES",
            "UK": "UNITED KINGDOM",
            "SOUTH KOREA": "KOREA",
            "CHINA": "Province Of China",
            "MACEDONIA": "Republic of North Macedonia",
            "CZECHOSLAVAKIA": "Slovakia",
            "SWAZILAND": "ESWATINI",
            "BOSNIA AND HERZEGOVINA": "BOSNIA",
            "NETHERLANDS ANTILLES": "SINT MAARTEN (DUTCH PART)",
            "GERMAN DEMOCRATIC REPUBLIC": "GERMANY",
            "USSR": "RUSSIAN FEDERATION",
        }
        if country in mapping:
            return mapping[country]
        if country in ["YUGOSLAVIA", "SERBIA AND MONTENEGRO"]:
            return "SERBIA"
        if country in ["UNKNOWN", "CÃ—TE D'IVOIRE", "NA; SINGLE COUNTRY", "EAST EUROPE"] and united_states == "Yes":
            return "UNITED STATES"
        return country

    df['COUNTRY'] = df.apply(lambda row: final_country_mapping(row['COUNTRY'], row['UNITEDSTATES']), axis=1)
    
    # Step 6: Remove duplicates and convert COUNTRY to uppercase
    df = df.drop_duplicates()
    df = df.dropna(subset=['COUNTRY'])
    df['COUNTRY'] = df['COUNTRY'].str.upper()
    
    # country_codes = pd.read_excel(os.path.join(data_dir, 'country_codes.xlsx'))
    country_codes = pd.read_excel("data/input/country_codes.xlsx")
    country_codes.rename(columns = {'Country': 'COUNTRY'}, inplace = True)
    country_codes['COUNTRY'] = country_codes['COUNTRY'].str.upper()
    country_codes['COUNTRY'] = country_codes['COUNTRY'].str.strip()
    merged_df = pd.merge(df, country_codes, on='COUNTRY', how='left')

    return merged_df
  

def get_dashboard_data(data_dir,
                       ec_file='EvidenceCatalog.csv',
                       ah_file='AssetHarmonization.csv',
                       ch_file='CategoryHarmonization.csv',
                       sh_file = 'StatusHarmonization.csv',
                       bp_file = 'Grants Budgets and Payments.xlsx',
                       subset_columns=True,
                       rename_columns=True,
                       apply_pre_filters=True):

    # Load Data.
    df = pd.read_csv(os.path.join(data_dir, ec_file), index_col=None)
    ah = pd.read_csv(os.path.join(data_dir, ah_file))
    ch = pd.read_csv(os.path.join(data_dir, ch_file))
    status_ah = pd.read_csv(os.path.join(data_dir, sh_file))
    grants_bp = pd.read_excel(os.path.join(data_dir,bp_file),sheet_name='Final')
    grants_bp['REMAINING_BUDGET'] = grants_bp.apply(calculate_remaining_budget, axis=1)
    #grants_bp.to_csv(os.path.join(data_dir,'GrantsBudgetsPayments.csv'))

    status_ah.status_native = status_ah.status_native.apply(lambda x: normalize_str(x))
    status_ah.harmonized_status = status_ah.harmonized_status.apply(lambda x: normalize_str(x))
    status_ah = status_ah[['status_native', 'harmonized_status', 'harmonized_status_detail']].drop_duplicates()

    harmonizeddrug = pd.merge(df, ah, on="PRIMARYDRUG", how="left")
    harmonizedcategory = pd.merge(harmonizeddrug, ch, on="CATEGORY", how="left")

    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['HARMONIZEDDRUGCATEGORY'].notna(), harmonizedcategory['HARMONIZEDDRUGCATEGORY'], harmonizedcategory['HARMONIZEDCATEGORY'])

    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSOP']=='GMG', 'GNT01', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSOP']=='CT24; CT34', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSOP']=='0', np.nan, harmonizedcategory['STUDYSOP'])
    harmonizedcategory['COUNTRIESOFSTUDY'] = harmonizedcategory['COUNTRIESOFSTUDY'].str.title()
    harmonizedcategory['COUNTRIESOFSTUDY'] = harmonizedcategory['COUNTRIESOFSTUDY'].str.replace('|',',')
    harmonizedcategory['COUNTRIESOFSTUDY'] = harmonizedcategory['COUNTRIESOFSTUDY'].str.replace('Taiwan, Province Of China','Taiwan')


    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase 1'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE 1'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase I'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE I'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase 2'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE 2'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase II'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE II'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase 3'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE 3'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Phase III'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('PHASE III'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('randomized'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Randomized'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('RANDOMIZED'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('randomised'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Randomised'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('RANDOMISED'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('double blind'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('Double Blind'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['TITLE'].notna() & harmonizedcategory['TITLE'].str.contains('DOUBLE BLIND'), 'CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYTYPE']=='INTERVENTIONAL','CT02', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSOP'].isna(), 'CT24', harmonizedcategory['STUDYSOP'])

    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT24, INVOLVES INVESTIGATORS/SITES AND ONLY USES SURVEYS, QUESTIONNAIRES, OR INTERVIEWS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT24, NO INVESTIGATORS/SITES AND ONLY USES SURVEYS, QUESTIONNAIRES, OR INTERVIEWS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT24, INVOLVES INVESTIGATORS/SITES AND IS NOT LIMITED TO SURVEYS, QUESTIONNAIRES OR INTERVIEWS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT24, NO INVESTIGATORS/SITES AND IS NOT LIMITED TO SURVEYS, QUESTIONNAIRES, OR INTERVIEWS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT24, YES INVESTIGATORS/SITES AND ONLY USES SURVEYS, QUESTIONNAIRES, OR INTERVIEWS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['PRIMARYDATACOLLECTION']=='YES - CT45, INVESTIGATORS/SITES', 'CT45', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['SECONDARYDATACOLLECTION']=='YES - CT24, STRUCTURED DATA ANALYSIS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['SECONDARYDATACOLLECTION']=='NO - CT24, PRIMARY DATA COLLECTION STUDY', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['SECONDARYDATACOLLECTION']=='YES - CT24, HUMAN REVIEW OF UNSTRUCTURED DATA- WITH SITES/INVESTIGATORS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['SECONDARYDATACOLLECTION']=='YES - CT24, HUMAN REVIEW OF UNSTRUCTURED DATA- WITHOUT SITES/INVESTIGATORS', 'CT24', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='LOW INTERVENTIONAL STUDY 1', 'CT45', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='LOW INTERVENTIONAL STUDY 2', 'CT45', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='Non-Interventional/Low-Interventional Study Type 1', 'CT45', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='PRAGMATIC CLINICAL TRIAL 2', 'CT45', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYTYPE']=='Research Collaboration', 'RC01', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYTYPE']=='Investigator Sponsored Research', 'GNT01', harmonizedcategory['STUDYSOP'])
    harmonizedcategory['STUDYSOP'] = np.where(harmonizedcategory['STUDYTYPE']=='General Research', 'GNT01', harmonizedcategory['STUDYSOP'])

    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='LOW INTERVENTIONAL STUDY 1', 'Low Interventional Study 1', harmonizedcategory['STUDYSUBTYPE'])
    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='LOW INTERVENTIONAL STUDY 2', 'Low Interventional Study 2', harmonizedcategory['STUDYSUBTYPE'])
    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='Non-Interventional/Low-Interventional Study Type 1', 'Low Interventional Study 1', harmonizedcategory['STUDYSUBTYPE'])
    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYSUBTYPE']=='PRAGMATIC CLINICAL TRIAL 2', 'Low Interventional Study 2', harmonizedcategory['STUDYSUBTYPE'])
    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYTYPE']=='Investigator Sponsored Research', 'Investigator Sponsored Research', harmonizedcategory['STUDYSUBTYPE'])
    harmonizedcategory['STUDYSUBTYPE'] = np.where(harmonizedcategory['STUDYTYPE']=='General Research', 'General Research', harmonizedcategory['STUDYSUBTYPE'])


    harmonizedcategory['DRUGPRIORITY'] = np.where(harmonizedcategory['HARMONIZEDPRIMARYDRUG']=='Not Applicable', 'No Drug', harmonizedcategory['DRUGPRIORITY'])
    harmonizedcategory['DRUGPRIORITY'] = np.where(harmonizedcategory['HARMONIZEDPRIMARYDRUG'].isna(), 'No Drug', harmonizedcategory['DRUGPRIORITY'])

    # saving original implementation for posterity
    # harmonizedcategory['STATUSDETAIL'] = harmonizedcategory['STATUS']

    harmonizedcategory['PASS'] = harmonizedcategory['PASS'].str.title()
    harmonizedcategory['PMS'] = harmonizedcategory['PMS'].str.title()
    harmonizedcategory['PMS'] = np.where(harmonizedcategory['PMS']=="N", 'No', harmonizedcategory['PMS'])
    harmonizedcategory['PMS'] = np.where(harmonizedcategory['PMS']=="Y", 'Yes', harmonizedcategory['PMS'])

    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['INDICATION']=="ATTR-CM (Transthyretin Amyloid Cardiomyopathy)",'Rare Disease',harmonizedcategory['HARMONIZEDCATEGORY'])
    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['INDICATION']=="Acromegaly",'Rare Disease',harmonizedcategory['HARMONIZEDCATEGORY'])
    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['INDICATION']=="Hemophilia",'Rare Disease',harmonizedcategory['HARMONIZEDCATEGORY'])
    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['INDICATION']=="Duchenne Muscular Dystrophy",'Rare Disease',harmonizedcategory['HARMONIZEDCATEGORY'])
    harmonizedcategory['HARMONIZEDCATEGORY'] = np.where(harmonizedcategory['INDICATION']=="Sickle Cell Disease",'Rare Disease',harmonizedcategory['HARMONIZEDCATEGORY'])

    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('Alliance Partner (SMPA Inc.)','Alliance Partner')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('Center of Excellence','RWE')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('China Medical Affairs','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('China','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('Country Med/RWE','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('Denmark','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('Finland Medical Affairs','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('France medical affairs','Country Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('GMA','Medical Affairs')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('HEOR','GAV')
    harmonizedcategory['EXECUTIONGROUP'] = harmonizedcategory['EXECUTIONGROUP'].str.replace('PHI','GAV')


    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Country Medical",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="GAV (HV&E)",'GAV',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="GAV (HV&E) and Medical",'GAV, Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="GAV co leading with medical",'GAV, Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="GAV, HVE Primary Care (ELIQUIS)",'GAV, Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Global Medical",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Israel Medical Affairs",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Japan",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Japan (Xu, Linghua) from Outcome &Evidence group",'Country H&V',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Japan Medical",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Japan Medical Team",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Japan Medical Team",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Korea",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Korea/Local PV",'Korea PMS',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Legacy Medical",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Local Medical affairs and RWE",'Country Medical Affairs, Country RWE',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Local RWE",'Country RWE',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Local RWE/Columbia",'Country RWE',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Local RWE/Columbia",'Country RWE',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="MEDICAL AFFAIR",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Medical",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Medical Affairs of Breast Cancer (Owner) but is supported by RWE, Quality, Compliance, and Legal)",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Medical affairs",'Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="PMS Affairs, Development Japan",'Japan PMS',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Polish medical team",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="RWE France",'Country RWE',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="RWE, Korean Post Marketing Surveillance",'Korea PMS',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Set up in old structure so being run out of Emerging Market medical group",'Emerging Markets Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Transferred to Merck",'Alliance Partner',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="UK",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="UK Medical Affairs",'Country Medical Affairs',harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="Unconfirmed",np.nan,harmonizedcategory['EXECUTIONGROUP'])
    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP']=="V&E group, study completed",'GAV',harmonizedcategory['EXECUTIONGROUP'])

    harmonizedcategory['EXECUTIONGROUP'] = np.where(harmonizedcategory['EXECUTIONGROUP'].notna() & harmonizedcategory['EXECUTIONGROUP'].str.contains('ENGINE'), np.nan, harmonizedcategory['EXECUTIONGROUP'])

    # saving original implementation for posterity
    # harmonizedcategory['STATUS'] = harmonizedcategory['STATUS'].str.title()
    # harmonizedcategory['STATUS'] = np.where(harmonizedcategory['STATUS'].str.contains('Approved'), 'Approved', harmonizedcategory['STATUS'])
    # harmonizedcategory['STATUS'] = np.where(harmonizedcategory['STATUS'].str.contains('Pending'), 'Pending', harmonizedcategory['STATUS'])
    # harmonizedcategory['STATUS'] = np.where(harmonizedcategory['STATUSDETAIL']=="Approved, Closed", 'Closed', harmonizedcategory['STATUS'])
    # harmonizedcategory['STATUS'] = np.where(harmonizedcategory['STATUSDETAIL']=="Approved, Study Complete", 'Completed', harmonizedcategory['STATUS'])

    # harmonizedcategory['STATUSDETAIL'] = harmonizedcategory['STATUSDETAIL'].str.replace('Approved, ', '')
    # harmonizedcategory['STATUSDETAIL'] = harmonizedcategory['STATUSDETAIL'].str.replace('Pending, ', '')
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Approved", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Ongoing", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Concept", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Planned", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Completed", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Cancelled", np.nan, harmonizedcategory['STATUSDETAIL'])
    # harmonizedcategory['STATUSDETAIL'] = np.where(harmonizedcategory['STATUSDETAIL'] == "Study Complete", np.nan, harmonizedcategory['STATUSDETAIL'])

    harmonizedcategory['STATUS'] = harmonizedcategory['STATUS'].fillna('Unknown')
    harmonizedcategory['STATUS'] = harmonizedcategory['STATUS'].apply(lambda x: normalize_str(x))

    harmonizedcategory = pd.merge(harmonizedcategory, status_ah, left_on='STATUS', right_on='status_native', how='left')
    harmonizedcategory = harmonizedcategory.drop(['status_native', 'STATUS'], axis=1).rename(columns={'harmonized_status': 'STATUS',
                                                                                                      'harmonized_status_detail': 'STATUSDETAIL'})
    harmonizedcategory = pd.merge(harmonizedcategory, grants_bp, how='left', left_on='NAME', right_on='GRANT_ID')
    harmonizedcategory = harmonizedcategory.drop(['GRANT_ID'], axis=1)



    displayData = harmonizedcategory.copy()

    if apply_pre_filters:
        displayData = displayData[(displayData['STUDYSOP']!='CT02') 
                        & (displayData['EXECUTIONGROUP']!='SSR')
                        & (displayData['EXECUTIONGROUP']!='GME')
                        & (displayData['SPONSORINGDIVISION']!='RU')
                        & (displayData['SPONSORINGDIVISION']!='GMG')
                        & (displayData['SPONSORINGDIVISION']!='PRD')
                        & (displayData['SPONSORINGDIVISION']!='Corporate Affairs')
                        & (displayData['SPONSORINGDIVISION']!='WRD')
                        & (displayData['SPONSORINGDIVISION']!='CONSUMER HEALTHCARE')
                        & (displayData['SPONSORINGDIVISION']!='BRDU')
                        & (displayData['SPONSORINGDIVISION']!='WRD TECHNOLOGY')]

    if subset_columns:
        displayData = displayData[["NAME",
                                   "TITLE",
                                   "STUDYTYPE",
                                   "STUDYSOP",
                                   "STUDYSUBTYPE",
                                   "PASS",
                                   "PMS",
                                   "HARMONIZEDCATEGORY",
                                   "INDICATION",
                                   "HARMONIZEDPRIMARYDRUG",
                                   "DRUGPRIORITY",
                                   "STATUS",
                                   "STATUSDETAIL",
                                   "COUNTRIESOFSTUDY",
                                   "UNITEDSTATES",
                                   "INTERNATIONALPRIORITY",
                                   "ANCHORMARKET",
                                   "EXECUTIONGROUP",
                                   "SPONSORINGDIVISION",
                                   "APPROVED_AMOUNT",
                                   "TOTAL_PAID",
                                   "REMAINING_BUDGET"]]
        
    if rename_columns:
        displayData.rename(columns = {'NAME':'ID',
                                      'STUDYSOP':'SOP',
                                      'TITLE':'Title',
                                      'STUDYTYPE':'Study Type',
                                      'STUDYSUBTYPE':'Study Subtype',
                                      'PASS':'PASS',
                                      'PMS':'Post Marketing Surveillance',
                                      'HARMONIZEDCATEGORY':'Category',
                                      'INDICATION':'Indication',
                                      'HARMONIZEDPRIMARYDRUG':'Primary Drug',
                                      'DRUGPRIORITY':'Asset Priority',
                                      'STATUS':'Status',
                                      'STATUSDETAIL':'Status Detail',
                                      'COUNTRIESOFSTUDY':'Study Country(s)',
                                      'UNITEDSTATES':'Study Conducted in United States',
                                      'INTERNATIONALPRIORITY':'Study Conducted in International Priority Market',
                                      'ANCHORMARKET':'Study Conducted in Anchor Market',
                                      'EXECUTIONGROUP':'Group Operationalizing',
                                      'SPONSORINGDIVISION':'Sponsoring Division',
                                      'APPROVED_AMOUNT':'Total',
                                      'TOTAL_PAID':'Paid',
                                      'REMAINING_BUDGET':'Remaining Budget'},
                                      inplace = True)
        
    return displayData
  
  
  
if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.realpath(__file__))
    data_dir = os.path.join(base_dir, '..', 'data')
    input_dir = os.path.join(data_dir, 'input')
    output_dir = os.path.join(data_dir, 'output')
    nis = get_dashboard_data(data_dir=input_dir, rename_columns=False)
    countries = parse_country(data_dir=input_dir, nis)
    print(nis.shape)
    print(os.path.join(output_dir, 'nis.csv'))
    nis.to_csv(os.path.join(output_dir, 'nis.csv'), index=False)
    countries.to_csv(os.path.join(output_dir, 'study_countries.csv'), index=False)
