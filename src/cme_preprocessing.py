import pandas as pd
import numpy as np
import glob
import re
import psycopg2
import hashlib
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("cme pipeline starts")
print("cme pipeline starts")

def generate_transaction_id(*args):
    """
    returns unique hash of n number of strings
    """
    m = hashlib.sha256()
    i = "".join(args)
    z=str.encode(i)
    m.update(z)
    id_ = m.hexdigest()
    return id_

def clean_name(name):
    if name is not None:
        name = name.strip().lower()
        name = name.replace('.', '')
        name = re.sub(r'[^A-Za-z0-9\s]', '', name)
        return name
    else:
        return None

def clean_spec(text):
    if pd.notna(text):
        text_clean = text.lower().strip()
        text_clean = text_clean.split(',')
        text_clean = [x.strip() for x in text_clean]
        return text_clean
    else:
        return None
        
def read_raw_data(folder_path):
    df_raw = pd.DataFrame()
    for file in glob.glob(folderpath+"/*"):
        # Load data from EXCEL file
        df_ = pd.read_excel(file, engine='openpyxl')
    df_raw = pd.concat([df_raw, df_]).reset_index(drop=True)
    print(f"raw_data_shape: {df_raw.shape}")
    logger.info(f"raw_data_shape: {df_raw.shape}")
    print(f"raw_data_columns: {df_raw.columns}")
    logger.info(f"raw_data_columns: {df_raw.columns}")
    return df_raw

def rename_columns(df_raw):
    df_renamed = df_raw.rename(columns={'HCP Full Name': 'hcp_full_name', 'HCP Dedup': 'dedup_id', 'Credential': 'credential', 'Specialty': 'specialization', 'Institution': 'affiliation', 'City': 'city', 'State': 'state', 'Country': 'country', 'Role': 'designation', 'Topic': 'topic', 'CME name': 'cme_name', 'Year': 'year', 'Sponsor': 'sponsor', 'Disclosures': 'cme_disclosure', 'Source Link': 'cme_url', 'email': 'contact', 'Bio': 'overview', 'NPI': 'npi', 'Source for HCP affliation or bio': 'source_url'})
    df_renamed = df_renamed.drop(columns=['Completed by', 'Notes', 'Prof or Assoc Prof etc', 'Source Link for HCP Affiliation and Bio'])
    df_renamed = df_renamed.drop_duplicates().reset_index(drop=True)
    df_renamed = df_renamed.dropna(how='all')
    return df_renamed

def create_source_id(df_renamed, source_name):
    df_renamed['hcp_first_name'] = df_renamed['hcp_full_name'].apply(lambda x: x.split(' ')[0].lower())
    df_renamed['hcp_middle_name'] = df_renamed['hcp_full_name'].apply(lambda x: x.split(' ')[1].lower() if len(x.split(' '))>2 else None)
    df_renamed['hcp_last_name'] = df_renamed['hcp_full_name'].apply(lambda x: x.split(' ')[-1].lower())
    df_renamed['hcp_first_name'] = df_renamed['hcp_first_name'].apply(lambda x: clean_name(x))
    df_renamed['hcp_middle_name'] = df_renamed['hcp_middle_name'].apply(lambda x: clean_name(x))
    df_renamed['hcp_last_name'] = df_renamed['hcp_last_name'].apply(lambda x: clean_name(x))
    df_renamed['source'] = source_name
    df_renamed['source_id'] = df_renamed.apply(lambda x: generate_transaction_id(str(x['hcp_first_name']), 
                                                                                 str(x['hcp_middle_name']),
                                                                                str(x['hcp_last_name']),
                                                                                 str(x['dedup_id']).lower()), axis=1)
    print(f"df_shape & unique source_ids: {df_renamed.shape, df_renamed['source_id'].nunique()}")
    logger.info(f"df_shape & unique source_ids: {df_renamed.shape, df_renamed['source_id'].nunique()}")
    return df_renamed

def cme_table(df_renamed, therapeutic_area):
    df_cme = df_renamed[['cme_name', 'cme_url', 'sponsor', 'topic', 'year']].drop_duplicates().reset_index(drop=True)
    df_cme = df_cme.dropna(how='all')
    df_cme['therapeutic_area'] = therapeutic_area
    df_cme['year'] = df_cme['year'].apply(lambda x: str(x) if pd.notna(x) else None)
    df_cme[['sponsor', 'topic']] = df_cme[['sponsor', 'topic']].applymap(lambda x: x.lower())
    df_cme['transaction_id'] = df_cme.apply(lambda x: generate_transaction_id(str(x['therapeutic_area']).lower(),
                                                                              str(x['sponsor']).lower(),
                                                                               str(x['cme_name']).lower(),
                                                                              str(x['topic']).lower(),
                                                                             str(x['year'])), axis=1)
    df_cme = df_cme.replace(np.nan, None)
    df_cme = df_cme.replace('', None)
    df_cme = df_cme.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"cme_table_shape & unique transaction_ids: {df_cme.shape, df_cme['transaction_id'].nunique()}")
    logger.info(f"cme_table_shape & unique transaction_ids: {df_cme.shape, df_cme['transaction_id'].nunique()}")
    return df_cme


def hcp_profile_table(df_renamed):
    df_profile = df_renamed[['source', 'source_id', 'hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url']].drop_duplicates().reset_index(drop=True)
    df_profile = df_profile.dropna(how='all')
    df_profile = df_profile.groupby(['source_id']).agg({'source': 'first', 'hcp_first_name': 'first', 'hcp_middle_name': 'first', 'hcp_last_name': 'first', 'source_url': 'first'}).reset_index()
    df_profile['transaction_id'] = df_profile.apply(lambda x: generate_transaction_id(str(x['source']).lower(), str(x['source_id'])), axis=1)
    df_profile = df_profile.replace(np.nan, None)
    df_profile = df_profile.replace('', None)
    df_profile = df_profile.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_profile_table_shape & unique transaction_ids: {df_profile.shape, df_profile['transaction_id'].nunique()}")
    logger.info(f"hcp_profile_table_shape & unique transaction_ids: {df_profile.shape, df_profile['transaction_id'].nunique()}")
    return df_profile

def hcp_membership_and_committee_table(df_renamed, df_cme, df_profile):
    df_member = df_renamed[['source', 'source_id', 'sponsor', 'cme_name', 'topic', 'designation', 'year']].drop_duplicates().reset_index(drop=True)
    df_member = df_member.dropna(subset=['designation']).reset_index(drop=True)
    df_member['year'] = df_member['year'].apply(lambda x: str(x) if pd.notna(x) else None)
    df_member['designation'] = df_member['designation'].apply(lambda x: x.lower() if pd.notna(x) else None)
    df_member[['sponsor', 'cme_name', 'topic']] = df_member[['sponsor', 'cme_name', 'topic']].applymap(lambda x: x.lower() if pd.notna(x) else None)
    df_member = df_member.merge(df_cme, on=['sponsor', 'cme_name', 'topic', 'year'], how='left')
    df_member = df_member.drop(columns=['sponsor', 'cme_name', 'topic', 'cme_url'])
    df_member = df_member.rename(columns={'transaction_id': 'member_of'})
    df_member = df_member.dropna(subset=['member_of'])
    df_member = df_member.merge(df_profile, on=['source', 'source_id'], how='left')
    df_member = df_member.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_member = df_member.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_member = df_member.drop_duplicates().reset_index(drop=True)
    df_member['transaction_id'] = df_member.apply(lambda x: generate_transaction_id(str(x['source']).lower(), str(x['source_id']), str(x['member_of']), str(x['designation']), str(x['year'])), axis=1)
    df_member = df_member.replace(np.nan, None)
    df_member = df_member.replace('', None)
    df_member = df_member.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_membership_table_shape & unique transaction_ids: {df_member.shape, df_member['transaction_id'].nunique()}")
    logger.info(f"hcp_membership_table_shape & unique transaction_ids: {df_member.shape, df_member['transaction_id'].nunique()}")
    return df_member
    
def affiliation_master_table(df_renamed):
    df_aff_mast = df_renamed[['source', 'affiliation', 'city', 'state', 'country']].drop_duplicates().reset_index(drop=True)
    df_aff_mast = df_aff_mast.dropna(subset=['affiliation']).reset_index(drop=True)
    df_aff_mast[['city', 'state', 'country']] = df_aff_mast[['city', 'state', 'country']].applymap(lambda x: x.lower().strip() if pd.notna(x) else None)
    df_aff_mast = df_aff_mast.rename(columns={'source':'reference'})
    df_aff_mast['affiliation_id'] = df_aff_mast.apply(lambda x: generate_transaction_id(str(x['affiliation']).lower(), str(x['city']).lower(), str(x['state']).lower(), str(x['country']).lower()), axis=1)
    df_aff_mast = df_aff_mast.drop_duplicates(subset=['affiliation_id']).reset_index(drop=True)
    df_aff_mast = df_aff_mast.replace(np.nan, None)
    df_aff_mast = df_aff_mast.replace('', None)
    df_aff_mast = df_aff_mast.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"affiliation_master_table_shape & unique affiliation_ids: {df_aff_mast.shape, df_aff_mast['affiliation_id'].nunique()}")
    logger.info(f"affiliation_master_table_shape & unique affiliation_ids: {df_aff_mast.shape, df_aff_mast['affiliation_id'].nunique()}")
    return df_aff_mast

def hcp_affiliation_table(df_renamed, df_aff_mast, df_profile):
    df_hcp_aff = df_renamed[['source', 'source_id', 'affiliation', 'city', 'state', 'country']].drop_duplicates().reset_index(drop=True)
    df_hcp_aff = df_hcp_aff.merge(df_aff_mast, on=['affiliation', 'city', 'state', 'country'], how='left')
    df_hcp_aff = df_hcp_aff.drop(columns=['affiliation', 'city', 'state', 'country'])
    df_hcp_aff = df_hcp_aff.merge(df_profile, on=['source', 'source_id'], how='left')
    df_hcp_aff = df_hcp_aff.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_hcp_aff = df_hcp_aff.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_hcp_aff = df_hcp_aff.drop_duplicates().reset_index(drop=True)
    df_hcp_aff['transaction_id'] = df_hcp_aff.apply(lambda x: generate_transaction_id(str(x['source'].lower()), str(x['source_id']), str(x['affiliation_id'])), axis=1)
    df_hcp_aff = df_hcp_aff.replace(np.nan, None)
    df_hcp_aff = df_hcp_aff.replace('', None)
    df_hcp_aff = df_hcp_aff.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_affiliation_table_shape & unique transaction_ids: {df_hcp_aff.shape, df_hcp_aff['transaction_id'].nunique()}")
    logger.info(f"hcp_affiliation_table_shape & unique transaction_ids: {df_hcp_aff.shape, df_hcp_aff['transaction_id'].nunique()}")
    return df_hcp_aff

def hcp_specialization_table(df_renamed, df_profile):
    df_spec = df_renamed[['source', 'source_id', 'specialization']].drop_duplicates().reset_index(drop=True)
    df_spec = df_spec.dropna(subset=['specialization']).reset_index(drop=True)
    df_spec['specialization'] = df_spec['specialization'].apply(lambda x: clean_spec(x))
    df_spec = df_spec.dropna(how='all').reset_index(drop=True)
    df_spec = df_spec.merge(df_profile, on=['source', 'source_id'], how='left').reset_index(drop=True)
    df_spec = df_spec.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_spec = df_spec.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_spec = df_spec.explode(['specialization'])
    df_spec = df_spec.drop_duplicates().reset_index(drop=True)
    df_spec['specialization'] = df_spec['specialization'].apply(lambda x: x.replace('and', '') if (pd.notna(x) and (x.startswith("and ") or x.endswith(" and"))) else x)
    df_spec['specialization'] = df_spec['specialization'].apply(lambda x: x.strip() if pd.notna(x) else x)
    df_spec['specialization'] = df_spec['specialization'].replace('', None)
    df_spec = df_spec.dropna(subset=['specialization']).reset_index(drop=True)
    df_spec['transaction_id'] = df_spec.apply(lambda x: generate_transaction_id(str(x['source']), str(x['source_id']), str(x['specialization'])), axis=1)
    df_spec = df_spec.replace(np.nan, None)
    df_spec = df_spec.replace('', None)
    df_spec = df_spec.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_specialization_table_shape & unique transaction_ids: {df_spec.shape, df_spec['transaction_id'].nunique()}")
    logger.info(f"hcp_specialization_table_shape & unique transaction_ids: {df_spec.shape, df_spec['transaction_id'].nunique()}")
    return df_spec

def hcp_contact_table(df_renamed, df_profile):
    df_contact = df_renamed[['source', 'source_id', 'contact']].drop_duplicates().reset_index(drop=True)
    df_contact = df_contact.dropna(subset=['contact']).reset_index(drop=True)
    df_contact['contact'] = df_contact['contact'].apply(lambda x: x.strip() if pd.notna(x) else None)
    df_contact['contact'] = df_contact['contact'].replace('', None)
    df_contact = df_contact.merge(df_profile, on=['source', 'source_id'], how='left').reset_index(drop=True)
    df_contact = df_contact.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_contact = df_contact.drop_duplicates().reset_index(drop=True)
    df_contact = df_contact.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_contact['contact_type'] = 'email'
    df_contact['transaction_id'] = df_contact.apply(lambda x: generate_transaction_id(str(x['source']), str(x['source_id']),
 str(x['contact_type']), str(x['contact'])), axis=1)
    df_contact = df_contact.replace(np.nan, None)
    df_contact = df_contact.replace('', None)
    df_contact = df_contact.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_contact_table_shape & unique transaction_ids: {df_contact.shape, df_contact['transaction_id'].nunique()}")
    logger.info(f"hcp_contact_table_shape & unique transaction_ids: {df_contact.shape, df_contact['transaction_id'].nunique()}")
    return df_contact

def hcp_credentials_table(df_renamed, df_profile):
    df_cred = df_renamed[['source', 'source_id', 'credential']].drop_duplicates().reset_index(drop=True)
    df_cred = df_cred.dropna(subset=['credential']).reset_index(drop=True)
    df_cred['credential'] = df_cred['credential'].apply(lambda x: x.lower().strip() if pd.notna(x) else None)
    df_cred['credential'] = df_cred['credential'].replace('', None)
    df_cred = df_cred.merge(df_profile, on=['source', 'source_id'], how='left').reset_index(drop=True)
    df_cred = df_cred.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_cred = df_cred.drop_duplicates().reset_index(drop=True)
    df_cred = df_cred.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_cred['transaction_id'] = df_cred.apply(lambda x: generate_transaction_id(str(x['source']), str(x['source_id']), str(x['credential']).lower()), axis=1)
    df_cred = df_cred.replace(np.nan, None)
    df_cred = df_cred.replace('', None)
    df_cred = df_cred.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_credentials_table_shape & unique transaction_ids: {df_cred.shape, df_cred['transaction_id'].nunique()}")
    logger.info(f"hcp_credentials_table_shape & unique transaction_ids: {df_cred.shape, df_cred['transaction_id'].nunique()}")
    return df_cred

def hcp_add_features_table(df_renamed, df_profile):
    df_bio = df_renamed[['source', 'source_id', 'cme_disclosure', 'overview']].drop_duplicates().reset_index(drop=True)
    df_bio = df_bio.dropna(subset=['overview']).reset_index(drop=True)
    df_bio[['cme_disclosure', 'overview']] = df_bio[['cme_disclosure', 'overview']].applymap(lambda x: x.lower().strip() if pd.notna(x) else None)
    df_bio[['cme_disclosure', 'overview']] = df_bio[['cme_disclosure', 'overview']].replace('', None)
    df_bio = df_bio.merge(df_profile, on=['source', 'source_id'], how='left').reset_index(drop=True)
    df_bio = df_bio.drop(columns=['hcp_first_name', 'hcp_middle_name', 'hcp_last_name', 'source_url'])
    df_bio = df_bio.drop_duplicates().reset_index(drop=True)
    df_bio = df_bio.rename(columns={'transaction_id': 'hcp_profile_transaction_id'})
    df_bio['transaction_id'] = df_bio.apply(lambda x: generate_transaction_id(str(x['source']), str(x['source_id']), str(x['overview']).lower(), str(x['cme_disclosure']).lower()), axis=1)
    df_bio = df_bio.replace(np.nan, None)
    df_bio = df_bio.replace('', None)
    df_bio = df_bio.fillna(psycopg2.extensions.AsIs('NULL'))
    print(f"hcp_add_features_table_shape & unique transaction_ids: {df_bio.shape, df_bio['transaction_id'].nunique()}")
    logger.info(f"hcp_add_features_table_shape & unique transaction_ids: {df_bio.shape, df_bio['transaction_id'].nunique()}")
    return df_bio
