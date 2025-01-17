import sys, os
from os.path import dirname, realpath
import pandas as pd
import hashlib
import shutil
import psycopg2
from configparser import ConfigParser
import logging
root_path = f'{dirname(dirname(dirname(dirname(realpath(__file__)))))}'
print(root_path)
print(os.getcwd())
os.path.join(root_path, "src/apis/pa_gateway/connection")
from utils.connection_pool import get_results_from_query, insert_data_to_table, upsert_data_to_table
from pag_preprocessing import *
configur = ConfigParser()
configur.read(os.path.join(root_path, "config/config_cme.ini"))
print(configur.sections())
logging.basicConfig(filename= os.path.join(root_path, "src/pipeline/cme/cme_pipeline.log"),
                    format='%(asctime)s | %(funcName)s | %(message)s',
                    filemode='w')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("cme pipeline starts")
print("cme pipeline starts")

username = 'psql'
folder_path_pag = root_path+configur.get('cme','folder_path_pag')
print(folder_path_pag)
source_name = root_path+configur.get('cme','source_name')
therapeutic_area = root_path+configur.get('cme','therapeutic_area')

df_raw = read_raw_data(folder_path_pag)
df_renamed = rename_columns(df_raw)
df_renamed = create_source_id(df_renamed, source_name)

############################## PAG TABLE #####################################################################
df_cme = cme_table(df_renamed, therapeutic_area)
table_name = 'public.continuing_medical_education'
colnames = list(df_cme.columns())
data = [tuple(x) for x in df_cme.to_numpy()]
msg = insert_data_to_table(table_name, colnames, data, username)
if msg=="success":
    logger.info("cme table is saved successfully")
    print("cme table is saved successfully")
else:
    logger.error("error in saving cme table")
    print("error in saving cme table")

############################## HCP PROFILE TABLE #####################################################################
df_hcp_profile = hcp_profile_table(df_renamed)
table_name = 'public.hcp_profile'
colnames = list(df_hcp_profile.columns())
data = [tuple(x) for x in df_hcp_profile.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("profile table is saved successfully")
    print("profile table is saved successfully")
else:
    logger.error("error in saving profile table")
    print("error in saving profile table")

############################## HCP MEMBERSHIP AND COMMITTEE TABLE #####################################################################
df_membership = hcp_membership_and_committee_table(df_renamed, df_cme, df_hcp_profile)
table_name = 'public.hcp_membership_and_committee'
colnames = list(df_membership.columns())
data = [tuple(x) for x in df_membership.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("membership_and_committee table is saved successfully")
    print("membership_and_committee table is saved successfully")
else:
    logger.error("error in saving membership_and_committee table")
    print("error in saving membership_and_committee table")

############################## AFFILIATION MASTER TABLE #####################################################################
df_aff_master = affiliation_master_table(df_renamed)
table_name = 'public.affiliation_master'
colnames = list(df_membership.columns())
data = [tuple(x) for x in df_membership.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("membership_and_committee table is saved successfully")
    print("membership_and_committee table is saved successfully")
else:
    logger.error("error in saving membership_and_committee table")
    print("error in saving membership_and_committee table")

############################## HCP AFFILIATION TABLE #####################################################################
df_hcp_affiliation = hcp_affiliation_table(df_renamed, df_aff_master, df_hcp_profile)
table_name = 'public.hcp_affiliation'
colnames = list(df_hcp_affiliation.columns())
data = [tuple(x) for x in df_hcp_affiliation.to_numpy()]
conflict_cols = ['affiliation_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("hcp_affiliation table is saved successfully")
    print("hcp_affiliation table is saved successfully")
else:
    logger.error("error in saving hcp_affiliation table")
    print("error in saving hcp_affiliation table")

############################## HCP SPECIALIZATION TABLE #####################################################################
df_hcp_spec = hcp_specialization_table(df_renamed, df_hcp_profile)
table_name = 'public.hcp_specialization'
colnames = list(df_hcp_spec.columns())
data = [tuple(x) for x in df_hcp_spec.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("hcp_specialization table is saved successfully")
    print("hcp_specialization table is saved successfully")
else:
    logger.error("error in saving hcp_specialization table")
    print("error in saving hcp_specialization table")

############################## HCP CONTACT TABLE #####################################################################
df_hcp_contact = hcp_contact_table(df_renamed, df_hcp_profile)
table_name = 'public.hcp_contact'
colnames = list(df_hcp_contact.columns())
data = [tuple(x) for x in df_hcp_contact.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("hcp_contact table is saved successfully")
    print("hcp_contact table is saved successfully")
else:
    logger.error("error in saving hcp_contact table")
    print("error in saving hcp_contact table")

############################## HCP CREDENTIALS TABLE #####################################################################
df_hcp_cred = hcp_credentials_table(df_renamed, df_hcp_profile)
table_name = 'public.hcp_credential'
colnames = list(df_hcp_cred.columns())
data = [tuple(x) for x in df_hcp_cred.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("hcp_credential table is saved successfully")
    print("hcp_credential table is saved successfully")
else:
    logger.error("error in saving hcp_credential table")
    print("error in saving hcp_credential table")

############################## HCP ADDITIONAL FEATURES TABLE #####################################################################
df_hcp_add_features = hcp_add_features_table(df_renamed, df_hcp_profile)
table_name = 'public.hcp_add_features'
colnames = list(df_hcp_add_features.columns())
data = [tuple(x) for x in df_hcp_add_features.to_numpy()]
conflict_cols = ['transaction_id']
msg = upsert_data_to_table(table_name, colnames, data, username, conflict_cols=conflict_cols)
if msg=="success":
    logger.info("hcp_add_features table is saved successfully")
    print("hcp_add_features table is saved successfully")
else:
    logger.error("error in saving hcp_add_features table")
    print("error in saving hcp_add_features table")

logger.info("cme pipeline ends")
print("cme pipeline ends")