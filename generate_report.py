from typing import Dict, List

from edc_src.connections import EDC

from fetch_data import PostgreSQLFetcher, MongoDBFetcher
from utils.table_functions import *


class PCReport:
	def __init__(self, state_code: str, dump_folder: str, template_file_id: str,
				 ac_acceptance_sample_threshold: int, calculation_score: str,
				 alliance: Dict[str, List[str]], election_round: str, election_cycle: str, caste_category_needed: str,
				 ac_wise_top_candidate_needed: int, pc_reports: list):
		"""
		:param state_code: The state code (e.g., "KL").
		:param dump_folder: The Google Drive folder path.
		:param template_file_id: The template document ID.
		:param ac_acceptance_sample_threshold: The minimum number of samples needed to consider one AC for the report.
		:param calculation_score: The score used to generate the report (e.g., "vn", "vn_ge", "raw").
		:param alliance: A dictionary with keys as alliance names and values as lists of parties in that alliance.
		:param election_round: Election round of survey
		:param election_cycle: Election cycle of survey
		:param caste_category_needed: List of caste categories that is needed to show in report for their respective voting preference
		:param ac_wise_top_candidate_needed: Top x candidate based on survey, e.g. top 4 in each acs needed
		:param pc_reports: list of pcs whose report is to be generated
		"""
		self.state_code = state_code
		self.dump_folder = dump_folder
		self.template_file_id = template_file_id
		self.ac_acceptance_sample_threshold = ac_acceptance_sample_threshold
		self.calculation_score = calculation_score
		self.alliance = alliance
		self.caste_category_needed = caste_category_needed
		self.ac_wise_top_candidate_needed = ac_wise_top_candidate_needed
		self.main_parties, self.all_stakeholders = self.get_main_parties()
		self.psql_tables = PostgreSQLFetcher(state_code=self.state_code)
		self.mongo_tables = MongoDBFetcher(database_name='survey_central', collection_name=f'{state_code}_raw_response',
										   election_round=election_round, election_cycle=election_cycle)
		self.ac_dict = dict(self.psql_tables.ac_pc_mapping[['ac_no', 'ac_name']].values)
		self.pre_process_data()
		if not pc_reports:
			self.pc_reports = self.mongo_tables.survey_samples['pc_name'].unique().tolist()
		else:
			self.pc_reports = pc_reports
		self.pc_wise_result = self.get_pc_wise_result()
		
		self.acs_in_pc = self.psql_tables.ac_pc_mapping['pc_name'].value_counts().max()
		self.gdrive_obj = EDC().client(api="gdrive", auth='./api_keys/drive/input.json')
		self.gdoc_obj = EDC().client(api="gdocs", auth={"credentials": './api_keys/client_secret.json'})
		
		self.generate_report()
	
	def generate_report(self):
		for pc_name in self.pc_reports:
			replacement_dictionary = self.get_replacement_dict(pc_name=pc_name)
			resp = self.gdrive_obj.copy_files(targetfolder=self.dump_folder,
											  file_ID=self.template_file_id,
											  new_title=f'{pc_name} Survey Report')
			self.gdoc_obj.replace_text(doc_ID=resp['id'], replace_dictionary=replacement_dictionary)
			print(f'Generated for {pc_name} at {resp["alternateLink"]}')
	
	def get_main_parties(self):
		main_parties = []
		all_stakeholders = []
		for alliance, parties in self.alliance.items():
			main_parties.extend(parties)
			all_stakeholders.append(alliance)
		all_stakeholders.append('Others')
		main_parties.append('Others')
		return main_parties, all_stakeholders
	
	def get_pc_wise_result(self):
		pc_wise_result = self.psql_tables.retro_data[self.psql_tables.retro_data['e_type'] == 'GE']
		pc_wise_result = pc_wise_result.merge(self.psql_tables.ac_pc_mapping, on='ac_no', how='left')
		pc_wise_result = pc_wise_result.groupby(by=['el_year', 'pc_name', 'candidate', 'party'], as_index=False).agg(
			{'votes': 'sum'})
		pc_wise_result = pc_wise_result.sort_values(by=['el_year', 'pc_name', 'votes'], ascending=[True, True, False])
		pc_wise_result['rank'] = pc_wise_result.groupby(by=['el_year', 'pc_name']).cumcount() + 1
		pc_wise_result['Total Votes'] = pc_wise_result.groupby(by=['el_year', 'pc_name'])['votes'].transform('sum')
		pc_wise_result['Vote Share'] = ((pc_wise_result['votes'] / pc_wise_result['Total Votes']) * 100).round(2)
		return pc_wise_result
	
	def pre_process_data(self):
		survey_samples = self.mongo_tables.survey_samples
		actual_caste_data = self.psql_tables.actual_caste_data
		
		ac_wise_samples = survey_samples.groupby('ac_no').agg({'raw': 'sum'})
		qualified_acs = ac_wise_samples[ac_wise_samples['raw'] >= self.ac_acceptance_sample_threshold].index.tolist()
		
		self.psql_tables.retro_data = self.psql_tables.retro_data[
			self.psql_tables.retro_data['ac_no'].isin(qualified_acs)]
		actual_caste_data = actual_caste_data[actual_caste_data['ac_no'].isin(qualified_acs)]
		actual_caste_data = actual_caste_data.merge(self.psql_tables.ac_pc_mapping[['ac_no', 'pc_name']], on='ac_no',
													how='left')
		
		survey_samples = survey_samples[survey_samples['ac_no'].isin(qualified_acs)]
		survey_samples = survey_samples.merge(self.psql_tables.ac_pc_mapping[['ac_no', 'pc_name']], on='ac_no',
											  how='left')
		survey_samples.to_excel(f'./dump_data/{self.state_code}_survey_samples.xlsx', index=False)
		survey_samples['future_vote_pref_ge'] = survey_samples['future_vote_pref_ge'].apply(
			lambda x: x if x in self.main_parties else 'Others')
		survey_samples['cand_pref_ques_ge'] = survey_samples['cand_pref_ques_ge'].fillna('')
		survey_samples['cand_pref_ques_ge'] = survey_samples['cand_pref_ques_ge'].apply(
			lambda x: "" if str(x).isnumeric() else x)
		survey_samples['cand_pref_ques_ge'] = survey_samples['cand_pref_ques_ge'].apply(
			lambda x: None if x == '' else x)
		survey_samples['pm_pref'] = survey_samples['pm_pref'].replace('Yogi Adityanath', 'Others')
		
		self.mongo_tables.survey_samples = survey_samples
		self.psql_tables.actual_caste_data = actual_caste_data
	
	def get_replacement_dict(self, pc_name):
		replace_dictionary = dict()
		pc_df = self.mongo_tables.survey_samples[self.mongo_tables.survey_samples['pc_name'] == pc_name]
		pc_caste_data = self.psql_tables.actual_caste_data
		pc_caste_data = pc_caste_data[pc_caste_data['pc_name'] == pc_name]
		party_col = 'future_vote_pref_ge'
		replace_dictionary['{{pc_name}}'] = pc_name
		replace_dictionary['{{samples_collected}}'] = pc_df['raw'].sum()
		replace_dictionary['{{estimated_winner}}'] = pc_df['future_vote_pref_ge'].value_counts().head(1).index.tolist()[
			0]
		
		replace_dictionary.update(get_retro_dict(pc_name=pc_name, pc_wise_result=self.pc_wise_result, year=2019))
		replace_dictionary.update(
			get_estimated_allaince_shares(df=pc_df, on=party_col, which_criteria=self.calculation_score,
										  alliance=self.alliance))
		replace_dictionary.update(
			get_estimated_party_shares(df=pc_df, on=party_col, which_criteria=self.calculation_score,
									   main_parties=self.main_parties))
		replace_dictionary.update(
			get_mp_prefrence(on='cand_pref_ques_ge', df=pc_df, which_criteria='vn', rows_needed=8))
		replace_dictionary.update(
			get_gender_wise(df=pc_df, alliance=self.alliance, all_stakeholders=self.all_stakeholders,
							party_col=party_col))
		
		replace_dictionary.update(get_category_wise(df=pc_df, party_col=party_col, alliance=self.alliance,
													all_stakeholders=self.all_stakeholders,
													categories_needed=self.caste_category_needed,
													score=self.calculation_score))
		replace_dictionary.update(
			get_top_caste_wise(df=pc_df, party_col=party_col, actual_caste_df=pc_caste_data, alliance=self.alliance,
							   all_stakeholders=self.all_stakeholders))
		replace_dictionary.update(
			get_age_wise(df=pc_df, alliance=self.alliance, party_col=party_col, score=self.calculation_score,
						 all_stakeholders=self.all_stakeholders))
		replace_dictionary.update(get_pm_pref(df=pc_df, pm_pref_col='pm_pref', score=self.calculation_score))
		replace_dictionary.update(
			get_ac_wise_winners(df=pc_df, alliance=self.alliance, party_col=party_col, score=self.calculation_score,
								ac_dict=self.ac_dict, rank_upto=self.ac_wise_top_candidate_needed,
								acs_in_pc=self.acs_in_pc))
		replace_dictionary.update(get_top_caste_pc_wise(pc_name=pc_name, caste_df=pc_caste_data))
		replace_dictionary.update(
			get_top_caste_prefs(df=pc_df, party_col=party_col, alliance=self.alliance, score=self.calculation_score))
		
		return replace_dictionary
