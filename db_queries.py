def get_survey_sample_pipeline(election_cycle: str = None, election_round=None):
	'''
	:param election_cycle
			election_round:
	:return:
	'''
	pipeline = [
		{
			"$match": {
				"election_round": str(election_round),
				"election_cycle": election_cycle,
				# 'v1_rejection': {"$nin": [False]},
				# "vn": {"$exists": True},
				# "remark": {"$nin": ['rejected']},
				"last_normalised": True
			}
		},
		{
			"$project": {
				'raw': 1,
				'future_vote_pref': 1,
				'future_vote_pref_ge': 1,
				'ac_no': 1,
				'vn': 1,
				'vn_ge': 1,
				'vn_average': 1,
				'caste': 1,
				'caste_category': 1,
				'pm_pref': 1,
				'cand_pref_ques_ge': 1,
				'mp_pref': 1,
				'gender': 1,
				'age': 1,
				'_id': 1,
			}
		}
	]
	return pipeline


def get_ac_pc_mapping_query(state_code):
	query = rf'''
	SELECT cr_cons_id ac_no, cons_name, pc_no, pc_name, dt_name, z_name from "ConstituencyRegions" cr
	INNER JOIN "Constituency" C on cr.cr_id = C.cons_cr_id
	INNER JOIN "District" D on cr.cr_district_id = D.dt_id
	INNER JOIN "Zone" Z on cr.cr_zone_id = Z.z_id
	INNER JOIN "PC" P on cr.cr_pc_id = P.pc_id
	where z_state = '{state_code}' order by ac_no;
	'''
	cols = ['ac_no', 'ac_name', 'pc_no', 'pc_name', 'district', 'zone']
	return query, cols


def get_retro_data_query(state_code):
	query = rf'''
	SELECT
	    cr_cons_id, e_type, el_year, ca_full_name, org_abb, el_vote_count, el_vote_perc, el_rank
	from
	    retro_data
	where
	    org_state = '{state_code}' and
	    el_year in (2019, 2014);
	'''
	cols = ['ac_no', 'e_type', 'el_year', 'candidate', 'party', 'votes', 'vote_share', 'rank']
	return query, cols


def get_caste_details_query(state_code):
	query = rf'''
	SELECT cr_cons_id ac_no, cons_name, lower(caste), upper(category), ceil(caste_perc * cons_total_voters_count) caste_voters
	from "Caste_details2.0"
	         INNER JOIN public."Constituency" C on "Caste_details2.0".cd_cons_id = C.cons_id
	         INNER JOIN "ConstituencyRegions" cr on cr.cr_id = C.cons_cr_id
	where cr_state = '{state_code}'
	order by cons_id;
	'''
	cols = ['ac_no', 'ac_name', 'caste', 'caste_category', 'caste_voters']
	return query, cols
