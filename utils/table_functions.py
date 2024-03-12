import pandas as pd

from utils.utils import get_vote_share, shift_to_last, add_allaince


def get_retro_dict(pc_name, pc_wise_result, year):
	retro_dict = {}
	pc_result = pc_wise_result[pc_wise_result['pc_name'] == pc_name]
	winner = pc_result[(pc_result['el_year'] == year) & (pc_result['rank'] == 1)][
		['party', 'candidate']].values.tolist()
	retro_dict['{{incumbant_mp}}'] = winner[0][0] + ' - ' + winner[0][1]
	
	pivot = pc_result.loc[(pc_result['pc_name'] == pc_name) & (pc_wise_result['rank'] < 5), :]
	pivot = pivot.assign(cand_res=pivot['party'] + '-\n' + pivot['candidate'] + '\n(' + pivot['Vote Share'].astype(
		str).values + '%)')
	
	pivot = pivot.pivot(index='el_year', columns='rank', values='cand_res')
	
	for ind in pivot.index:
		for rank in pivot.columns:
			retro_dict["{{el_" + f"{ind}_rank{rank}_candidate" + '}}'] = pivot.loc[ind, rank]
	return retro_dict


def get_estimated_allaince_shares(df, on='future_vote_pref_ge', which_criteria='vn', alliance=None):
	if not alliance:
		return
	data = get_vote_share(on=on, df=df, which_alliance=alliance, which_criteria=which_criteria)
	inds = shift_to_last(data.index.tolist(), 'other')
	data = data.loc[inds]
	alliance_shares = {}
	for i, ind in enumerate(data.index, 1):
		alliance_shares['{{' + f"allaince{i}" + "}}"] = ind
		if ind == 'INDI':
			alliance_shares['{{' + f"allaince{i}" + "}}"] = 'INDI Alliance'
		alliance_shares['{{' + f"allaince{i}_vn" + "}}"] = data.loc[ind, which_criteria]
	if len(data) < len(alliance):
		for j in range(len(data), len(alliance) + 1):
			alliance_shares['{{' + f"allaince{j}" + "}}"] = '-'
			alliance_shares['{{' + f"allaince{j}_vn" + "}}"] = '-'
	return alliance_shares


def get_estimated_party_shares(df, on='future_vote_pref_ge', which_criteria='vn', main_parties=None):
	data = get_vote_share(on=on, df=df, which_criteria=which_criteria)
	for party in main_parties:
		if party not in data.index.tolist():
			data.loc[party] = 0.0
	data = data.loc[main_parties]
	party_shares = {}
	for ind in data.index:
		party_shares['{{' + f"{str(ind).lower()}_vn" + "}}"] = data.loc[ind, which_criteria]
	return party_shares


def get_mp_prefrence(df, on='cand_pref_ques_ge', which_criteria='vn', rows_needed=8):
	data = get_vote_share(on=on, df=df, which_criteria=which_criteria)
	inds = shift_to_last(data.index.tolist(), 'other')
	data = data.loc[inds]
	if len(data) >= rows_needed:
		d1 = data.iloc[:rows_needed - 1]
		d1.loc['Others', which_criteria] = data.iloc[rows_needed - 1:][which_criteria].sum()
		data = d1.copy()
	mp_pref = {}
	for i, ind in enumerate(data.index, 1):
		mp_pref['{{' + f"mp{i}" + "}}"] = ind
		mp_pref['{{' + f"mp{i}_vn" + "}}"] = data.loc[ind, which_criteria].round(2)
	for i in range(len(data) + 1, rows_needed + 1):
		mp_pref['{{' + f"mp{i}" + "}}"] = '-'
		mp_pref['{{' + f"mp{i}_vn" + "}}"] = '-'
	
	return mp_pref


def get_gender_wise(df, alliance=None, all_stakeholders=None, party_col=None, gender_col='gender',
					score='vn'):
	allaince_updated = add_allaince(df, alliance=alliance, on=party_col)
	
	allaince_wise = allaince_updated.pivot_table(index=gender_col,
												 columns=party_col,
												 values=score,
												 aggfunc='sum',
												 fill_value=0)
	for all in all_stakeholders:
		if all not in allaince_wise.columns.tolist():
			allaince_wise[all] = 0
	allaince_wise = allaince_wise.div(allaince_wise.sum(axis=1), axis=0)[all_stakeholders] * 100
	allaince_wise = allaince_wise.loc[['Female', 'Male']]
	allaince_wise.loc['total'] = allaince_wise.mean(axis=0)
	allaince_wise = allaince_wise.round(2)
	
	gender_wise = {}
	for gender in allaince_wise.index:
		for allaince in allaince_wise.columns:
			gender_wise['{{' + f"{gender.lower()}_{allaince.lower()}_share" + "}}"] = allaince_wise.loc[
				gender, allaince]
	return gender_wise


def get_category_wise(df, party_col: str, alliance, categories_needed: list, category_col='caste_category', score=None,
					  all_stakeholders=None):
	allaince_updated = add_allaince(df, alliance=alliance, on=party_col)
	
	allaince_wise = allaince_updated.pivot_table(index=category_col,
												 columns=party_col,
												 values=score,
												 aggfunc='sum',
												 fill_value=0)
	for all in all_stakeholders:
		if all not in allaince_wise.columns.tolist():
			allaince_wise[all] = 0
	
	allaince_wise = allaince_wise.div(allaince_wise.sum(axis=1), axis=0)[all_stakeholders] * 100
	
	for cat in categories_needed:
		if cat not in allaince_wise.index.tolist():
			allaince_wise.loc[cat] = 0
	allaince_wise = allaince_wise.loc[categories_needed]
	
	allaince_wise.loc['total'] = allaince_wise.mean(axis=0)
	
	allaince_wise = allaince_wise.round(2)
	
	category_wise = {}
	for category in allaince_wise.index:
		for allaince in allaince_wise.columns:
			category_wise['{{' + f"category_{category.lower()}_{allaince.lower()}_share" + "}}"] = allaince_wise.loc[
				category, allaince]
	return category_wise


def get_top_caste(
		df, actual_caste_df, party_col, alliance, n=5, category='GEN', score='vn',
		all_stakeholders: list = None
):
	alliance_updated = add_allaince(df, alliance=alliance, on=party_col)
	total_voters = actual_caste_df['caste_voters'].sum()
	total_samples = df['raw'].sum()
	top_castes = (
		df[df['caste_category'] == category]
		.groupby('caste')
		.agg({'raw': 'sum'})
		.sort_values('raw', ascending=False)
		.div(total_samples)
		.head(n)
	)
	
	top_castes['actual'] = actual_caste_df[actual_caste_df['caste_category'] == category].groupby('caste').agg(
		{'caste_voters': 'sum'}).sort_values(by='caste_voters', ascending=False).div(total_voters)
	
	top_castes = top_castes * 100
	top_castes = top_castes[['actual', 'raw']]
	alliance_wise = alliance_updated.pivot_table(index='caste',
												 columns=party_col,
												 values=score,
												 aggfunc='sum',
												 fill_value=0)
	for all in all_stakeholders:
		if all not in alliance_wise.columns.tolist():
			alliance_wise[all] = 0
	alliance_wise = alliance_wise.div(alliance_wise.sum(axis=1), axis=0)[
						all_stakeholders] * 100
	top_alliance_wise = top_castes.copy()
	top_alliance_wise[alliance_wise.columns] = alliance_wise
	
	inds = shift_to_last(top_alliance_wise.index.tolist(), 'other')
	
	top_alliance_wise = top_alliance_wise.loc[inds].round(2)
	top_alliance_wise = top_alliance_wise.fillna(0)
	return top_alliance_wise


def get_top_caste_wise(df, party_col, actual_caste_df, alliance, all_stakeholders, n=5):
	caste_wise = {}
	
	categories = ['GEN', 'OBC', 'SC', 'ST']
	for category in categories:
		data = get_top_caste(df=df[df['caste_category'] == category], party_col=party_col, category=category,
							 actual_caste_df=actual_caste_df, alliance=alliance, all_stakeholders=all_stakeholders, n=n)
		for i, caste in enumerate(data.index, 1):
			for col in data.columns:
				caste_wise['{{' + f"{category.lower()}_caste{i}" + "}}"] = caste.title()
				caste_wise['{{' + f"{category.lower()}_caste{i}_{str(col).lower()}_share" + "}}"] = data.loc[caste, col]
		for j in range(len(data) + 1, n + 1):
			for col in data.columns:
				caste_wise['{{' + f"{category.lower()}_caste{j}" + "}}"] = '-'
				caste_wise['{{' + f"{category.lower()}_caste{j}_{str(col).lower()}_share" + "}}"] = '-'
	return caste_wise


def get_age_wise(df, party_col, alliance, score, all_stakeholders):
	allaince_updated = add_allaince(df, alliance=alliance, on=party_col)
	
	allaince_wise = allaince_updated.pivot_table(index='age',
												 columns=party_col,
												 values=score,
												 aggfunc='sum',
												 fill_value=0)
	for all in all_stakeholders:
		if all not in allaince_wise.columns.tolist():
			allaince_wise[all] = 0
	allaince_wise = allaince_wise.div(allaince_wise.sum(axis=1), axis=0)[
						all_stakeholders] * 100
	
	allaince_wise.loc['total'] = allaince_wise.mean(axis=0)
	allaince_wise = allaince_wise.round(2)
	
	age_wise = {}
	for i, age in enumerate(allaince_wise.index, 1):
		for col in allaince_wise.columns:
			age_wise['{{' + f"age_{age}_{str(col).lower()}_share" + "}}"] = allaince_wise.loc[age, col]
	return age_wise


def get_pm_pref(df, pm_pref_col, score, rows_needed=12):
	top_pms = df.groupby(pm_pref_col).agg({score: 'sum'}).sort_values(by=score, ascending=False).head(
		15).index.tolist()
	df = df.copy()
	df[pm_pref_col] = df[pm_pref_col].apply(lambda x: x if x in top_pms else 'Others')
	pm_pref = df.groupby(pm_pref_col).agg({score: 'sum'}).sort_values(by=score, ascending=False)
	pm_pref = pm_pref.div(pm_pref[score].sum())
	inds = shift_to_last(pm_pref.index.tolist(), 'other')
	pm_pref = (pm_pref.loc[inds] * 100).round(2)
	pm_pref_dict = {}
	for i, ind in enumerate(pm_pref.index, 1):
		pm_pref_dict['{{' + f"pm_aspirant{i}" + "}}"] = ind
		pm_pref_dict['{{' + f"pm_aspirant{i}_vn" + "}}"] = pm_pref.loc[ind, score]
	for j in range(len(pm_pref) + 1, rows_needed + 1):
		pm_pref_dict['{{' + f"pm_aspirant{j}" + "}}"] = '-'
		pm_pref_dict['{{' + f"pm_aspirant{j}_vn" + "}}"] = '-'
	return pm_pref_dict


def get_ac_wise_winners(df, alliance, party_col, score, ac_dict, rank_upto,
						acs_in_pc):
	data = add_allaince(df, on=party_col, alliance=alliance)
	data = data.groupby(['ac_no', party_col], as_index=False).agg({score: 'sum'}).sort_values(
		by=['ac_no', score], ascending=[True, False])
	data['rank'] = data.groupby(by='ac_no').cumcount() + 1
	# TODO: Need to findout why fill_value is giving error in following pivot fill_value={party_col: '-', score: 0}
	data = data.pivot_table(index='ac_no', columns='rank', values=[party_col, score], aggfunc='sum')
	data[party_col] = data[party_col].fillna('-')
	data[score] = data[score].fillna(0)
	data[score] = (data[score].div(data[score].sum(1), 0) * 100).round(2)
	projected_winners = {}
	for i, ac in enumerate(data.index, 1):
		projected_winners["{{" + f"ac{i}" + "}}"] = str(ac) + "-" + ac_dict[ac]
		for rank in range(1, rank_upto + 1):
			if not (party_col, rank) in data.columns:
				projected_winners["{{" + f"ac{i}_pos{rank}_cand" + "}}"] = '-'
			else:
				if not data.loc[ac, (score, rank)] == 0:
					projected_winners["{{" + f"ac{i}_pos{rank}_cand" + "}}"] = data.loc[ac, (
						party_col, rank)] + f"\n({data.loc[ac, (score, rank)]}%)"
				else:
					projected_winners["{{" + f"ac{i}_pos{rank}_cand" + "}}"] = '-'
	for i in range(len(data) + 1, acs_in_pc + 1):
		projected_winners["{{" + f"ac{i}" + "}}"] = '-'
		for rank in range(1, rank_upto + 1):
			projected_winners["{{" + f"ac{i}_pos{rank}_cand" + "}}"] = '-'
	return projected_winners


def get_top_10_caste(caste_df=None, pc_name=None):
	if not pc_name:
		return None
	pc_castes = caste_df[caste_df['pc_name'] == pc_name].groupby(['caste', 'caste_category'], as_index=False).agg(
		{'caste_voters': 'sum'}).sort_values(by=['caste_voters'], ascending=False)
	others_removed = pc_castes[~(pc_castes['caste'].str.contains('others'))]
	with_others = pc_castes[(pc_castes['caste'].str.contains('others'))]
	if len(others_removed) > 10:
		final_df = others_removed.iloc[0:10]
		others_df = pd.DataFrame([{"caste": 'others',
								   'caste_category': '',
								   'caste_voters': others_removed['caste_voters'].iloc[10:].sum() + with_others[
									   'caste_voters'].sum()}])
		final_df = pd.concat([final_df, others_df], ignore_index=True)
	else:
		final_df = others_removed.copy()
		others_df = pd.DataFrame(
			[{"caste": 'others', 'caste_category': '', 'caste_voters': with_others['caste_voters'].sum()}])
		final_df = pd.concat([final_df, others_df], ignore_index=True)
	final_df['caste_voters'] = (final_df['caste_voters'].div(final_df['caste_voters'].sum()) * 100).round(2)
	return final_df


def get_top_caste_pc_wise(caste_df=None, pc_name=None):
	# Getting top 10 actual castes in a pc
	if not pc_name:
		return {}
	
	data = get_top_10_caste(caste_df=caste_df, pc_name=pc_name)
	
	top_castes = {}
	for i, index in enumerate(data.index, 1):
		top_castes["{{" + f"caste{i}_name" + "}}"] = str(data.loc[index, 'caste']).title()
		top_castes["{{" + f"caste{i}_category" + "}}"] = str(data.loc[index, 'caste_category']).upper()
		top_castes["{{" + f"caste{i}_share" + "}}"] = data.loc[index, 'caste_voters']
	
	for i in range(len(data) + 1, 12):
		top_castes["{{" + f"caste{i}_name" + "}}"] = '-'
		top_castes["{{" + f"caste{i}_category" + "}}"] = '-'
		top_castes["{{" + f"caste{i}_share" + "}}"] = '-'
	return top_castes


def get_caste_prefs(party_col=None, df=None, alliance=None, score=None):
	df = df.copy()
	df = add_allaince(df, alliance=alliance, on=party_col)
	
	pc_castes = df.groupby(['caste', 'caste_category'], as_index=False).agg(
		{'raw': 'sum'}).sort_values(by=['raw'], ascending=False)
	others_removed = pc_castes[~pc_castes['caste'].str.contains('other')]
	with_others = pc_castes[pc_castes['caste'].str.contains('other')]
	if len(others_removed) > 9:
		final_df = others_removed.iloc[0:9]
		others_df = pd.DataFrame([{"caste": 'others',
								   'caste_category': '',
								   'raw': others_removed['raw'].iloc[9:].sum() + with_others[
									   'raw'].sum()}])
		final_df = pd.concat([final_df, others_df], ignore_index=True)
	else:
		final_df = others_removed.copy()
		others_df = pd.DataFrame([{"caste": 'others', 'caste_category': '', 'raw': with_others['raw'].sum()}])
		final_df = pd.concat([final_df, others_df], ignore_index=True)
	final_df['raw'] = (final_df['raw'].div(final_df['raw'].sum()) * 100).round(2)
	
	df['caste'] = df['caste'].apply(lambda x: x if x in final_df['caste'].values.tolist() else 'others')
	inds = df[df['caste'] == 'others'].index
	df.loc[inds, 'caste_category'] = ''
	all_castes = df.pivot_table(index=['caste', 'caste_category'], columns=party_col, values=score,
								aggfunc='sum', fill_value=0)
	all_castes = (all_castes.div(all_castes.sum(1), 0) * 100).round(2)
	all_castes = all_castes.reset_index()
	final_df = final_df.merge(all_castes, on=['caste', 'caste_category'], how='left')
	return final_df


def get_top_caste_prefs(party_col=None, df=None, alliance=None, score=None):
	data = get_caste_prefs(party_col=party_col, df=df, alliance=alliance, score=score)
	
	top_castes = {}
	for i, index in enumerate(data.index, 1):
		top_castes["{{" + f"sample_caste{i}" + "}}"] = str(data.loc[index, 'caste']).title()
		top_castes["{{" + f"sample_caste{i}_share" + "}}"] = data.loc[index, 'raw']
		for col in data.columns.tolist()[2:]:
			top_castes["{{" + f"sample_caste{i}_{col.lower()}_share" + "}}"] = data.loc[index, col]
	
	for i in range(len(data) + 1, 11):
		top_castes["{{" + f"sample_caste{i}" + "}}"] = '-'
		top_castes["{{" + f"sample_caste{i}_share" + "}}"] = '-'
		for col in data.columns.tolist()[2:]:
			top_castes["{{" + f"sample_caste{i}_{col.lower()}_share" + "}}"] = '-'
	return top_castes
