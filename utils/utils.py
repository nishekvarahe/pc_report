import yaml
from pydrive2.auth import GoogleAuth


def add_allaince(df=None, on=None, alliance=None):
	df = df.copy()
	for alliance_name, parties in alliance.items():
		df[on] = df[on].apply(lambda x: alliance_name if x in parties else x)
	df[on] = df[on].apply(lambda x: x if x in alliance.keys() else "Others")
	return df


def shift_to_last(indexes: list, what_to_shift: str) -> list:
	shifted = [i for i in indexes if what_to_shift in str(i).lower()]
	remaining = [i for i in indexes if what_to_shift not in str(i).lower()]
	return remaining + shifted


def get_vote_share(on=None, df=None, which_alliance=None, which_criteria=None):
	if not on:
		print("Specify the column name on which voteshare to be calculated")
		return None
	if not which_criteria:
		print("Add criteria for voteshare calculation like raw, vn, or actual votes...")
		return None
	if which_alliance:
		df = add_allaince(df, on, which_alliance)
	result = df.groupby([on]).aggregate({which_criteria: "sum"})
	total_votes = result[which_criteria].sum()
	vote_share = (result.div(total_votes) * 100).round(2)
	vote_share = vote_share.sort_values(by=which_criteria, ascending=False)
	inds = shift_to_last(vote_share.index.tolist(), 'other')
	return vote_share.loc[inds]


def get_config():
	with open('config.yaml', 'r') as file:
		config = yaml.safe_load(file)
	state_code = config['state_code']
	state_config = config[f'{state_code}_config']
	return state_code.upper(), state_config


def generate_oauth_credential_file():
	gauth = GoogleAuth(settings_file=r"./api_keys/drive/settings.yaml")
	gauth.LocalWebserverAuth()
