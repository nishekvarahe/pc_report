import sys

from generate_report import PCReport
from utils.utils import get_config

state_code, config = get_config()

if not config:
	print("Pass the correct statecode or add config for the same in config.yaml file")
	sys.exit(0)

report_obj = PCReport(state_code=state_code, dump_folder=config['dump_folder'],
					  template_file_id=config['template_file_id'],
					  ac_acceptance_sample_threshold=config['ac_acceptance_sample_threshold'],
					  calculation_score=config['calculation_score'], alliance=config['alliance'],
					  election_round=config['election_round'], election_cycle=config['election_cycle'],
					  caste_category_needed=config['caste_category_needed'],
					  ac_wise_top_candidate_needed=config['ac_wise_top_candidate_needed'],
					  pc_reports=config['pc_reports'])
