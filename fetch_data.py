import json

import pandas as pd
import psycopg2
from pymongo import MongoClient

from db_queries import get_survey_sample_pipeline, get_ac_pc_mapping_query, get_retro_data_query, \
	get_caste_details_query


class MongoDBFetcher:
	def __init__(self, database_name: str, collection_name: str, election_round: str, election_cycle: str):
		with open('./api_keys/mongo_creds.json', 'r') as file:
			creds = json.load(file)
		username = creds.get('username', '')
		password = creds.get('password', '')
		
		connection_string = (f'mongodb+srv://{username}:{password}@surveycluster.pb3m1pe.mongodb.net/?retryWrites=true'
							 f'&w=majority')
		self.client = MongoClient(connection_string)
		self.collection = self.client[database_name][collection_name]
		self.survey_samples = self.get_survey_samples(election_round=election_round, election_cycle=election_cycle)
		self.client.close()
	
	def convert_cursor_to_list(self, cursor=None):
		resp = []
		for doc in cursor:
			resp.append(doc)
		return resp
	
	def get_survey_samples(self, election_round: str, election_cycle: str):
		pipeline = get_survey_sample_pipeline(election_cycle=election_cycle, election_round=election_round)
		cursor = self.collection.aggregate(pipeline=pipeline)
		survey_samples = pd.DataFrame(self.convert_cursor_to_list(cursor=cursor))
		survey_samples = survey_samples.join(pd.json_normalize(survey_samples['_id']))
		survey_samples = survey_samples.drop(columns="_id")
		return survey_samples


class PostgreSQLFetcher:
	def __init__(self, state_code: str):
		with open('./api_keys/postgre_creds.json', 'r') as file:
			creds = json.load(file)
		self.state_code = state_code
		dbname = creds.get('database', '')
		user = creds.get('user', '')
		password = creds.get('password', '')
		host = creds.get('host', 'localhost')
		port = creds.get('port', '5432')
		self.connection = psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")
		self.ac_pc_mapping = self.fetch_data(*get_ac_pc_mapping_query(self.state_code))
		self.retro_data = self.fetch_data(*get_retro_data_query(self.state_code))
		self.actual_caste_data = self.fetch_data(*get_caste_details_query(self.state_code))
		
		self.connection.close()
	
	def fetch_data(self, query: str, columns: list):
		cur = self.connection.cursor()
		cur.execute(query=query)
		data = cur.fetchall()
		cur.close()
		data = pd.DataFrame(data, columns=columns)
		return data
