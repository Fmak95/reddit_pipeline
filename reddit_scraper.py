import praw
from settings import CLIENT_ID, CLIENT_SECRET, USER_AGENT
import pdb
from datetime import datetime
from dateutil import tz
import pprint
import pandas as pd
from collections import defaultdict
import time
import pymysql
from sqlalchemy import create_engine

"""
Plan on creating three tables (Title), (Comments), and (Redditors) 
see the Entity Recognition diagram for more details
"""

class Reddit():
	def __init__(self,subreddit):
		self.reddit = praw.Reddit(client_id=CLIENT_ID,
							client_secret=CLIENT_SECRET,
							user_agent=USER_AGENT)

		self.subreddit = self.reddit.subreddit(subreddit)


def pull_and_clean_data(reddit, filter_by = "hot", limit = 10):
	'''
	Searches a subreddit and gets the data from the top posts.
	Return 3 pandas dataframes from Titles, Comments and Redditors


	'''
	start = time.time()
	submission_dict = defaultdict(list)
	comment_dict = defaultdict(list)
	redditor_dict = defaultdict(list)

	subreddit = reddit.subreddit
	submissions = getattr(subreddit, filter_by)(limit=limit)
	for submission in submissions:

		if submission:
			submission_data = clean_submission(submission)
			redditor_data = clean_redditor(submission.author)

			for k, v in submission_data.items():
				submission_dict[k].append(v)

			for k, v in redditor_data.items():
				redditor_dict[k].append(v)

		#Only grabbing the top ten comments
		for comment in submission.comments[:10]:

			comment_data = clean_comment(comment)
			redditor_data = clean_redditor(comment.author)

			if comment_data:
				for k, v in comment_data.items():
					comment_dict[k].append(v)

			if redditor_data:
				for k, v in redditor_data.items():
					redditor_dict[k].append(v)

	title_df = pd.DataFrame(submission_dict)
	comment_df = pd.DataFrame(comment_dict)
	redditor_df = pd.DataFrame(redditor_dict).drop_duplicates()
	stop = time.time()
	print(stop - start)
	return title_df, comment_df, redditor_df

def clean_submission(submission):
	'''
	Takes in a reddit submission instance and returns the following values in a dictionary:
	- submission_id (primary key)
	- title
	- title_score
	- author_id
	- date_posted
	- num_comments
	'''
	submission_dict = {
		'submission_id': submission.id,
		'title': submission.title,
		'title_score': submission.score,
		'author_id': submission.author.id,
		'date_posted': utc_to_est(submission.created_utc),
		'num_comments': submission.num_comments
	}
	return submission_dict

def utc_to_est(utc):
	'''
	Converts float to est local time, truncates seconds
	'''
	from_zone = tz.gettz('UTC')
	to_zone = tz.gettz('America/New_York')
	utc = datetime.utcfromtimestamp(utc)
	utc = utc.replace(tzinfo=from_zone)
	est = utc.astimezone(to_zone)
	return str(est)[:-6]

def clean_comment(comment):
	'''
	Takes in a reddit comment instance and returns the following values in a dictionary:
	- comment_id (primary key)
	- submission_id (foreign key)
	- commentor_id
	- comment
	- date_posted
	- comment_score
	'''
	try:
		comment_dict = {
			'comment_id': comment.id,
			'submission_id': comment.link_id[3:],
			'commentor_id': comment.author.id,
			'comment': comment.body,
			'date_posted': utc_to_est(comment.created_utc),
			'comment_score': comment.score
		}
		return comment_dict

	except AttributeError:
		return None

def clean_redditor(redditor):
	try:
		redditor_dict = {
			'redditor_id': redditor.id,
			'username': redditor.name
		}
		return redditor_dict

	except AttributeError:
		return None

def df_to_sql(dfs, engine):
	'''
	A function that takes a pandas dataframe and stores it into an SQL database
	'''
	title_df, comment_df, redditor_df = dfs
	title_df.to_sql(name='Title', con=engine, if_exists = 'replace', index=False)
	comment_df.to_sql(name='Comment', con=engine, if_exists = 'replace', index=False)
	redditor_df.to_sql(name='Redditor', con=engine, if_exists = 'replace', index=False)

def main():
	reddit = Reddit('askreddit')
	title_df, comment_df, redditor_df = pull_and_clean_data(reddit, filter_by='hot')
	engine = create_engine('mysql+pymysql://root:password123@localhost:3306/reddit_database', echo=False)
	df_to_sql([title_df, comment_df, redditor_df], engine)

if __name__ == '__main__':
	main()