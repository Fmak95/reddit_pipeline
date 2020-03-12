"""
Grab data from reddit as JSON blobs and store them in S3 bucket
1. connect to reddit PRAW
2. get json data
3. store it in s3
"""

import praw
from settings import CLIENT_ID, CLIENT_SECRET, USER_AGENT
import pdb


"""
Create a Reddit object that holds some useful information we wish to store in our s3 bucket later on
1. subreddit name
2. score of post
3. title of post
4. num_comments on post
5. 

"""
class Reddit():
	def __init__(subreddit):
		self.api = praw.Reddit(cliend_id=CLIENT_ID,
							client_secret=CLIENT_SECRET,
							user_agent=USER_AGENT)
		self.subreddit = self.api.subreddit(subreddit)



reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET,
                     user_agent=USER_AGENT)

for submission in reddit.subreddit('askreddit').hot(limit=10):
    print(submission.title)
    pdb.set_trace()