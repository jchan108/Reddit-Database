
import pandas as pd
import numpy as np
import praw
import datetime as dt

import praw
import time
import datetime
from datetime import datetime, timedelta


import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine

#need to authenticate ourselves before scraping data, create a reddit instance
reddit = praw.Reddit(client_id='6hFNZUMglTJ0OMOIbLk_OQ', client_secret='SE-U9OP2x5j0lqLeNAdL3CH-ac9m9g', user_agent='Webscrape')


def submissionsWithin24hours(subreddit):
    #default is new
    subreddit = reddit.subreddit(subreddit)
    #hot_posts = reddit.subreddit('MachineLearning').hot(limit=10)

    #i = 0
    posts = []
    comments = []
    #submissionsLast24 = []
    for submission in subreddit.new(limit=None): 
        #    for submission in subreddit.new(limit=None): 

        #i = i + 1
        sub_name = submission.subreddit
        utcPostTime = submission.created
        submissionDate = datetime.utcfromtimestamp(utcPostTime)
        submissionDateTuple = submissionDate.timetuple()

        currentTime = datetime.utcnow()

        #How long ago it was posted.
        #submissionDelta = currentTime - submissionDate
        
        #submissionDelta = str(submissionDelta)
        
        #convert the redditor and title class types to strings
        posts.append([str(submission.title), str(submission.author), submission.score, submission.id, str(submission.subreddit), submission.url, submission.num_comments, submission.selftext, submission.created,submission.upvote_ratio,submission.stickied])

        #list() will conduct Breadth-first traversal using a queue.
        #limit = none if you want to access all the load more.
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            #print(comment.body)
            comments.append([str(comment.author),str(comment.body),comment.created_utc,comment.id,comment.is_submitter,comment.link_id, comment.parent_id,
                           comment.score,comment.stickied,str(comment.submission)])
        
        
        #if 'day' not in submissionDelta:
        #    submissionsLast24.append((title, link, author, submissionDelta,submissionDate, sub_name))
        
    comments = pd.DataFrame(comments,columns = ['author', 'body', 'timestampUTC', 'commentid', 'is_submitter','link_id','parent_id','score','is_stickied','submission'])        
    threads = pd.DataFrame(posts,columns=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created','upvoteratio','stickied'])
    return threads,comments
    #return submissionsLast24


def combinestring(zep):
    concatstring = ""
    i = 1
    for z in zep:
        concatstring = concatstring + z 
        if (len(zep)!= i):
            concatstring = concatstring + "+"
        i = i + 1
    
    #print(concatstring)
    return concatstring

#initiate connection with postgresql with sqlalchemy function
def get_connection():
    
    try:
    
        user = input("Enter username:")
        password = input("Enter password:")
        host = '127.0.0.1'
        port = '5432'
        database = 'Reddit Database'
    
        #returns the engine, host, and user
        return (create_engine(
            url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, database)),
                user,
                host)
    except Exception as ex:
            print("Connection could not be made due to the following error: \n", ex)
            
def exists_sub(subs):
    exists = True
    try:
        reddit.subreddits.search_by_name(subs, exact = True)
    except Exception as notfound:
        exists = False
    return exists	
	
def obtain_subreddits():
    user_subreddits = []
    
    getsubs = True

    new_sub = input("Enter a subreddit that you would like to add to the database, or type '+Exit' to exit\n")
    if new_sub == "+Exit":
        getsubs = False
    else:
        user_subreddits.append(new_sub)
        
    #need to check if a requested subreddit exists too
    while getsubs:
        new_sub = input("Enter another subreddit that you would like to add to database, or type ''+Exit' to exit\n")
        if new_sub == "+Exit":
            getsubs = False
        elif exists_sub(new_sub) == False:
            print("Subreddit does not exist.")
        else:
            user_subreddits.append(new_sub)
            
    #return user_subreddits
    return combinestring(user_subreddits)
            
def get_connection():
    
    try:
    
        user = input("Enter username:")
        password = input("Enter password:")
        host = '127.0.0.1'
        port = '5432'
        dbname = 'Reddit Database'
    
        #returns the engine, host, and user
        return (create_engine(
            url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
                user, password, host, port, dbname)),
                user, password, host, port, dbname)
    except Exception as ex:
            print("Connection could not be made due to the following error: \n", ex)
			

#create tables using psycopg2
def create_tables(params):
    
    commands = (
        
        """
        DROP TABLE IF EXISTS posts
        """,
        
        """
        DROP TABLE IF EXISTS threads
        """,
        
        """
        CREATE TABLE IF NOT EXISTS threads (
            title varchar(300),
            author varchar(255),
            link_id varchar(255),
            subreddit varchar(255),
            url varchar(255),
            num_comments int,
            body varchar(40000),
            timestampUTC bigint,
            upvoteratio decimal(5,2),
            stickied bit,
            PRIMARY KEY (link_id)
        )
        """,
        
        """ CREATE TABLE IF NOT EXISTS posts (
                author varchar(255),
                body varchar(10000),
                timestampUTC bigint,
                commentid varchar(255),
                is_submitter bit,
                link_id varchar(255),
                parent_id varchar(255),
                score int,
                is_stickied bit,
                submission varchar(255),
                PRIMARY KEY (commentid),
                FOREIGN KEY (link_id) REFERENCES threads(link_id)
                )     
        """
    )
    
    try:

        # connect to the PostgreSQL server
        conn = psycopg2.connect(user = params[0],
                                password = params[1],
                                host = params[2],
                                port = params[3],
                                dbname = params[4]
        )
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
        print("Tables succesfully created in PostgreSQL")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
	
	
if __name__ == '__main__':

        
  
    try:
        
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        #conn_string = "postgresql://postgres:password@127.0.0.1:5432/Reddit Database"
        
        engine = get_connection()
        
        #subreddit = 'KusanaliMains'
        subreddit = obtain_subreddits()
        validSubmissions = submissionsWithin24hours(subreddit)

        
        
        create_tables(engine[1:6])
        
        
        #insert our data into postgres tables
        conn = engine[0].connect()
        validSubmissions[1].to_sql('posts',con = conn,if_exists = 'replace', index = False)
        validSubmissions[0].to_sql('threads',con=conn,if_exists = 'replace', index = False)
        
        #conn = psycopg2.connect(conn_string)
        #conn.autocommit = True
        
        conn.close() 
        
        print(
            f"Connection to the {engine[2]} for user {engine[1]} created successfully.")
     
        
    except Exception as ex:
        print("Could not interact with database due to the following error: \n", ex)
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
