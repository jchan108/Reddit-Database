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

import traceback
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
    for submission in subreddit.new(limit=30): 
        #    for submission in subreddit.new(limit=None): 

        #i = i + 1
        sub_name = submission.subreddit
        utcPostTime = submission.created
        submissionDate = datetime.utcfromtimestamp(utcPostTime)
        submissionDateTuple = submissionDate.timetuple()

        currentTime = datetime.utcnow()

        #How long ago it was posted.
        submissionDelta = currentTime - submissionDate
        
        submissionDelta = str(submissionDelta)
        
        #convert the redditor and title class types to strings
        posts.append([str(submission.title), str(submission.author), submission.score, submission.id, str(submission.subreddit), submission.url, submission.num_comments, submission.selftext, submission.created,submission.upvote_ratio,submission.stickied])

        #list() will conduct Breadth-first traversal using a queue.
        #limit = none if you want to access all the load more.
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


def InsertTables(stageposts,stagecomments,engine):
                
        #insert our data into postgres tables
        conn = engine[0].connect()
        
        
        comments = pd.DataFrame(stagecomments,columns = ['author', 'body', 'timestampUTC', 'commentid', 'is_submitter','link_id','parent_id','score','is_stickied','submission'])        
        posts = pd.DataFrame(stageposts,columns=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created','upvoteratio','stickied'])
        
        
        comments.to_sql('posts',con = conn,if_exists = 'append', index = False)
        posts.to_sql('threads',con=conn,if_exists = 'append', index = False)
        
        #conn = psycopg2.connect(conn_string)
        #conn.autocommit = True
        
        conn.close()   




def create_thread_update(params,stage):
    
        conn = psycopg2.connect(user = params[0],
                   password = params[1],
                    host = params[2],
                    port = params[3],
                   dbname = params[4])
    
        #update score of post stored in postgresql.
        #batch for posts
        tempthreads = stage.iloc[:,[3,2,6,9]]
        ids2 = [f"t3_{post_id}" for post_id in tempthreads.iloc[:,0]]
   
        thread_attributes = [(submission.score, submission.num_comments, submission.upvote_ratio, submission.id) for submission in reddit.info(fullnames=ids2)]
        
        try:
            sql = """UPDATE threads SET score = %s, num_comments = %s, upvoteratio = %s WHERE id = %s"""
            cur = conn.cursor()
            
            #executemany(query, vars_list)
            #vars_list must be a list of tuples
            cur.executemany(sql, (thread_attributes))
            conn.commit()
            cur.close()
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("error encountered")
            #print(Exception)
            print(traceback.format_exc())
        finally:
            if conn is not None:
                conn.close()
                
        
def create_posts_update(params, stage):
    
        conn = psycopg2.connect(user = params[0],
                   password = params[1],
                    host = params[2],
                    port = params[3],
                   dbname = params[4])
    
        #update score of post stored in postgresql.
       #batch for posts
        tempcomm = stage.iloc[:,[3]]
        ids = [f"t1_{post_id}" for post_id in tempcomm.iloc[:,0]]
   
        post_attributes = [(submission.score,submission.id) for submission in reddit.info(fullnames=ids)]
        
        try:
            sql = """UPDATE posts SET score = %s WHERE commentid = %s"""
            cur = conn.cursor()
            #cur.execute(sql, (1,))
            
            #executemany(query, vars_list)
            #vars_list must be a list of tuples
            cur.executemany(sql, (post_attributes))
            conn.commit()
            cur.close()
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("error encountered")
            #print(Exception)
            print(traceback.format_exc())
        finally:
            if conn is not None:
                conn.close()
                
#keep searching for new comments
#if a comment's thread is not found, add the thread to the threads table
#comments are streamed oldest to newest
#skip all comments that are already in table

def comment_stream3(subreddit, comment_table, thread_table, engine):
    #how often to merge staging table with the production table? every thread or like every 50 comments?
    
    #staging tables are uploaded to postgresql in batches, 
    #comment_table and thread_table are up to date and stored in python and referenced in loop
    #might be a more effecient way (access reference directly to sql stored data instead of storing in python?)
    
    temptablethread = []
    temptablecomments = []
    
    i = 1
    for comment in reddit.subreddit(subreddit).stream.comments():
        #print(i)
        #print(comment.body)
        #if comment is already in
        #need to cut off first 3 letters from commentid
        if ( str(comment.id)[3:] in comment_table.commentid.unique() ) == True:
        #if (comment.id in comment_table.iloc[:,3].unique()) == True:
            print("Comment is already inside the comment table")
            continue
            
        #if the thread is not in threads table, add it to the threads table

        if ( str(comment.link_id)[3:] in thread_table.id.unique() ) == False:
        #if ( str(comment.link_id) in thread_table.iloc[:,3].unique() ) == False:
            print("attempting to create new thread")
            tempthread = comment.submission
            tempthreadparam = [str(tempthread.title), str(tempthread.author), tempthread.score, tempthread.id, 
                          str(tempthread.subreddit), tempthread.url, tempthread.num_comments, tempthread.selftext, 
                          tempthread.created,tempthread.upvote_ratio,tempthread.stickied]
            #update our our staging table for threads.
            temptablethread.append(tempthreadparam)
            
            #append pd type to permanent thread_table

            pdthreads = pd.Series(tempthreadparam,index=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created','upvoteratio','stickied'])        
            thread_table = thread_table.append(pdthreads, ignore_index = True)
      
        else: 
            print("thread already in the table")
            #print(comment.link_id)
        
        #final if resolution, add the comment to the temp comment table
        print('adding new comment')
        
    
        tempcommentparam = [str(comment.author),str(comment.body),comment.created_utc,
                         comment.id,comment.is_submitter,comment.link_id, comment.parent_id,
                           comment.score,comment.stickied,str(comment.submission)]    
        temptablecomments.append(tempcommentparam)
        
        #append pd type to permanent comment_table
        pdcomments = pd.Series(tempcommentparam,index = ['author', 'body', 'timestampUTC', 'commentid', 'is_submitter','link_id','parent_id','score','is_stickied','submission'])        
        comment_table = comment_table.append(pdcomments, ignore_index = True)
    
        
        i = i + 1
        #every 20 new comments, start merging our tables with our postgre database
        #also update parameters of the database
        if i%20 == 0:
            print("attempting merge")
            InsertTables(temptablethread,temptablecomments, engine)
            
            #access column values of score
            #tempcommentupdate = comment_table.iloc[:,[3,7]]
            create_posts_update(engine[1:6],comment_table)
            print("posts merge done.")
            
            #access column values of score, num_comments, and upvoteratio
            #tempthreadupdate = thread_table.iloc[:,[3,2,6,9]]
            create_thread_update(engine[1:6], thread_table)
            print("threads merge done.")
            
        
            #empty our staging tables
            temptablethread = []
            temptablecomments = []


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
        dbname = 'Reddit Database2'
    
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

def realupdate(subreddit, stagingcomments, stagingthreads, engine):
    
    update = False
    while update == False:
        
        realtimeupdate = input("Please enter 'Continue' if you would like to proceed with launching the real time update system, or enter 'No' to finish the database.")
        if realtimeupdate == "Continue":
            print("you want to continue")
            update = True
            comment_stream2(subreddit,stagingcomments,stagingthreads, engine)

            
        elif realtimeupdate == "Stop":
            print("you want to stop")
            update = True
            break
        else:
            print("you did not put in a valid input, please try again.")
            



if __name__ == '__main__':

        
  
    try:
        
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        #conn_string = "postgresql://postgres:password@127.0.0.1:5432/Reddit Database"
        
        engine = get_connection()
        
        subreddit = obtain_subreddits()
        validSubmissions = submissionsWithin24hours(subreddit)

        #initialize empty tables
        create_tables(engine[1:6])
        
        
        #insert our data into postgres tables
        conn = engine[0].connect()
        
        #staging tables for threads and comments to be moved to database
        stagingthreads = validSubmissions[0]
        stagingcomments = validSubmissions[1]
        
        stagingcomments.to_sql('posts',con = conn,if_exists = 'replace', index = False)
        stagingthreads.to_sql('threads',con=conn,if_exists = 'replace', index = False)
        
        print(
            f"Connection to the {engine[2]} for user {engine[1]} created successfully.")     
        
        conn.close()
        
        #now we see if the user wants to proceed with realtime updating, or stop here.

        realupdate(subreddit, stagingcomments, stagingthreads, engine)
        
        print("Database connection is now terminated and the program has ended.")
        

        
    except Exception as ex:
        print("Could not interact with database due to the following error: \n", ex)





















