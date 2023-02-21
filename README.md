"#Reddit-Database"

This script predominantly uses the following packages: praw, pandas, and psycopg2

"RedditDatabase.py" Will pull the most recent 1000 posts and each post's comments from a list of user inputted subreddits and store them into a PostgreSQL database that has been previously created.

Once "RedditDatabase.py" is run, the user will be asked to enter their PostgreSQL credentials so that the SQLAlchemy package will be able to make a connection with it.

The user will then be asked to enter all of the subreddits that they want to be implemented into the database.

The program will connect to the PostgreSQL DBMS and generate a database which will be populated with posts and comments from various subreddits according to the user's specification.

The PRAW API only allows 1000 Threads to be parsed each time a request is called. For each thread called, the comments are accessed through breadth-first traversal to quickly access replies to the thread and replies to other replies.


After the initial threads and posts table is created, the user can choose to have new comments and threads stored in the database in real time. While being run, the python program will fetch all new threads and comments made in the specified subreddits and update the database to include them as they are fetched. 

Additional parameters for the posts and comments, such as number of upvotes and number of subcomments are also updated during this time.
