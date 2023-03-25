--all comments with the title and body of the post they were made on
CREATE VIEW postsTitle AS
SELECT threads.title, threads.body AS threadbody, posts.body AS postbody
FROM threads
INNER JOIN posts ON threads.id = posts.submission

--Find all comments for a specific post
SELECT * FROM posts WHERE submission =  'xs028h';

--Find all top level comments for a specific post xs028h
SELECT * FROM posts 
WHERE parent_id = link_id AND submission = 'xs028h';

--find all comments made by a user who has posted a thread
SELECT threads.author, posts.body, posts.commentid, posts.submission FROM threads
INNER JOIN posts ON threads.author = posts.author
WHERE threads.id = posts.submission

--Find the total number of comments made by users on their own threads.
CREATE VIEW table1 AS
SELECT body FROM posts WHERE is_submitter = true

--Alternative way to find total number of comments made by users on their own threads
CREATE VIEW table2 AS
SELECT posts.body FROM threads
INNER JOIN posts ON threads.author = posts.author
WHERE threads.id = posts.submission

SELECT count(*) FROm table2
--expect 2991, but output is 2999, investigate what is causing the issue.

CREATE VIEW missing AS
SELECT * FROM table2
WHERE body NOT IN (select body from table1)

SELECT * FROm missing

SELECT * FROM posts
WHERE posts.body IN (SELECT * FROM missing)
--so the author is none is causing these issues, 


--find all comments that people have responded to
--produces a list of all the commentid's that another user has commented to.
SELECT distinct(p1.commentid) FROM posts p1, posts p2
WHERE p1.commentid = SUBSTRING(p2.parent_id,4,length(p2.parent_id)) 


--select base comments
SELECT * FROM posts WHERE parent_id = link_id;

--parent comments that are not threads are prefixed with t1
--parent comments that are threads are prefixed with t3


--hierarchical query
--make a change from two table threads/post to a more defined hierarchical model.
--model the hierarchy with a single table approach/adjacency model


CREATE TABLE posts3 AS SELECT null AS title, author, body, commentid, SUBSTRING(parent_id,4,length(parent_id)) AS parent_id FROM posts;
CREATE TABLE AdjTable AS SELECT title, author, body, id AS commentid, id AS parent_id  FROM threads;

SELECT * FRom posts3;
SELECT * FROM threads3;

INSERT INTO AdjTable
SELECT * FROM posts3;

--combine posts and threads into one table so that we can use an adjacency list hierarchical model.
--commentid = parent_id indicates that it is a thread.

--Threads with no text in the body
--SELECT * FROM AdjTable
--WHERE body = ''

SELECT * FROM AdjTable


DROP TABLE Adj
Create Table Adj AS SELECT * FROM AdjTable

--create a top node with null parent value that all threads will point to.
INSERT INTO Adj 
VALUES ('Reddit', NULL, NULL, 'abcde', NULL);

--set all threads parent_id to be equal to our Reddit node.

Update Adj
SET parent_id = 'abcde'
WHERE parent_id = commentid



Select * FROM Adj

--begin working with our Adj list

-- find all roots (comments or threads that have no replies)
-- self join, pivot on commentid and compile all comments/posts that have that commentid as their parent_id
-- if no post/comment has the pivoted commentid as their parent_id, then it will be assigned as NULL
SELECT a.title, a.author, a.body
FROM adj a
LEFT JOIN adj b ON a.commentid = b.parent_id
WHERE b.commentid IS NULL;


--utilize CTE to assign node levels to each post.

Create Table adjlevels AS WITH RECURSIVE empdata AS (
	(SELECT commentid, title, author, body, parent_id, 1 AS level
 	FROM adj
 	WHERE parent_id IS NULL)
	UNION ALL
	(SELECT this.commentid, this.title, this.author, this.body, this.parent_id, prior.level + 1
	FROM empdata prior
	INNER JOIN adj this ON this.parent_id = prior.commentid)
)
SELECT e.commentid, e.title, e.author, e.body, e.parent_id, e.level
FROM empdata e
ORDER BY e.level;

-- find all first level comments and the thread that they point to.
SELECT a.title, a.author, a.body, b.title
FROM adjlevels a
LEFT JOIN adjlevels b ON a.parent_id = b.commentid
WHERE a.level = 3

--select all comments from a specific thread, organized by node level.
WITH RECURSIVE empdata AS (
	(SELECT commentid, title, author, body, parent_id, 1 AS level
 	FROM adj
 	WHERE commentid = 'xtdqw5')
	UNION ALL
	(SELECT this.commentid, this.title, this.author, this.body, this.parent_id, prior.level + 1
	FROM empdata prior
	INNER JOIN adj this ON this.parent_id = prior.commentid)
)
SELECT e.commentid, e.title, e.author, e.body, e.parent_id, e.level
FROM empdata e
ORDER BY e.level;



--receive all leaf nodes (comments or threads with no replies)
SELECT * FROM
AdjTable AS t1 LEFT JOIN AdjTable as t2
ON t1.commentid = t2.parent_id
WHERE t2.commentid IS NULL;
--we have 100643 leaf nodes





