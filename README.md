Day 1
=====

* Initialisation of repository
* Create a file to make connection with sql and also create a sample query to fire at mysql
* Created functions to initialise connection, drop tables if already present, create new tables, shutdown the connection
  to give a more OO approach to application...:D
* Using Eclipse IDE for developing the application
* Completed the insertion part.. final database is now ready for a mapred job.
* Need to figure out the mapper and reducer job related to this data.

Following schema will be implemented:-

TODO
-----
    Show aggregate data on the screen like facebook(DONE)
    Create a map reduce job something like, outputting a wordcount on each users wall
        The above requires creating a new table aggregate something like (userid,aggregated string of wall)
        Then do a mapred job of word count for each userid and store the result in db as userid,word,count with pk as
        (userid,word)

User
----
    CREATE TABLE User (
    id INTEGER PRIMARY KEY,
    username VARCHAR(64),
    password VARCHAR(64)
    );

UserProfile
-----------
    CREATE TABLE UserProfile (
    user_id INTEGER REFERENCES User(id),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER
    relationship_status VARCHAR(10)
    );

Friends
-------
    CREATE TABLE Friends (
    user_id INTEGER REFERENCES User(id),
    friend_id INTEGER REFERENCES User(id),
    PRIMARY KEY (user_id,friend_id)
    );

WallPosts
----------
    CREATE TABLE WallPosts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES User(id),
    body VARCHAR (200),
    timestamp TIMESTAMP
    );

Comments
--------
    CREATE TABLE Comments (
    id INTEGER,
    wall_id INTEGER REFERENCES WallPosts(id),
    userid INTEGER,
    body VARCHAR(200),
    timestamp TIMESTAMP
    );


Day 2
=====
* Started on figuring out on how to implement Mapper and Reducer
* Found some examples and tutorials explaining the same, and took their reference to implement the same.
* Current implementation is not tested, messy, not clean and involves lot of bugs

