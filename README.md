Day 1
=====

* Initialisation of repository
* Create a file to make connection with sql and also create a sample query to fire at mysql
* Created functions to initialise connection, drop tables if already present, create new tables, shutdown the connection
  to give a more OO approach to application...:D
* Using Eclipse IDE for developing the application

Following schema will be implemented:-

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


