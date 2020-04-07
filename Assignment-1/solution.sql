/* Author: Nithiya Shree Uppara
   I'd: 1215229834
*/

/* Creating table for users*/
CREATE TABLE users(
   userid int PRIMARY KEY,
   name TEXT NOT NULL
);

/* Creating table for movies*/
CREATE TABLE movies(
   movieid int PRIMARY KEY,
   title TEXT NOT NULL
);

/* Creating table for taginfo*/
CREATE TABLE taginfo(
   tagid int PRIMARY KEY,
   content TEXT NOT NULL
);

/* Creating table for genres*/
CREATE TABLE genres(
   genreid int PRIMARY KEY,
   name TEXT UNIQUE NOT NULL
);

/* Creating table for ratings*/
CREATE TABLE ratings(
   userid int REFERENCES users ( userid) ON UPDATE CASCADE ON DELETE CASCADE,
   movieid int REFERENCES movies ( movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   rating numeric CHECK ( rating >= 0 and rating <= 5),
   timestamp bigint NOT NULL,
   PRIMARY KEY (userid, movieid)
);

/* Creating table for tags*/
CREATE TABLE tags(
   userid int REFERENCES users ( userid) ON UPDATE CASCADE ON DELETE CASCADE,
   movieid int  REFERENCES movies ( movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   tagid int REFERENCES taginfo ( tagid) ON UPDATE CASCADE ON DELETE CASCADE,
   timestamp bigint NOT NULL default (extract(epoch from now()) * 1000),
   PRIMARY KEY( userid, movieid, tagid)
);

/* Creating table for hasagenre*/
CREATE TABLE hasagenre(
   movieid int REFERENCES movies(movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   genreid int REFERENCES genres(genreid) ON UPDATE CASCADE On DELETE CASCADE,
   PRIMARY KEY ( movieid, genreid)
);

