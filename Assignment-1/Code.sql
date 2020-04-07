DROP DATABASE movies;
CREATE DATABASE IF NOT EXISTS movies;

SHOW DATABASES;

CREATE TABLE users(
   userid int PRIMARY KEY,
   name TEXT NOT NULL
);

CREATE TABLE movies(
   movieid int PRIMARY KEY,
   title TEXT NOT NULL
);

CREATE TABLE taginfo(
   tagid int PRIMARY KEY,
   content TEXT NOT NULL
);

CREATE TABLE genres(
   genreid int PRIMARY KEY,
   name TEXT UNIQUE NOT NULL
);

CREATE TABLE ratings(
   userid int REFERENCES users ( userid) ON UPDATE CASCADE ON DELETE CASCADE,
   movieid int REFERENCES movies ( movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   rating numeric CHECK ( rating > 0 and rating < 6),
   timestamp bigint NOT NULL,
   PRIMARY KEY (userid, movieid)
);

CREATE TABLE tags(
   userid int REFERENCES users ( userid) ON UPDATE CASCADE ON DELETE CASCADE,
   movieid int  REFERENCES movies ( movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   tagid int REFERENCES taginfo ( tagid) ON UPDATE CASCADE ON DELETE CASCADE,
   timestamp bigint NOT NULL default (extract(epoch from now()) * 1000),
   PRIMARY KEY( userid, movieid, tagid)
);

CREATE TABLE hasagenre(
   movieid int REFERENCES movies(movieid) ON UPDATE CASCADE ON DELETE CASCADE,
   genreid int REFERENCES genres(genreid) ON UPDATE CASCADE On DELETE CASCADE,
   PRIMARY KEY ( movieid, genreid)
);

COPY users
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/users.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY movies
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/movies.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY taginfo
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/taginfo.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY genres
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/genres.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY ratings
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/ratings.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY tags
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/tags.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';

COPY hasagenre
     FROM '/Users/nithiya/Downloads/Coursera-ASU-Database-master/course1/assignment1/exampleinput/hasagenre.dat'
     USING DELIMITERS '%' WITH NULL AS 'null_string';




