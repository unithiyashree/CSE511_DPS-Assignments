/* Author: Nithiya Shree Uppara
   I'd: 1215229834
*/

/* 1st query solution */
CREATE TABLE query1 AS SELECT genres.name as name, count(movies.movieid) as movieCount
FROM genres, movies, hasagenre
WHERE genres.genreid = hasagenre.genreid AND movies.movieid = hasagenre.movieid GROUP By genres.genreid ORDER BY genres.name;

/* 2nd query solution */
CREATE TABLE query2 AS SELECT genres.name as name, avg(ratings.rating) as rating
FROM genres, ratings, hasagenre
WHERE genres.genreid = hasagenre.genreid AND ratings.movieid = hasagenre.movieid GROUP By genres.genreid ORDER BY genres.name;

/* 3nd query solution */
CREATE TABLE query3 AS SELECT movies.title as title, count(ratings.rating) as countofratings
FROM movies, ratings
WHERE movies.movieid = ratings.movieid GROUP BY movies.movieid HAVING count(ratings.rating) >= 10 ORDER BY movies.title;

/* 4th query solution */
CREATE TABLE query4 AS SELECT movies.movieid as movieid, movies.title as title
FROM movies, hasagenre, genres
WHERE movies.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid AND genres.name = 'Comedy' ORDER BY movies.title;

/* 5th query solution */
CREATE TABLE query5 AS SELECT movies.title as title, avg(ratings.rating) as average
FROM movies, ratings
WHERE movies.movieid = ratings.movieid GROUP BY movies.movieid ORDER BY movies.title;

/* 6th query solution */
CREATE TABLE query6 AS SELECT avg(ratings.rating) as average
FROM movies, ratings, hasagenre, genres
WHERE movies.movieid = ratings.movieid AND movies.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid AND genres.name = 'Comedy';

/* 7th query solution */
CREATE TABLE query7 AS SELECT avg(ratings.rating) as average
FROM movies, ratings, hasagenre, genres
WHERE movies.movieid = ratings.movieid AND movies.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid AND genres.name = 'Comedy' AND 
movies.movieid in (SELECT m.movieid
FROM movies m, ratings r, hasagenre h, genres g
WHERE m.movieid = r.movieid AND m.movieid = h.movieid AND h.genreid = g.genreid AND g.name = 'Romance') ;

/* 8th query solution */
CREATE TABLE query8 AS SELECT avg(ratings.rating) as average
FROM movies, ratings, hasagenre, genres
WHERE movies.movieid = ratings.movieid AND movies.movieid = hasagenre.movieid AND hasagenre.genreid = genres.genreid AND genres.name = 'Romance' AND 
movies.movieid NOT in (SELECT m.movieid
FROM movies m, ratings r, hasagenre h, genres g
WHERE m.movieid = r.movieid AND m.movieid = h.movieid AND h.genreid = g.genreid AND g.name = 'Comedy') ;

/* 9th query solution */
CREATE TABLE query9 AS SELECT movies.movieid as movieid, ratings.rating as rating
FROM movies, ratings
WHERE movies.movieid = ratings.movieid AND ratings.userid =:v1;

/* 10th query solution */
CREATE VIEW avgrate AS SELECT ratings.movieid AS movieid,avg(rating) AS avgrating FROM ratings GROUP BY ratings.movieid;
CREATE VIEW simtable AS SELECT rate1.movieid AS movieid1, rate2.movieid AS movieid2,1-abs(rate1.avgrating - rate2.avgrating)/5 AS sim FROM avgrate rate1 CROSS JOIN avgrate rate2;
CREATE VIEW notuser AS SELECT DISTINCT(ratings.movieid) AS movieid FROM ratings WHERE ratings.movieid not IN (SELECT (ratings.movieid) FROM ratings WHERE ratings.userid=:v1);
CREATE VIEW userrated AS SELECT (ratings.movieid),ratings.rating FROM ratings WHERE ratings.userid=:v1;
CREATE VIEW possiblepairs AS SELECT notuser.movieid AS movieid1, userrated.movieid AS movieid2,userrated.rating FROM notuser CROSS JOIN userrated;
CREATE VIEW finalresult AS SELECT simtable.movieid1 FROM simtable INNER JOIN possiblepairs ON simtable.movieid1=possiblepairs.movieid1 AND simtable.movieid2=possiblepairs.movieid2 GROUP BY simtable.movieid1 HAVING (sum(simtable.sim*possiblepairs.rating)/sum(simtable.sim)) > 3.9;
CREATE TABLE recommendation AS SELECT  movies.title AS title FROM movies INNER JOIN finalresult ON movies.movieid=finalresult.movieid1;

