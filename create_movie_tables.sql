-- Databricks notebook source
DROP TABLE IF EXISTS star;
DROP TABLE IF EXISTS movie_to_star;
DROP TABLE IF EXISTS movie;
DROP TABLE IF EXISTS movie_to_genre;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS movie_to_director;
DROP TABLE IF EXISTS director;
DROP TABLE IF EXISTS title;

CREATE TABLE title (
    id      INTEGER NOT NULL,
    name    STRING
);

CREATE TABLE genre (
    id      INTEGER NOT NULL,
    name    STRING  
);

CREATE TABLE star (
    id      INTEGER NOT NULL,
    name    STRING 
);

CREATE TABLE director (
    id      INTEGER NOT NULL,
    name    STRING 
);

CREATE TABLE movie_to_star (
    star_id     INTEGER, 
    movie_id    INTEGER
);

CREATE TABLE movie (
    id          INTEGER,
    title_id    INTEGER,
    rating      FLOAT,
    one_line    STRING,
    votes       INTEGER,
    start_year  INTEGER,
    end_year    INTEGER
);

CREATE TABLE movie_to_director (
    director_id INTEGER,
    movie_id    INTEGER
);

CREATE TABLE movie_to_genre (
    movie_id    INTEGER,
    genre_id    INTEGER
);
