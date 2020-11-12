package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class MongodbDatabase extends AbstractDatabase {
    
    
    MongoClient mongoClient;
    DB db;
    
    public MongodbDatabase(){
        mongoClient= new MongoClient( new MongoClientURI("mongodb://localhost:27017"));
        db = mongoClient.getDB("movie_recommender");
    }
    
    
    @Override
    public List<Movie> getAllMovies() {
        // TODO: write query to retrieve all movies from DB
        List<Movie> movies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(1, "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})));
        movies.add(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        
        int nb = db.command("db.find('movies')").size();
        System.out.println("NB MOVIES :"+ nb);
        DBCollection collection = db.getCollection("movies");
       
        //DBObject res = collection.findOne();
        //movies.add(buildMovie(res));
                
        // movies
        DBCollection moviesColl = db.getCollection("movies");
        DBObject query = new BasicDBObject();
        query.put("_id",0);
        query.put("date",0);
        DBCursor m = moviesColl.find(query);

        // mov_gen
        DBCollection mov_genColl = db.getCollection("movi_genre");
        DBObject query2 = new BasicDBObject();
        query2.put("_id",0);
        DBCursor mg = mov_genColl.find(query2);
        // genres
        DBCollection genresColl = db.getCollection("genres");
        DBObject query3 = new BasicDBObject();
        query3.put("_id",0);
        query3.put("date",0);
        DBCursor g = genresColl.find(query3);
        // mapping
        
        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: write query to retrieve all movies rated by user with id userId
        List<Movie> movies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        List<Rating> ratings = new LinkedList<Rating>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }  
    
    private Movie buildMovie(DBObject movie,DBObject m_g,DBObject genres){
        Movie m = null;
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.buildMovie()" + movie.toMap().toString());
        if (movie != null){
            m = new Movie(
                            Integer.parseInt((String) movie.get("id")),
                            (String)movie.get("title"),
                    null
                            );
        }
        return m;
    }
}
