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
import com.mongodb.WriteResult;
import com.mongodb.client.model.DBCollectionUpdateOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


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
        /*
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(1, "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})));
        movies.add(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        */
        
        int nb = db.command("db.find('movies')").size();
        System.out.println("NB MOVIES :"+ nb);
       
        //DBObject res = collection.findOne();
        //movies.add(buildMovie(res));
                
        // movies
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        fields.put("date",0);
        DBCursor cursorM = moviesColl.find(new BasicDBObject(),fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies()" + next);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        DBObject querygenre = new BasicDBObject();
        querygenre.put("_id",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        Map<Integer,Genre> genres= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find(querygenre);
         try{
            while (g.hasNext()) {
                DBObject next =g.next();
                Genre g1 = buildGenre(next);
                genres.put(g1.getId(),g1);
            }
        }finally {
            g.close();
        }
        
        // mov_gen
        DBCollection mov_genColl = db.getCollection("movie_genre");
        DBObject query2 = new BasicDBObject();       
        query2.put("mov_id", new BasicDBObject("$in",dicoMovies.keySet()));
        DBObject fields2 = new BasicDBObject();
        fields2.put("_id",0);
        DBCursor mg = mov_genColl.find(query2,fields2);
        
        try{
            while (mg.hasNext()) {
                DBObject next = mg.next();
                int movid = Integer.parseInt((String) next.get("mov_id"));
                int genreId = Integer.parseInt((String) next.get("genre"));
                Movie currentM = dicoMovies.get(movid);
                Genre currentG = genres.get(genreId);
                if (!currentM.getGenres().contains(currentG)){
                    currentM.addGenre(currentG);
                }
            }
        }finally {
            mg.close();
        }
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies() " + dicoMovies.size());
        List<Movie> movies = new LinkedList<Movie>(dicoMovies.values());
        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: write query to retrieve all movies rated by user with id userId
        //List<Movie> movies = new LinkedList<Movie>();
        /*Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2}))); */
        
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject queryRatings = new BasicDBObject();
        queryRatings.put("user_id",userId+"");
        DBObject fields3 = new BasicDBObject();
        fields3.put("_id",0);
        fields3.put("user_id",0);
        fields3.put("rating",0);
        fields3.put("timestamp",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        System.out.println(queryRatings);
        System.out.println(fields3);
        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(queryRatings,fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                String str = (String)next.get("mov_id");
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + str);
                ids.add(str);
            }
        }finally {
            rat.close();
        }
       
         System.out.println(" elemetns" + ids.size());
        for (String id :ids){
            System.out.println(id);
        } 
        
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        fields.put("date",0);
        DBObject body = new BasicDBObject();
        body.put("id", new BasicDBObject("$in",ids));
       
        DBCursor cursorM = moviesColl.find(body,fields );
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + body );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies()" + next);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        DBObject querygenre = new BasicDBObject();
        querygenre.put("_id",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        Map<Integer,Genre> genres= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find(querygenre);
         try{
            while (g.hasNext()) {
                DBObject next =g.next();
                Genre g1 = buildGenre(next);
                genres.put(g1.getId(),g1);
            }
        }finally {
            g.close();
        }
        
        // mov_gen
        DBCollection mov_genColl = db.getCollection("movie_genre");
        DBObject query2 = new BasicDBObject();
        query2.put("_id",0);
        query2.put("mov_id", new BasicDBObject("$in",dicoMovies.keySet()));
        DBCursor mg = mov_genColl.find(query2);
        
        try{
            while (mg.hasNext()) {
                DBObject next = mg.next();
                int movid = Integer.parseInt((String) next.get("mov_id"));
                int genreId = Integer.parseInt((String) next.get("genre"));
                Movie currentM = dicoMovies.get(movid);
                Genre currentG = genres.get(genreId);
                if (!currentM.getGenres().contains(currentG)){
                    currentM.addGenre(currentG);
                }
            }
        }finally {
            mg.close();
        }
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies() " + dicoMovies.size());
        List<Movie> movies = new LinkedList<Movie>(dicoMovies.values());
        return movies;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        /*Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        */
        
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        fields.put("date",0);
        DBCursor cursorM = moviesColl.find(new BasicDBObject(),fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies()" + next);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        DBObject querygenre = new BasicDBObject();
        querygenre.put("_id",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        Map<Integer,Genre> genres= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find(querygenre);
         try{
            while (g.hasNext()) {
                DBObject next =g.next();
                Genre g1 = buildGenre(next);
                genres.put(g1.getId(),g1);
            }
        }finally {
            g.close();
        }
        
        // mov_gen
        DBCollection mov_genColl = db.getCollection("movie_genre");
        DBObject query2 = new BasicDBObject();       
        query2.put("mov_id", new BasicDBObject("$in",dicoMovies.keySet()));
        DBObject fields2 = new BasicDBObject();
        fields2.put("_id",0);
        DBCursor mg = mov_genColl.find(query2,fields2);
        List<Integer> genresId = new ArrayList<Integer>();
        
        try{
            while (mg.hasNext()) {
                DBObject next = mg.next();
                int movid = Integer.parseInt((String) next.get("mov_id"));
                int genreId = Integer.parseInt((String) next.get("genre"));
                Movie currentM = dicoMovies.get(movid);
                Genre currentG = genres.get(genreId);
                if (!currentM.getGenres().contains(currentG)){
                    currentM.addGenre(currentG);
                }
            }
        }finally {
            mg.close();
        }
        
        
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject queryRatings = new BasicDBObject();
        queryRatings.put("user_id",userId+"");
        DBObject fields3 = new BasicDBObject();
        fields3.put("_id",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        System.out.println(queryRatings);
        System.out.println(fields3);
        List<Rating> ratings = new ArrayList<Rating>();
        DBCursor rat = ratingsColl.find(queryRatings,fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                int movieId = Integer.parseInt((String)next.get("mov_id"));
                int score =Integer.parseInt((String)next.get("rating"));
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + movieId+ "/ " + dicoMovies.get(movieId));
                ratings.add(new Rating( dicoMovies.get(movieId),userId,score));
            }
        }finally {
            rat.close();
        }
        
        return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject query = new BasicDBObject();       
        query.put("mov_id", rating.getMovieId()+"");        
        query.put("user_id", rating.getUserId()+"");
        DBObject update = new BasicDBObject();       
        DBObject data = new BasicDBObject();
        data.put("mov_id",rating.getMovieId()+"");        
        data.put("user_id",rating.getUserId()+"");      
        data.put("rating",rating.getScore()+"");
        update.put("$set",data);
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.addOrUpdateRating()" +query + "\n"+update );
        WriteResult r = ratingsColl.update(query, update, new DBCollectionUpdateOptions().upsert(true));
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.addOrUpdateRating()"+ r.toString());
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
    
    private Movie buildMovie(DBObject movie){
        Movie m = null;
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.buildMovie()" + movie);
        if (movie != null){
            m = new Movie(
                            Integer.parseInt((String) movie.get("id")),
                            (String)movie.get("title")
                         );
        }
        return m;
    }
    
    private List<Integer> getAllMovieIds(List<Movie> listM){
       List<Integer> res = new ArrayList<Integer>();
        int i =0;
        for (Movie m :listM){
            res.add(m.getId());
            i++;
        }
        
        return res;
    }
    
    private Genre buildGenre(DBObject genre){
        Genre g = null;
        if (genre != null){
            g = new Genre(
                            Integer.parseInt((String) genre.get("id")),
                            (String)genre.get("name")
                         );
        }
        return g;
    }
    
  
    
}
