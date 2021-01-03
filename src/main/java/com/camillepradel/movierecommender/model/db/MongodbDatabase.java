package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
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
import java.util.Iterator;


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
                //System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getAllMovies()" + next);
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
                //System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + str);
                ids.add(str);
            }
        }finally {
            rat.close();
        }
       
        /* System.out.println(" elemetns" + ids.size());
        for (String id :ids){
            System.out.println(id);
        } 
        */
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
                int score = (int)next.get("rating");
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
        data.put("rating",rating.getScore());
        update.put("$set",data);
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
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.processRecommendationsForUser()");
        //String titlePrefix;
        if (processingMode == 0) {
            //titlePrefix = "0_";
            recommandationWithOneUser(recommendations,userId);
        } else if (processingMode == 1) {
            //titlePrefix = "1_";
            recommandationWithFiveUser(recommendations,userId);
        } else if (processingMode == 2) {
            //titlePrefix = "2_";
            recommandationWithRatings(recommendations,userId);
        } else {
            //titlePrefix = "default_";
            
        }
        /*
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        */
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
    
    /*private List<Integer> getAllMovieIds(List<Movie> listM){
       List<Integer> res = new ArrayList<Integer>();
        int i =0;
        for (Movie m :listM){
            res.add(m.getId());
            i++;
        }
        
        return res;
    }
    */
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

    private void recommandationWithOneUser(List<Rating> recommendations, int userId) {
        // on r��cup�re la liste de film de cet utilisateur
        
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
        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(queryRatings,fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                String str = (String)next.get("mov_id");
                //System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + str);
                ids.add(str);
            }
        }finally {
            rat.close();
        }
       // Pour chaque utilisateur, on stock celui qui a le max de film en commun
       
        
       DBObject match = new BasicDBObject();
       List<DBObject> and = new ArrayList<DBObject>();
       and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids)));
       and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
    
       match.put("$and", and);
       
        DBObject sum = new BasicDBObject();
        sum.put("$sum",1);
        
       Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb",sum );
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1)),
        (DBObject) new BasicDBObject("$limit", 1)
        );
       for (DBObject d : request){
           System.out.println(d.toString()+ ",");
       }
        Cursor aggrCursor =  ratingsColl.aggregate(request,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
        
       List<String> idList = new ArrayList<String>();
       while (aggrCursor.hasNext()){
           String id = aggrCursor.next().get("_id").toString(); 
           idList.add(id);
       }
       
        String user_id = idList.get(0);
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() user_id :" + user_id);
       // on prend tout les films poir cet utilisateurs qui ne sont pas dans la liste du premier
        
        
        DBObject matchMovies = new BasicDBObject();
       List<DBObject> andM = new ArrayList<DBObject>();
       andM.add(new BasicDBObject("mov_id", new BasicDBObject("$nin",ids)));
       andM.add(new BasicDBObject("user_id",new BasicDBObject("$in", idList)));
    
       matchMovies.put("$and", and);
        
        Map<String, Object> dbObjIdMapM = new HashMap<String, Object>();
        dbObjIdMapM.put("_id", "$mov_id");
        dbObjIdMapM.put("avg_rate", new BasicDBObject( "$avg" , "$rating") );
        
        List<DBObject> requestMovies  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMapM)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("avg_rate", -1))
        );
        
        
        Cursor cursorM = ratingsColl.aggregate(requestMovies,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
         
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() TEST  " +(String)next.get("_id") );
                int movieId = Integer.parseInt((String)next.get("_id"));
                
                recommendations.add(new Rating( getMovieById(movieId) ,userId));
            }
        }
       finally {
            cursorM.close();
        }
         
         for (Rating r : recommendations){
             System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() resultats " + r.getMovie().getTitle()+ " score:" + r.getScore());
         }
        
    }

    private void recommandationWithFiveUser(List<Rating> recommendations, int userId) {
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
        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(queryRatings,fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                String str = (String)next.get("mov_id");
                //System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + str);
                ids.add(str);
            }
        }finally {
            rat.close();
        }
       // Pour chaque utilisateur, on stock celui qui a le max de film en commun
       
        
       DBObject match = new BasicDBObject();
       List<DBObject> and = new ArrayList<DBObject>();
       and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids)));
       and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
    
       match.put("$and", and);
       
        DBObject sum = new BasicDBObject();
        sum.put("$sum",1);
        
       Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb",sum );
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1)),
        (DBObject) new BasicDBObject("$limit", 5)
        );
       for (DBObject d : request){
           System.out.println(d.toString()+ ",");
       }
        Cursor aggrCursor =  ratingsColl.aggregate(request,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
        
       List<String> idList = new ArrayList<String>();
       while (aggrCursor.hasNext()){
           String id = aggrCursor.next().get("_id").toString(); 
           idList.add(id);
       }
       
        String user_id = idList.get(0);
        System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() user_id :" + user_id);
       // on prend tout les films poir cet utilisateurs qui ne sont pas dans la liste du premier
       
       
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        DBObject body = new BasicDBObject();
        body.put("mov_id", new BasicDBObject("$nin",ids));
        body.put("user_id",user_id);
        
        Cursor cursorM = ratingsColl.find(body,fields).sort(new BasicDBObject("rating",-1) ) ;
         
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                int movieId = Integer.parseInt((String)next.get("mov_id"));
                int score = (int)next.get("rating");
                
                recommendations.add(new Rating( getMovieById(movieId) ,userId,score));
            }
        }
       finally {
            cursorM.close();
        }
         
         for (Rating r : recommendations){
             System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() resultats " + r.getMovie().getTitle()+ " score:" + r.getScore());
         }
        
    }
    
    private void recommandationWithRatings(List<Rating> recommendations, int userId) {
         
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject queryRatings = new BasicDBObject();
        queryRatings.put("user_id",userId+"");
        DBObject fields3 = new BasicDBObject();
        fields3.put("_id",0);
        fields3.put("user_id",0);
        fields3.put("timestamp",0);
        // query3.put("id", new BasicDBObject("$in",genresId));
        //List<Genre> genres = new ArrayList<Genre>();
        Map<String,String> ids = new HashMap<String,String>();
        DBCursor rat = ratingsColl.find(queryRatings,fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                String str = (String)next.get("mov_id");
                String note = next.get("rating").toString();
                //System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.getMoviesRatedByUser()" + str);
                ids.put(str,note);
            }
        }finally {
            rat.close();
        }
         
        DBObject match = new BasicDBObject();
        List<DBObject> and = new ArrayList<DBObject>();
       and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids.keySet())));
       and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
    
       match.put("$and", and);
       
        DBObject sum = new BasicDBObject();
        sum.put("$sum",1);
      
        Map<String, Object> push = new HashMap<String, Object>();
        push.put("mov_id", "$mov_id");        
        push.put("rating", "$rating");

                
       Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb",sum );
        dbObjIdMap.put("movies",new BasicDBObject( "$push" , new BasicDBObject(push) ) );
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1))
        );
       for (DBObject d : request){
           System.out.println(d.toString()+ ",");
       }
        Cursor aggrCursor =  ratingsColl.aggregate(request,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
        
       Map<String,DBObject> idList = new HashMap<String,DBObject>();
       
       String idUser = null;
       int max = -1 ;
       while (aggrCursor.hasNext()){
           DBObject next = aggrCursor.next();
           String id = next.get("_id").toString(); 
           
           int simValue = computeSim(ids,(BasicDBList)next.get("movies"));
           if (simValue > max ){
               idUser = id ;
           }
       }
       
        
          
        DBObject matchMovies = new BasicDBObject();
       List<DBObject> andM = new ArrayList<DBObject>();
       andM.add(new BasicDBObject("mov_id", new BasicDBObject("$nin",ids)));
       andM.add(new BasicDBObject("user_id",new BasicDBObject("$in", idList)));
    
       matchMovies.put("$and", and);
        
        Map<String, Object> dbObjIdMapM = new HashMap<String, Object>();
        dbObjIdMapM.put("_id", "$mov_id");
        dbObjIdMapM.put("avg_rate", new BasicDBObject( "$avg" , "$rating") );
        
        List<DBObject> requestMovies  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMapM)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("avg_rate", -1))
        );
        
        
        Cursor cursorM = ratingsColl.aggregate(requestMovies,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
         
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() TEST  " +(String)next.get("_id") );
                int movieId = Integer.parseInt((String)next.get("_id"));
                
                recommendations.add(new Rating( getMovieById(movieId) ,userId));
            }
        }
       finally {
            cursorM.close();
        }
         
         for (Rating r : recommendations){
             System.out.println("com.camillepradel.movierecommender.model.db.MongodbDatabase.recommandationWithOneUser() resultats " + r.getMovie().getTitle()+ " score:" + r.getScore());
         }
         
        
    }

    private Movie getMovieById(int movieId) {
        
        Movie m = null;
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        fields.put("date",0);
        DBCursor cursorM = moviesColl.find(new BasicDBObject("id",movieId+ ""),fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                m = buildMovie(next);
               
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
        Map<Integer,Genre> genresMap= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find(querygenre);
         try{
            while (g.hasNext()) {
                DBObject next =g.next();
                Genre g1 = buildGenre(next);
                genresMap.put(g1.getId(), g1);
            }
        }finally {
            g.close();
        }
        
        // mov_gen
        DBCollection mov_genColl = db.getCollection("movie_genre");
        DBObject query2 = new BasicDBObject();       
        query2.put("mov_id", m.getId()+"");
        DBObject fields2 = new BasicDBObject();
        fields2.put("_id",0);
        DBCursor mg = mov_genColl.find(query2,fields2);
        
        try{
            while (mg.hasNext()) {
                DBObject next = mg.next();
                int movid = Integer.parseInt((String) next.get("mov_id"));
                int genreId = Integer.parseInt((String) next.get("genre"));
                
                Genre currentG = genresMap.get(genreId);
                if (!m.getGenres().contains(currentG)){
                    m.addGenre(currentG);
                }
            }
        }finally {
            mg.close();
        }
        
        return m;
    }

    private int computeSim(Map<String, String> ids, BasicDBList idList) {
        int res = 0;
        for (Iterator<Object> iterator = idList.iterator(); iterator.hasNext();) {
            DBObject next = (DBObject)iterator.next();
            String mov_id = next.get("mov_id").toString();
            int rating = Integer.parseInt(next.get("rating").toString());
            
            res += 4 - Math.abs(Integer.parseInt(ids.get(mov_id).toString()) - rating);
        }
        
        
        return res;
    }
}
