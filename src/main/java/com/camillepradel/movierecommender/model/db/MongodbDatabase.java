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
        // movies
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject("_id",0);
        fields.put("date",0);
        DBCursor cursorM = moviesColl.find(new BasicDBObject(),fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        Map<Integer,Genre> genres = new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find( new BasicDBObject("_id",0));
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
        List<Movie> movies = new LinkedList<Movie>(dicoMovies.values());
        return movies;
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        DBCollection ratingsColl = db.getCollection("ratings");
        
        DBObject fields3 = new BasicDBObject("_id",0);
        fields3.put("user_id",0);
        fields3.put("rating",0);
        fields3.put("timestamp",0);
       
        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(new BasicDBObject("user_id",userId+""),fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                ids.add((String)next.get("mov_id"));
            }
        }finally {
            rat.close();
        }
      
         
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject("_id",0);
        fields.put("date",0);
        DBObject body = new BasicDBObject("id", new BasicDBObject("$in",ids));
        
        DBCursor cursorM = moviesColl.find(body,fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        Map<Integer,Genre> genres= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find(new BasicDBObject("_id",0));
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
        DBObject query2 = new BasicDBObject("_id",0);
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
        Map<Integer,Movie> dicoMovies = new HashMap<Integer,Movie>();
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject("_id",0);
        fields.put("date",0);
        DBCursor cursorM = moviesColl.find(new BasicDBObject(),fields );
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                Movie m = buildMovie(next);
                dicoMovies.put(m.getId(),m);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            cursorM.close();
        }
        
       
        // genres
        DBCollection genresColl = db.getCollection("genres");
        Map<Integer,Genre> genres= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find( new BasicDBObject("_id",0));
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
        DBObject query2 = new BasicDBObject("mov_id", new BasicDBObject("$in",dicoMovies.keySet()));       
        DBObject fields2 = new BasicDBObject("_id",0);
        
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
        
        
        DBCollection ratingsColl = db.getCollection("ratings");
        List<Rating> ratings = new ArrayList<Rating>();
        DBCursor rat = ratingsColl.find(new BasicDBObject("user_id",userId+""),new BasicDBObject("_id",0));
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
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject query = new BasicDBObject("mov_id", rating.getMovieId()+"");
        query.put("user_id", rating.getUserId()+"");
        
        DBObject data = new BasicDBObject("mov_id",rating.getMovieId()+"");
        data.put("user_id",rating.getUserId()+"");      
        data.put("rating",rating.getScore());
        
        WriteResult r = ratingsColl.update(query, new BasicDBObject("$set",data), new DBCollectionUpdateOptions().upsert(true));
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        List<Rating> recommendations = new LinkedList<Rating>();
        
        switch(processingMode){
            case 0:
                recommandationWithOneUser(recommendations,userId);
                break;
            case 1:
                recommandationWithFiveUser(recommendations,userId);
                break;
            case 2:
                 recommandationWithRatings(recommendations,userId);
                break;
            default:
                 recommandationWithOneUser(recommendations,userId);
        }
        
        return recommendations;
    }  
    
    private Movie buildMovie(DBObject movie){
        Movie m = null;
        if (movie != null){
            m = new Movie(
                            Integer.parseInt((String) movie.get("id")),
                            (String)movie.get("title")
                         );
        }
        return m;
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

    private void recommandationWithOneUser(List<Rating> recommendations, int userId) {
      
        DBCollection ratingsColl = db.getCollection("ratings");
       
        DBObject fields3 = new BasicDBObject("_id",0);
        fields3.put("user_id",0);
        fields3.put("rating",0);
        fields3.put("timestamp",0);       

        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(new BasicDBObject("user_id",userId+""),fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                ids.add((String)next.get("mov_id"));
            }
        }finally {
            rat.close();
        }
       
      
        List<DBObject> and = new ArrayList<DBObject>();
        and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids)));
        and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
        DBObject match = new BasicDBObject("$and", and);
       
       
        
        Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb",new BasicDBObject("$sum",1 ));
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1)),
        (DBObject) new BasicDBObject("$limit", 1)
        );
       
       
        Cursor aggrCursor =  ratingsColl.aggregate(request,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
        
       List<String> idList = new ArrayList<String>();
       while (aggrCursor.hasNext()){
           String id = aggrCursor.next().get("_id").toString(); 
           idList.add(id);
       }
       if (idList.size() > 0 ){
           
       
        String user_id = idList.get(0);
       
        
        
        
        
         
        DBObject fields = new BasicDBObject();
        fields.put("_id",0);
        DBObject body = new BasicDBObject();
        body.put("mov_id", new BasicDBObject("$nin",ids));
        body.put("user_id",user_id);
        
        Cursor cursorM = ratingsColl.find(body,fields).sort(new BasicDBObject("rating",-1) ).limit(10) ;
         
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
        
        
        
        
       
        }
    }

    private void recommandationWithFiveUser(List<Rating> recommendations, int userId) {
       DBCollection ratingsColl = db.getCollection("ratings");
        DBObject fields3 = new BasicDBObject("_id",0);
        fields3.put("user_id",0);
        fields3.put("rating",0);
        fields3.put("timestamp",0);
       
        List<String> ids = new ArrayList<String>();
        DBCursor rat = ratingsColl.find(new BasicDBObject("user_id",userId+""),fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                ids.add((String)next.get("mov_id"));
            }
        }finally {
            rat.close();
        }
      
       
       List<DBObject> and = new ArrayList<DBObject>();
       and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids)));
       and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
       DBObject match = new BasicDBObject("$and", and);
      
       Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb", new BasicDBObject("$sum",1) );
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1)),
        (DBObject) new BasicDBObject("$limit", 5)
        );
      
        Cursor aggrCursor =  ratingsColl.aggregate(request,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
        
       List<String> idList = new ArrayList<String>();
       while (aggrCursor.hasNext()){
           String id = aggrCursor.next().get("_id").toString(); 
           idList.add(id);
       }
       
       
       
        DBObject matchMovies = new BasicDBObject();
       List<DBObject> andM = new ArrayList<DBObject>();
       andM.add(new BasicDBObject("mov_id", new BasicDBObject("$nin",ids)));
       andM.add(new BasicDBObject("user_id",new BasicDBObject("$in",idList)));
    
       matchMovies.put("$and", and);
        
        Map<String, Object> dbObjIdMapM = new HashMap<String, Object>();
        dbObjIdMapM.put("_id", "$mov_id");
        dbObjIdMapM.put("avg_rate", new BasicDBObject( "$avg" , "$rating") );
        
        List<DBObject> requestMovies  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMapM)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("avg_rate", -1)),
        (DBObject) new BasicDBObject("$limit", 10)
        );
        
        
        Cursor cursorM = ratingsColl.aggregate(requestMovies,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
         
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                int movieId = Integer.parseInt((String)next.get("_id"));
                
                recommendations.add(new Rating( getMovieById(movieId) ,userId));
            }
        }
       finally {
            cursorM.close();
        }
        
    }
    
    private void recommandationWithRatings(List<Rating> recommendations, int userId) {
         
        DBCollection ratingsColl = db.getCollection("ratings");
        DBObject fields3 = new BasicDBObject("_id",0);
        fields3.put("user_id",0);
        fields3.put("timestamp",0);
      
        Map<String,String> ids = new HashMap<String,String>();
        DBCursor rat = ratingsColl.find(new BasicDBObject("user_id",userId+""),fields3);
         try{
            while (rat.hasNext()) {
                DBObject next = rat.next();
                String str = (String)next.get("mov_id");
                String note = next.get("rating").toString();
                ids.put(str,note);
            }
        }finally {
            rat.close();
        }
         
        
        List<DBObject> and = new ArrayList<DBObject>();
       and.add(new BasicDBObject("mov_id", new BasicDBObject("$in",ids.keySet())));
       and.add(new BasicDBObject("user_id",new BasicDBObject("$ne", userId+"")));
    
       DBObject match = new BasicDBObject("$and", and);
      
        Map<String, Object> push = new HashMap<String, Object>();
        push.put("mov_id", "$mov_id");        
        push.put("rating", "$rating");

                
       Map<String, Object> dbObjIdMap = new HashMap<String, Object>();
        dbObjIdMap.put("_id", "$user_id");
        dbObjIdMap.put("nb",new BasicDBObject("$sum",1) );
        dbObjIdMap.put("movies",new BasicDBObject( "$push" , new BasicDBObject(push) ) );
       
       List<DBObject> request  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",match),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMap)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("nb", -1))
        );
      
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
       
        
          
        
       List<DBObject> andM = new ArrayList<DBObject>();
       andM.add(new BasicDBObject("mov_id", new BasicDBObject("$nin",ids)));
       andM.add(new BasicDBObject("user_id",new BasicDBObject("$in", idList)));
    
       DBObject matchMovies = new BasicDBObject("$and", and);
     
        
        Map<String, Object> dbObjIdMapM = new HashMap<String, Object>();
        dbObjIdMapM.put("_id", "$mov_id");
        dbObjIdMapM.put("avg_rate", new BasicDBObject( "$avg" , "$rating") );
        
        List<DBObject> requestMovies  = Arrays.asList(
        (DBObject) new BasicDBObject("$match",matchMovies),
        (DBObject) new BasicDBObject("$group", new BasicDBObject(dbObjIdMapM)),
        (DBObject) new BasicDBObject("$sort", new BasicDBObject("avg_rate", -1)),
        (DBObject) new BasicDBObject("$limit", 10)
        );
        
        
        Cursor cursorM = ratingsColl.aggregate(requestMovies,AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build());
         
        try{
            while (cursorM.hasNext()) {
                DBObject next = cursorM.next();
                int movieId = Integer.parseInt((String)next.get("_id"));
                
                recommendations.add(new Rating( getMovieById(movieId) ,userId));
            }
        }
       finally {
            cursorM.close();
        }
         
    }

    private Movie getMovieById(int movieId) {
        
        Movie m = null;
        DBCollection moviesColl = db.getCollection("movies");
        DBObject fields = new BasicDBObject("_id",0);
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
       
        Map<Integer,Genre> genresMap= new HashMap<Integer,Genre>();
        DBCursor g = genresColl.find( new BasicDBObject("_id",0));
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
       
        DBCursor mg = mov_genColl.find(new BasicDBObject("mov_id", m.getId()+""), new BasicDBObject("_id",0));
        
        try{
            while (mg.hasNext()) {
                DBObject next = mg.next();
                int movid = Integer.parseInt((String) next.get("mov_id"));
                int genreId = Integer.parseInt((String) next.get("genre"));
                
                Genre currentG = genresMap.get(genreId);
                if (movid == m.getId() && !m.getGenres().contains(currentG)){
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
