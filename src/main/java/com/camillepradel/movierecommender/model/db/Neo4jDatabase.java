package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import static org.neo4j.driver.Values.parameters;

public class Neo4jDatabase extends AbstractDatabase {

    private final Driver driver;
    private Transaction tx;
    
    public Neo4jDatabase() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic( "oreo4j", "oreo4j"));
    }

    @Override
    public List<Movie> getAllMovies() {
        HashMap<Integer, Genre> allGenres = new HashMap<Integer,Genre>();
        HashMap<Integer, Movie> allMovies = new HashMap<Integer,Movie>();
        
        
        /// Todo : Change LinkedList
        try ( Session session = driver.session() )
        {
            return session.readTransaction(tx -> {
                Result rs = tx.run(
                    "MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                    "RETURN m.id, m.title, g.id, g.name"
                );
                while(rs.hasNext()) {
                    Record aRec = rs.next();
                    Integer movieId = aRec.get(0).asInt();
                    String movieName = aRec.get(1).asString();
                    Integer genreId = aRec.get(2).asInt();
                    String genreTitle = aRec.get(3).asString();
                    
                    // Ajout du genre si non-existant
                    if (!allGenres.containsKey(genreId)) {
                        allGenres.put(genreId, new Genre(genreId, genreTitle));
                    }
                    
                    Movie aMovie = allMovies.get(movieId);
                    // Ajoute du film si inexistant
                    if(aMovie == null) {
                        List<Genre> movieGenres = new LinkedList<>();
                        movieGenres.add(allGenres.get(genreId));
                        allMovies.put(movieId, new Movie(movieId, movieName, movieGenres));
                    } else {
                        aMovie.getGenres().add(allGenres.get(genreId));
                    }
                }
                return new LinkedList<Movie>(allMovies.values());
            });
        }
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        HashMap<Integer, Genre> allGenres = new HashMap<Integer,Genre>();
        HashMap<Integer, Movie> allMovies = new HashMap<Integer,Movie>();
        
        /// Todo : Change LinkedList
        try ( Session session = driver.session() )
        {
            return session.readTransaction(tx -> {
                Result rs = tx.run(
                    "MATCH (u1:User {id:$userId})-[:RATED]->(m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                    "RETURN m.id, m.title, g.id, g.name",
                    parameters("userId", userId)
                );
                while(rs.hasNext()) {
                    Record aRec = rs.next();
                    Integer movieId = aRec.get(0).asInt();
                    String movieName = aRec.get(1).asString();
                    Integer genreId = aRec.get(2).asInt();
                    String genreTitle = aRec.get(3).asString();
                    
                    // Ajout du genre si non-existant
                    if (!allGenres.containsKey(genreId)) {
                        allGenres.put(genreId, new Genre(genreId, genreTitle));
                    }
                    
                    // Ajoute du film si inexistant
                    Movie aMovie = allMovies.get(movieId);
                    // Ajoute du film si inexistant
                    if(aMovie == null) {
                        List<Genre> movieGenres = new LinkedList<>();
                        movieGenres.add(allGenres.get(genreId));
                        allMovies.put(movieId, new Movie(movieId, movieName, movieGenres));
                    } else {
                        aMovie.getGenres().add(allGenres.get(genreId));
                    }
                }
                return new LinkedList<Movie>(allMovies.values());
            });
        }
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        /*List<Rating> ratings = new LinkedList<Rating>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        return ratings;*/
        
        HashMap<Integer, Genre> allGenres = getAllGenres();
        List<Movie> allMovies = new LinkedList<Movie>();
        List<Rating> allRatings = new LinkedList<Rating>();
        
        /// Todo : Change LinkedList
        try ( Session session = driver.session() )
        {
            return session.readTransaction(tx -> {
                Result rs = tx.run(
                    "MATCH (u1:User {id:$userId})-[r:RATED]->(m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                    "RETURN m.id, m.title, collect(g.id), r.note",
                    parameters("userId", userId)
                );
                while(rs.hasNext()) {
                    Record aRec = rs.next();
                    Integer movieId = aRec.get(0).asInt();
                    String movieName = aRec.get(1).asString();
                    String genreIds = aRec.get(2).toString();
                    Integer rating = aRec.get(3).asInt();
                    
                    // Ajout du genre pour le film correspondant
                    String IdsExtracted = genreIds.substring(genreIds.indexOf("[")+1, genreIds.indexOf("]"));
                    List<Genre> movieGenres = new LinkedList<>();
                    for (String genreId : IdsExtracted.split(", ")) {
                        movieGenres.add(allGenres.get(Integer.parseInt(genreId)));
                    }
                    
                    // Des genres dans le film qui est ajouté dans Rating avec l'ID user et la note associé
                    allRatings.add(new Rating(new Movie(movieId, movieName, movieGenres), userId, rating));
                }
                return allRatings;
            });
        }
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
        
        try ( Session session = driver.session() )
        {
            session.writeTransaction( new TransactionWork<Void>() {
                @Override
                public Void execute( Transaction tx ) {
                    tx.run(
                        "MATCH (u:User{id:$userId})\n" +
                        "MATCH (m:Movie{id:$movieId})\n" +
                        "MERGE (u)-[r:RATED]->(m)\n" +
                        "ON CREATE SET r.note = $rate\n" +
                        "ON MATCH SET r.note = $rate",
                        parameters(
                            "userId", rating.getUserId(),
                            "movieId", rating.getMovieId(),
                            "rate", rating.getScore()
                        )
                    );
                    return null;
                }
            });
        }
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        
        switch(processingMode) {
            case 1 : {
                HashMap<Integer, Genre> allGenres = getAllGenres();
                List<Movie> allMovies = new LinkedList<Movie>();
                List<Rating> recommendations = new LinkedList<Rating>();

                /// Todo : Change LinkedList
                try ( Session session = driver.session() )
                {
                    return session.readTransaction(tx -> {
                        Result rs = tx.run(
                            "MATCH (target_user:User {id :$userId})-[:RATED]->(m:Movie) <-[:RATED]-(other_user:User)\n" +
                            "WITH other_user, count(distinct m.title) AS num_common_movies, target_user\n" +
                            "ORDER BY num_common_movies DESC\n" +
                            "LIMIT 1\n" +
                            "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                            "WHERE NOT (target_user)-[:RATED]->(m2)\n" +
                            "RETURN m2.id as rec_movie_id, m2.title AS rec_movie_title, collect(g.id) as genre_ids, rat_other_user.note AS rating, other_user.id AS watched_by\n" +
                            "ORDER BY rat_other_user.note DESC",
                            parameters("userId", userId)
                        );
                        while(rs.hasNext()) {
                            Record aRec = rs.next();
                            Integer movieId = aRec.get(0).asInt();
                            String movieName = aRec.get(1).asString();
                            String genreIds = aRec.get(2).toString();
                            Integer rating = aRec.get(3).asInt();
                            Integer userWhoWatched = aRec.get(4).asInt();

                            // Ajout du genre pour le film correspondant
                            String IdsExtracted = genreIds.substring(genreIds.indexOf("[")+1, genreIds.indexOf("]"));
                            List<Genre> movieGenres = new LinkedList<>();
                            for (String genreId : IdsExtracted.split(", ")) {
                                movieGenres.add(allGenres.get(Integer.parseInt(genreId)));
                            }

                            // Des genres dans le film qui est ajouté dans Rating avec l'ID user et la note associé
                            recommendations.add(new Rating(new Movie(movieId, movieName, movieGenres), userWhoWatched, rating));
                        }
                        return recommendations;
                    });
                } catch (Exception e) {
                    System.err.println("Error : " + e.getMessage());
                    break;
                }
            }
            case 2 : {
                HashMap<Integer, Genre> allGenres = getAllGenres();
                List<Movie> allMovies = new LinkedList<Movie>();
                List<Rating> recommendations = new LinkedList<Rating>();

                /// Todo : Change LinkedList
                try ( Session session = driver.session() )
                {
                    return session.readTransaction(tx -> {
                        Result rs = tx.run(
                            "MATCH (target_user:User {id :$userId})-[:RATED]->(m:Movie)<-[:RATED]-(other_user:User)\n" +
                            "WITH other_user, count(distinct m.title) AS num_common_movies, target_user\n" +
                            "ORDER BY num_common_movies DESC\n" +
                            "LIMIT 5\n" +
                            "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)\n" +
                            "WHERE NOT (target_user)-[:RATED]->(m2)\n" +
                            "RETURN m2.id as rec_movie_id, m2.title AS rec_movie_title, avg(rat_other_user.note) AS rating, count(other_user.id) AS nb_watched\n" +
                            "ORDER BY rating DESC, nb_watched DESC",
                            parameters("userId", userId)
                        );
                        while(rs.hasNext()) {
                            Record aRec = rs.next();
                            Integer movieId = aRec.get(0).asInt();
                            String movieName = aRec.get(1).asString();

                            // Des genres dans le film qui est ajouté dans Rating avec l'ID user et la note associé
                            recommendations.add(new Rating(new Movie(movieId, movieName, null), 0, 0));
                        }
                        return recommendations;
                    });
                } catch (Exception e) {
                    System.err.println("Error : " + e.getMessage());
                    break;
                }
            }
            case 3 : {
                HashMap<Integer, Genre> allGenres = getAllGenres();
                List<Movie> allMovies = new LinkedList<Movie>();
                List<Rating> recommendations = new LinkedList<Rating>();

                /// Todo : Change LinkedList
                try ( Session session = driver.session() )
                {
                    return session.readTransaction(tx -> {
                        Result rs = tx.run(
                            "MATCH (u:User {id :$userId})-[r:RATED]->(:Movie)\n" +
                            "WITH u as target_user, count(r) as nbTgRate\n" +
                            "MATCH (target_user)-[r_target_user:RATED]->(m:Movie)<-[r_other_user:RATED]-(other_user:User)\n" +
                            "WITH count(distinct m.title) AS num_common_movies, target_user, other_user, avg(4 - abs(r_target_user.note - r_other_user.note)) as degreeDeSimilarite, nbTgRate as ratesNb\n" +
                            "WHERE num_common_movies >= (ratesNb / 2)\n" +
                            "RETURN other_user.id\n" +
                            "ORDER BY degreeDeSimilarite DESC, num_common_movies DESC\n" +
                            "LIMIT 1",
                            parameters("userId", userId)
                        );
                        
                        Integer otherUserId = null;
                        
                        if (rs.hasNext()) {
                            Record aRec = rs.next();
                            otherUserId = aRec.get(0).asInt();
                        }
                        
                        if (otherUserId != null) {
                            rs = tx.run(
                                "MATCH (other_user:User {id :$otherUserId})-[rat_other_user:RATED]->(m2:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                                "WHERE NOT (:User {id: $userId})-[:RATED]->(m2)\n" +
                                "RETURN m2.id as rec_movie_id, m2.title AS rec_movie_title, collect(g.id) as genre_ids, rat_other_user.note AS rating, other_user.id AS watched_by\n" +
                                "ORDER BY rat_other_user.note DESC",
                                parameters(
                                        "otherUserId", otherUserId,
                                        "userId", userId
                                )
                            );
                            
                            while(rs.hasNext()) {
                                Record aRec = rs.next();
                                Integer movieId = aRec.get(0).asInt();
                                String movieName = aRec.get(1).asString();

                                // Des genres dans le film qui est ajouté dans Rating avec l'ID user et la note associé
                                recommendations.add(new Rating(new Movie(movieId, movieName, null), 0, 0));
                            }
                        }
                        return recommendations;
                    });
                } catch (Exception e) {
                    System.err.println("Error : " + e.getMessage());
                    break;
                }
            }
            
            default :
                return new LinkedList<Rating>();
        }
        return new LinkedList<Rating>();
    }
    
    /* ----- HELPERS ----- */
    
    private HashMap<Integer, Genre> getAllGenres() {
        try ( Session session = driver.session() )
        {
            return session.readTransaction(tx -> {
                HashMap<Integer, Genre> allGenres = new HashMap<Integer, Genre>();
                Result rs = tx.run(
                    "MATCH (g:Genre)\n" +
                    "RETURN g.id, g.name"
                );
                while(rs.hasNext()) {
                    Record aRec = rs.next();
                    Integer id = aRec.get(0).asInt();
                    String name = aRec.get(1).asString();
                    Genre g = new Genre(id, name);
                    allGenres.put(id, g);
                }
                return allGenres;
            });
        }
    }

    /*private Boolean checkIfMovieExists(List<Movie> allMovies, Integer movieId) {
        for (Movie aMovie : allMovies) {
            if (aMovie.getId() == movieId) {
                return true;
            }
        }
        return false;
    }*/
    
    private Integer getMovieContainingId(List<Movie> allMovies, Integer movieId) {
        for (int i = 0; i <= allMovies.size(); i++) {
            if (allMovies.get(i).getId() == movieId) {
                return i;
            }
        }
        return -1;
    }
}
