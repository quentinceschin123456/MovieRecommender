package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
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
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic( "neo4j", "neo4j"));
    }

    @Override
    public List<Movie> getAllMovies() {
        HashMap<Integer, Genre> allGenres = new HashMap<Integer,Genre>();
        List<Movie> allMovies = new LinkedList<Movie>();
        
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
                    
                    // Ajoute du film si inexistant
                    if(!checkIfMovieExists(allMovies, movieId)) {
                        List<Genre> movieGenres = new LinkedList<>();
                        movieGenres.add(allGenres.get(genreId));
                        allMovies.add(new Movie(movieId, movieName, movieGenres));
                    } else {
                        // Si existant il faut ajouter un autre genre
                        int movieIndex = getMovieContainingId(allMovies, movieId);
                        if (movieIndex > -1) {
                            Movie aMovie = allMovies.get(movieIndex);
                            aMovie.getGenres().add(allGenres.get(genreId));
                        } else {
                            System.err.println("Impossible mais pourquoi pas : Le film n'a pas �t� trouv�...");
                        }
                    }
                }
                return allMovies;
            });
        }
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: write query to retrieve all movies rated by user with id userId
        /*List<Movie> movies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));
        return movies;*/
        
        HashMap<Integer, Genre> allGenres = new HashMap<Integer,Genre>();
        List<Movie> allMovies = new LinkedList<Movie>();
        
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
                    if(!checkIfMovieExists(allMovies, movieId)) {
                        List<Genre> movieGenres = new LinkedList<>();
                        movieGenres.add(allGenres.get(genreId));
                        allMovies.add(new Movie(movieId, movieName, movieGenres));
                    } else {
                        // Si existant il faut ajouter un autre genre
                        int movieIndex = getMovieContainingId(allMovies, movieId);
                        if (movieIndex > -1) {
                            Movie aMovie = allMovies.get(movieIndex);
                            aMovie.getGenres().add(allGenres.get(genreId));
                        } else {
                            System.err.println("Impossible mais pourquoi pas : Le film n'a pas �t� trouv�...");
                        }
                    }
                }
                return allMovies;
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
                    
                    // Des genres dans le film qui est ajout� dans Rating avec l'ID user et la note associ�
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

    private Boolean checkIfMovieExists(List<Movie> allMovies, Integer movieId) {
        for (Movie aMovie : allMovies) {
            if (aMovie.getId() == movieId) {
                return true;
            }
        }
        return false;
    }
    
    private Integer getMovieContainingId(List<Movie> allMovies, Integer movieId) {
        for (int i = 0; i <= allMovies.size(); i++) {
            if (allMovies.get(i).getId() == movieId) {
                return i;
            }
        }
        return -1;
    }
}
