package com.camillepradel.movierecommender.model;

import java.util.ArrayList;
import java.util.List;

public class Movie {

    private int id;
    private String title;
    private List<Genre> genres;

    public Movie(int id, String title, List<Genre> genres) {
        this.id = id;
        this.title = title;
        this.genres = genres;
    }
    
    public Movie(int id,String title){
        this.id = id;
        this.title = title;
        this.genres = new ArrayList<Genre>();
    }

    public int getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }

    public List<Genre> getGenres() {
        return this.genres;
    }
    
    public void addGenre(Genre g){
        this.genres.add(g);
    }
}
