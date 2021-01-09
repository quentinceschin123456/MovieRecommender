/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.camillepradel.movierecommender.testscript;

import com.camillepradel.movierecommender.model.Rating;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 *
 * @author AdminEtu
 */
public class TestRequest {
    
    
       public static DataInputStream getAllMovies() {
        InputStream is = null;

        try {
            URL u = new URL("http://localhost:8080/MovieRecommender/movies");
            is = u.openStream();
            return new DataInputStream(new BufferedInputStream(is));
        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        }
        return null;
    }
       
    public static DataInputStream getMoviesRatedByUser(int userId) {
        InputStream is = null;

        try {
            URL u = new URL("http://localhost:8080/MovieRecommender/movies?user_id="+userId);
            is = u.openStream();
            return new DataInputStream(new BufferedInputStream(is));
        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        }
        return null;
    }
    
    public static DataInputStream getRatingsFromUser(int userId) {
        InputStream is = null;

        try {
            URL u = new URL("http://localhost:8080/MovieRecommender/movieratings?user_id="+userId);
            is = u.openStream();
            return new DataInputStream(new BufferedInputStream(is));
        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            try {
                is.close();
            } catch (IOException ioex) {
                System.err.println("Ouch - An exception occured while closing open stream.");
            }
        }
        return null;
    }
    
    public static DataInputStream addOrUpdateRatingRequest(Rating rating){
        DataInputStream out = null;
        InputStream is = null;
        String url = "http://localhost:8080/MovieRecommender/movieratings";
        try {
            URL u = new URL(url);
            URLConnection con = u.openConnection();
            HttpURLConnection http = (HttpURLConnection)con;
            http.setRequestMethod("POST"); // PUT is another valid option
            http.setDoOutput(true);
            
            Map<String,String> arguments = new HashMap<>();
            arguments.put("userID", rating.getUserId()+"");
            arguments.put("movieId", rating.getMovieId()+"");
            arguments.put("score", rating.getScore()+"");
            StringJoiner sj = new StringJoiner("&");
            for(Map.Entry<String,String> entry : arguments.entrySet())
                sj.add(URLEncoder.encode(entry.getKey(), "UTF-8") + "=" 
                     + URLEncoder.encode(entry.getValue(), "UTF-8"));
            byte[] params = sj.toString().getBytes(StandardCharsets.UTF_8);
            int length = params.length;
            http.setFixedLengthStreamingMode(length);
            http.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            http.connect();
            OutputStream os = con.getOutputStream();
            os.write(params);
            os.flush();
            os.close();
            
            //out = new DataInputStream (con.getInputStream());

        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
           
            System.exit(2);
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            System.exit(3);
        } 
        return out;
    }
    
    public static DataInputStream processRecommendationsForUser1(int userId){
        DataInputStream out = null;
        InputStream is = null;
        String url = "http://localhost:8080/MovieRecommender/recommendations";
        url += "?user_id=" +userId; 
        url += "&processing_mode=0";
        try {
            URL u = new URL(url);
            is = u.openStream();
            out = new DataInputStream(new BufferedInputStream(is));

        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
           
            System.exit(2);
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            System.exit(3);
        } 
        return out;
    }
    public static DataInputStream processRecommendationsForUser2(int userId){
        DataInputStream out = null;
        InputStream is = null;
        String url = "http://localhost:8080/MovieRecommender/recommendations";
        url += "?user_id=" +userId; 
        url += "&processing_mode=1";
        try {
            URL u = new URL(url);
            is = u.openStream();
            out = new DataInputStream(new BufferedInputStream(is));

        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
           
            System.exit(2);
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            System.exit(3);
        } 
        return out;
    }
    public static DataInputStream processRecommendationsForUser3(int userId){
        DataInputStream out = null;
        InputStream is = null;
        String url = "http://localhost:8080/MovieRecommender/recommendations";
        url += "?user_id=" +userId; 
        url += "&processing_mode=3";
        try {
            URL u = new URL(url);
            is = u.openStream();
            out = new DataInputStream(new BufferedInputStream(is));

        } catch (MalformedURLException mue) {
            System.err.println("Ouch - a MalformedURLException happened.");
            mue.printStackTrace();
           
            System.exit(2);
        } catch (IOException ioe) {
            System.err.println("Oops- an IOException happened.");
            ioe.printStackTrace();
            System.exit(3);
        } 
        return out;
    }

}
