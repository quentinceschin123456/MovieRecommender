/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.camillepradel.movierecommender.testscript;

import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.camillepradel.movierecommender.utils.TimerUtils;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cette classe est dédiée à l'execution successive des endpoints de l'application.
 * @author AdminEtu
 */
public class TestAllFonctions {
    
    public static void main(String[]args){
        ArrayList<ArrayList<Double>> getMoviesTimer = new ArrayList<>();
        
        // Test : GetAllMovies
        getMoviesTimer.add(getTimerForGetAllMovies(10));   // [0]
        getMoviesTimer.add(getTimerForGetAllMovies(100));  // [1]
        getMoviesTimer.add(getTimerForGetAllMovies(1000)); // [2]
        System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(0).get(0) + "s");
        
        // Test : GetMoviesRatedByUser
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(10));   // [3]
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(100));  // [4]
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(1000)); // [5]
        System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(3).get(0) + "s");
        
        // Test : GetRatingsFromUser
        getMoviesTimer.add(getTimerForGetRatingsFromUser(10));   // [6]
        getMoviesTimer.add(getTimerForGetRatingsFromUser(100));  // [7]
        getMoviesTimer.add(getTimerForGetRatingsFromUser(1000)); // [8]
        System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(6).get(0) + "s");
        
        // Test : AddOrUpdateRatingRequest
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(10));   // [9]
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(100));  // [10]
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(1000)); // [11]
        getMoviesTimer.add(new ArrayList<Double>());
        getMoviesTimer.add(new ArrayList<Double>());
        getMoviesTimer.add(new ArrayList<Double>());
        //System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(9).get(0) + "s");
        
        // Test : ProcessRecommendationsForUser1
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(10));   // [12]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(100));  // [13]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(1000)); // [14]
        System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(9).get(0) + "s");
        
        // Test : ProcessRecommendationsForUser2
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(10));   // [15]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(100));  // [16]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(1000)); // [17]
        //System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(9).get(0) + "s");
        
        // Test : ProcessRecommendationsForUser3
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(10));   // [18]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(100));  // [19]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(1000)); // [20]
        //System.out.println("Time to process requests in one thread: " + getMoviesTimer.get(9).get(0) + "s");
        
        doGenerateReport(getMoviesTimer);
    }
    
    private static void doGenerateReport(ArrayList<ArrayList<Double>> datas) {
        PrintWriter writer = null;
        String csvLine = "";
        try {
            writer = new PrintWriter("GenerateReport.csv");
            for(int line = 0; line < datas.size(); line++) {
                ///TODO
            }
            
            writer.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TestAllFonctions.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            writer.close();
        }
    }
    
    // getTimerForProcessRecommendationsForUser1
    
    private static ArrayList<Double> getTimerForGetAllMovies(int count) {
        ArrayList<Double> timer = new ArrayList<>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getAllMovies();
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForGetMoviesRatedByUser(int count) {
        ArrayList<Double> timer = new ArrayList<>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200); //200 : nb utilisateur
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getMoviesRatedByUser(userId);
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForGetRatingsFromUser(int count) {
        ArrayList<Double> timer = new ArrayList<>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200); //200 : nb utilisateur
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getRatingsFromUser(userId);
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForAddOrUpdateRatingRequest(int count) {
        ArrayList<Double> timer = new ArrayList<Double>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200)+1;  // [1..200] : nb utilisateur
            int userRate = (int)(Math.random() * 6);    // [0..5  ] : minMax rate utilisateur
            int movieId = (int)(Math.random() * 400)+1; // [1..400] : nb movie
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.addOrUpdateRatingRequest(new Rating(new Movie(movieId, null), userId, userRate));
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForProcessRecommendationsForUser1(int count) {
        ArrayList<Double> timer = new ArrayList<Double>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200)+1;  // [1..200] : nb utilisateur
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.processRecommendationsForUser1(userId);
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForProcessRecommendationsForUser2(int count) {
        ArrayList<Double> timer = new ArrayList<Double>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200)+1;  // [1..200] : nb utilisateur
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.processRecommendationsForUser2(userId);
                }
            }));
        }
        return timer;
    }
    
    private static ArrayList<Double> getTimerForProcessRecommendationsForUser3(int count) {
        ArrayList<Double> timer = new ArrayList<Double>();
        
        for(int iteration = 0; iteration < count; iteration++) {
            
            int userId = (int)(Math.random() * 200)+1;  // [1..200] : nb utilisateur
            
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.processRecommendationsForUser3(userId);
                }
            }));
        }
        return timer;
    }
}
