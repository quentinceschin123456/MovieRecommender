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
        long startTime = 0;
        double endTime = 0.0;
        
        // Test : GetAllMovies
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForGetAllMovies(10));   // [0]
        getMoviesTimer.add(getTimerForGetAllMovies(100));  // [1]
        getMoviesTimer.add(getTimerForGetAllMovies(1000)); // [2]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process GetAllMovies : " + endTime + "s");
        
        // Test : GetMoviesRatedByUser
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(10));   // [3]
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(100));  // [4]
        getMoviesTimer.add(getTimerForGetMoviesRatedByUser(1000)); // [5]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process GetMoviesRatedByUser : " + endTime + "s");
        
        // Test : GetRatingsFromUser
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForGetRatingsFromUser(10));   // [6]
        getMoviesTimer.add(getTimerForGetRatingsFromUser(100));  // [7]
        getMoviesTimer.add(getTimerForGetRatingsFromUser(1000)); // [8]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process GetRatingsFromUser : " + endTime + "s");
        
        // Test : AddOrUpdateRatingRequest
        startTime = System.nanoTime();
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(10));   // [9]
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(100));  // [10]
        //getMoviesTimer.add(getTimerForAddOrUpdateRatingRequest(1000)); // [11]
        getMoviesTimer.add(new ArrayList<Double>());
        getMoviesTimer.add(new ArrayList<Double>());
        getMoviesTimer.add(new ArrayList<Double>());
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process AddOrUpdateRatingRequest : " + endTime + "s");
        
        // Test : ProcessRecommendationsForUser1
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(10));   // [12]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(100));  // [13]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser1(1000)); // [14]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process ProcessRecommendationsForUser1 : " + endTime + "s");
        
        // Test : ProcessRecommendationsForUser2
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(10));   // [15]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(100));  // [16]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser2(1000)); // [17]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process ProcessRecommendationsForUser2 : " + endTime + "s");
        
        // Test : ProcessRecommendationsForUser3
        startTime = System.nanoTime();
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(10));   // [18]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(100));  // [19]
        getMoviesTimer.add(getTimerForProcessRecommendationsForUser3(1000)); // [20]
        endTime = (double) (System.nanoTime() - startTime) / 1000000000.;
        System.out.println("Time to process ProcessRecommendationsForUser3 : " + endTime + "s");
        
        ArrayList<Double> timersAverage = getAvgForTimers(getMoviesTimer);
        
        doGenerateReport(timersAverage);
        doPrintTimerAvg(timersAverage);
    }
    
    private static void doGenerateReport(ArrayList<Double> avgs) {
        PrintWriter writer = null;
        String csvLine = "";
        try {
            writer = new PrintWriter("GenerateReport.csv");
            for(int line = 0; line < avgs.size(); line++) {
                String message = "";
            
                int interations = line % 3;
                int functionType = (int)Math.floor(line / 3);

                switch(functionType){
                    case 0: message = "GetAllMovies"; break;
                    case 1: message = "GetMoviesRatedByUser"; break;
                    case 2: message = "GetRatingsFromUser"; break;
                    case 3: message = "AddOrUpdateRatingRequest"; break;
                    case 4: message = "ProcessRecommendationsForUser1"; break;
                    case 5: message = "ProcessRecommendationsForUser2"; break;
                    case 6: message = "ProcessRecommendationsForUser3"; break;
                }

                switch(interations){
                    case 0: message += " (10): \t"; break;
                    case 1: message += " (100): \t"; break;
                    case 2: message += " (1000): \t"; break;
                }

                message += ";" + avgs.get(line);
                writer.println(message);
            }
            
            writer.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TestAllFonctions.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            writer.close();
        }
    }
    
    private static void doPrintTimerAvg(ArrayList<Double> avgs) {
        System.out.println("==================== Test results : ====================");
        for(int line=0; line < avgs.size(); line++) {
            String message = "";
            
            int interations = line % 3;
            int functionType = (int)Math.floor(line / 3);

            switch(functionType){
                case 0: message = "GetAllMovies"; break;
                case 1: message = "GetMoviesRatedByUser"; break;
                case 2: message = "GetRatingsFromUser"; break;
                case 3: message = "AddOrUpdateRatingRequest"; break;
                case 4: message = "ProcessRecommendationsForUser1"; break;
                case 5: message = "ProcessRecommendationsForUser2"; break;
                case 6: message = "ProcessRecommendationsForUser3"; break;
            }
            
            switch(interations){
                case 0: message += " (10)  : "; break;
                case 1: message += " (100) : "; break;
                case 2: message += " (1000): "; break;
            }
            
            message += avgs.get(line);
            System.out.println(message);
        }
    }
    
    private static ArrayList<Double> getAvgForTimers(ArrayList<ArrayList<Double>> datas) {
        ArrayList<Double> timerAvg = new ArrayList<>();
        
        for(ArrayList<Double> aLine : datas) {
            Double avg = 0.0;
            for (Double aValue : aLine) {
                if (aValue == null) break;
                avg += aValue;
            }
            avg = (avg / aLine.size());
            timerAvg.add(avg);
        }
        return timerAvg;
    }
    
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
