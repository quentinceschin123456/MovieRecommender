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
    
    private static int[] iterations = {10,100,1000};
    
    public static void main(String[]args){
        ArrayList<ArrayList<Double>> getMoviesTimer = new ArrayList<>();
        
        Double totalTime = 0.0;
        
        
        // Test : GetAllMovies
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForGetAllMovies(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process GetAllMovies : " + totalTime + "s");
        
        // Test : GetMoviesRatedByUser
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForGetMoviesRatedByUser(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process GetMoviesRatedByUser : " + totalTime + "s");
        
        // Test : GetRatingsFromUser
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForGetRatingsFromUser(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process GetRatingsFromUser : " + totalTime + "s");
        
        // Test : AddOrUpdateRatingRequest
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            //ArrayList<Double> aRow = getTimerForAddOrUpdateRatingRequest(iterations[iter]);
            ArrayList<Double> aRow = new ArrayList<Double>();
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process AddOrUpdateRatingRequest : " + totalTime + "s");
        
        // Test : ProcessRecommendationsForUser1
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForProcessRecommendationsForUser1(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process ProcessRecommendationsForUser1 : " + totalTime + "s");
        
        // Test : ProcessRecommendationsForUser2
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForProcessRecommendationsForUser2(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process ProcessRecommendationsForUser2 : " + totalTime + "s");
        
        // Test : ProcessRecommendationsForUser3
        totalTime = 0.0;
        for(int iter = 0; iter < iterations.length; iter++) {
            ArrayList<Double> aRow = getTimerForProcessRecommendationsForUser3(iterations[iter]);
            getMoviesTimer.add(aRow);
            totalTime += getTotalTimeForRow(aRow);
        }
        System.out.println("Time to process ProcessRecommendationsForUser3 : " + totalTime + "s");
        
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
            
                int interations = line % iterations.length;
                int functionType = (int)Math.floor(line / iterations.length);

                switch(functionType){
                    case 0: message = "GetAllMovies"; break;
                    case 1: message = "GetMoviesRatedByUser"; break;
                    case 2: message = "GetRatingsFromUser"; break;
                    case 3: message = "AddOrUpdateRatingRequest"; break;
                    case 4: message = "ProcessRecommendationsForUser1"; break;
                    case 5: message = "ProcessRecommendationsForUser2"; break;
                    case 6: message = "ProcessRecommendationsForUser3"; break;
                }

                message += " ("+ iterations[interations] +"): \t";

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
            
            int interations = line % iterations.length;
            int functionType = (int)Math.floor(line / iterations.length);

            switch(functionType){
                case 0: message = "GetAllMovies"; break;
                case 1: message = "GetMoviesRatedByUser"; break;
                case 2: message = "GetRatingsFromUser"; break;
                case 3: message = "AddOrUpdateRatingRequest"; break;
                case 4: message = "ProcessRecommendationsForUser1"; break;
                case 5: message = "ProcessRecommendationsForUser2"; break;
                case 6: message = "ProcessRecommendationsForUser3"; break;
            }
            
            message += " ("+ iterations[interations] +"): \t";
            
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
    
    private static Double getTotalTimeForRow(ArrayList<Double> timersInRow) {
        Double totalTime = 0.0;
        for(double aTime : timersInRow) {
            totalTime += aTime;
        }
        return totalTime;
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
