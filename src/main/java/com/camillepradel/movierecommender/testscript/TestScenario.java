/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.camillepradel.movierecommender.testscript;

import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.camillepradel.movierecommender.utils.TimerUtils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author AdminEtu
 */
public class TestScenario {
    static int  idMovieReco = -1 ; 
    public static void main(String[]args){
        int nbUserForTest = 1000;
        ArrayList<ArrayList<Double>> getMoviesTimer = new ArrayList<>();
        
            ArrayList<Double> timer = new ArrayList<Double>();
            for (int i = 0; i<nbUserForTest ; i++){
               
            int userId = (int)(Math.random() * 200); //200 : nb utilisateur
            
            // regarde la liste des films
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getAllMovies();
                }
            }));
            // après ne rien avoir trouver,l'utilisateur demande une recommenadation
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser1(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation du film
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get recommandation 
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser1(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation 
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get movies by user
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getMoviesRatedByUser(userId);
                }
            }));
            }
            getMoviesTimer.add(timer);
            
            
            timer = new ArrayList<Double>();
            for (int i = 0; i<nbUserForTest ; i++){
               
            int userId = (int)(Math.random() * 200); //200 : nb utilisateur
            
            // regarde la liste des films
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getAllMovies();
                }
            }));
            // après ne rien avoir trouver,l'utilisateur demande une recommenadation
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser2(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation du film
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get recommandation 
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser2(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation 
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get movies by user
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getMoviesRatedByUser(userId);
                }
            }));
            }
            getMoviesTimer.add(timer);
            
            
            timer = new ArrayList<Double>();
            for (int i = 0; i<nbUserForTest ; i++){
               
            int userId = (int)(Math.random() * 200); //200 : nb utilisateur
            
            // regarde la liste des films
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getAllMovies();
                }
            }));
            // après ne rien avoir trouver,l'utilisateur demande une recommenadation
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser3(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation du film
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get recommandation 
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    DataInputStream out = TestRequest.processRecommendationsForUser3(userId);
                   try {
             String response = null ;
             String html = "";
            while (null != ((response = out.readLine()))) {
                html += response ;
            }
            Pattern pat = Pattern.compile("(id:\\d*)");
            Matcher mat = pat.matcher(html);
            
            if (mat.find()){
                idMovieReco = Integer.parseInt(mat.group(1).split(":")[1]);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(TestGetRecommendations.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
                    
                }
            }));
            // notation 
             timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    int mark = (int)(Math.random() * 6);
                    Rating rate = new Rating(new Movie(idMovieReco , null), mark);
                    TestRequest.addOrUpdateRatingRequest(rate);
                }
            }));
            // get movies by user
            timer.add(TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    TestRequest.getMoviesRatedByUser(userId);
                }
            }));
            }
            getMoviesTimer.add(timer);
            
           ArrayList<Double> liste = new ArrayList<Double>();
           for ( ArrayList<Double> line : getMoviesTimer) {
               double v =0;
               for( Double d: line){
                  // System.out.println("test " + d);
                   v +=d;
               }
               liste.add(v/nbUserForTest);
               //System.out.println("");
           }
           for(Double d : liste){
               System.out.println(d);
           }
        //TestAllFonctions.doGenerateReport(getMoviesTimer);
    }
    
    
}
