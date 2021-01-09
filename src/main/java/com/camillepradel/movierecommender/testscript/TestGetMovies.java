package com.camillepradel.movierecommender.testscript;

import com.camillepradel.movierecommender.utils.TimerUtils;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class TestGetMovies {

    public static String executeTestGetMoviesFor100RandomUsers() {
        String urlStart = "http://localhost:8080/MovieRecommender/movies?user_id=";
        int nbIterations = 100;
        double time = 0;
        
        for (int i = 0; i < nbIterations; i++) {
            
            System.out.println(i + "/" + nbIterations);

            time += TimerUtils.getResponseTime(new Runnable() {
                @Override
                public void run() {
                    
                    int userId = (int)(Math.random() * 200); //200 : nb utilisateur

                    InputStream is = null;
                    DataInputStream dis;

                    try {
                        URL u = new URL(urlStart + userId);
                        is = u.openStream();
                        dis = new DataInputStream(new BufferedInputStream(is));
                        while (dis.readLine() != null) {
                        }
                    } catch (MalformedURLException mue) {
                        System.err.println("Ouch - a MalformedURLException happened.");
                        mue.printStackTrace();
                        System.exit(2);
                    } catch (IOException ioe) {
                        System.err.println("Oops- an IOException happened.");
                        ioe.printStackTrace();
                        System.exit(3);
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ioe) {
                        }
                    }
                }
            });
        }
        String result = "Time to process " + nbIterations + " requests in one thread: " + time + "s";
        System.out.println(result);
        return result;
    }
    
 
}
