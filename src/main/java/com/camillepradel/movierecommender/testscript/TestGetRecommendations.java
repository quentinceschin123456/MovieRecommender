package com.camillepradel.movierecommender.testscript;

import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestGetRecommendations {

    public static void main(String[] args) {

        /*String urlStart = "http://localhost:8080/MovieRecommender/recommendations?user_id=";
        int nbIterations = 1000;
        int userId = 0;
        long startTime = System.nanoTime();

        for (int i = 0; i < nbIterations; i++) {

            URL u;
            InputStream is = null;
            DataInputStream dis;

            try {
                u = new URL(urlStart + userId);
                is = u.openStream();
                dis = new DataInputStream(new BufferedInputStream(is));
                while (dis.readLine() != null) {
                }
                System.out.println(i + "/" + nbIterations);
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

        long endTime = System.nanoTime();
        double time = (double) (endTime - startTime) / 1000000000.;
        System.out.println("Time to process " + nbIterations + " requests in one thread: " + time + "s");
*/
        TestRequest.addOrUpdateRatingRequest(new Rating(new Movie(0, "test"), 0, 0));
       /*DataInputStream out = TestRequest.processRecommendationsForUser2(0);
        String response = null;
        try {
            while (null != ((response = out.readLine()))) {
                System.out.println(response);
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
*/
}
        
    
    
    
}
