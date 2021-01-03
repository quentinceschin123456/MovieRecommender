/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.camillepradel.movierecommender.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author AdminEtu
 */
public abstract class TimerUtils {
    public static double getResponseTime(Runnable runnable) {
        long startTime = System.nanoTime();
        Thread aThread = new Thread(() -> {
            try {
                runnable.run();
            }
            catch (Exception e){
                System.err.println(e);
            }
        });
        
        try {
            aThread.start();
            aThread.join();
            return (double) (System.nanoTime() - startTime) / 1000000000.;
        } catch (InterruptedException ex) {
            Logger.getLogger(TimerUtils.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }
}
