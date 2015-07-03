/*
 * FraudScoreExample.java
 *
 * Copyright (C) 2010-2015 by Revolution Analytics Inc.
 *
 * This program is licensed to you under the terms of Version 2.0 of the
 * Apache License. This program is distributed WITHOUT
 * ANY EXPRESS OR IMPLIED WARRANTY, INCLUDING THOSE OF NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Please refer to the
 * Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) for more details.
 *
 */
package com.revo.deployr.rbroker.example;

import com.revo.deployr.client.*;
import com.revo.deployr.client.params.*;
import com.revo.deployr.client.data.*;
import com.revo.deployr.client.auth.*;
import com.revo.deployr.client.auth.basic.*;
import com.revo.deployr.client.factory.*;
import com.revo.deployr.client.broker.*;
import com.revo.deployr.client.broker.config.*;
import com.revo.deployr.client.broker.task.*;
import com.revo.deployr.client.broker.options.*;
import com.revo.deployr.client.broker.app.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;

/*
 * Simple code example demonstrating the use of DeployR
 * as a real-time, R analytics scoring engine powered 
 * by the RBroker Framework.
 */
public class FraudScoreExample
    implements RTaskListener, RBrokerListener, RTaskAppSimulator {

    public static void main(String[] args) {

        log.info("Using DeployR Endpoint @ " + endpoint);
        new FraudScoreExample();
    }

    public FraudScoreExample() {

        RBroker rBroker = null;

        try {

            /*
             * Create and configure an RBroker instance.
             *
             * Configuration includes:
             *
             * 1. Number of R sessions in the pool.
             * 2. Basic authentication credentials.
             * 3. Per R session workspace pre-initialization.
             */

            int poolSize =
                Integer.valueOf(System.getProperty("example-pool-size"));

            RAuthentication rAuth =
                new RBasicAuthentication(System.getProperty("username"),
                                         System.getProperty("password"));

            PoolCreationOptions poolOptions = new PoolCreationOptions();
            PoolPreloadOptions preloadOptions = 
                                    new PoolPreloadOptions();
            preloadOptions.filename = System.getProperty("repository-model");
            preloadOptions.directory = System.getProperty("repository-directory");
            preloadOptions.author = System.getProperty("username");
            poolOptions.preloadWorkspace = preloadOptions;
            boolean allowSelfSigned = Boolean.valueOf(
                    System.getProperty("allow.SelfSignedSSLCert"));

            /*
             * Ensure releaseGridResources property is enabled
             * so server-side grid resource management will auto
             * clear old pool resources before creating a new
             * pool on each run of FraudScoreExample.
             */
            poolOptions.releaseGridResources = true;


            PooledBrokerConfig brokerConfig =
                new PooledBrokerConfig(endpoint,
                                       rAuth,
                                       poolSize,
                                       poolOptions);
            brokerConfig.allowSelfSignedSSLCert = allowSelfSigned;

            /*
             * Create an instance of a Pooled Task Runtime.
             */
            rBroker = RBrokerFactory.pooledTaskBroker(brokerConfig);
            log.info("RBroker pool [ " + rBroker.maxConcurrency() +
                                         " R Sessions ] created.");

            /*
             * Register asynchronous callback listeners for RTask
             * and RBroker events.
             */
            rBroker.addTaskListener(this);
            rBroker.addBrokerListener(this);

            /*
             * Launch the RTaskAppSimulator. This RBroker will
             * execute the simulator defined by (RTaskAppSimulator)this.
             */
            rBroker.simulateApp(this);

            /*
             * Latch acts as a simple block on the main thread of
             * FraudScoreExample until the simulation completes.
             */
            latch.await();

            log.info("Simulation completes.");

        } catch(Exception ex) {
            log.warn("Unexpected error: ex=" + ex);
        } finally {
            try{
                if(rBroker != null) {
                    rBroker.shutdown();
                    log.info("RBroker released, exiting.");
                }
            } catch(Exception bex) {
                log.warn("RBroker shutdown: ex=" + bex);
            }
        }
    }

    /*
     * RTaskAppSimulator Interface method: simulateApp.
     *
     * Method implementation simply needs to submit one
     * or more RTask on the provided instance of RBroker.
     *
     * The RTask results and any errors are handled by the
     * asynchronous callback listeners registered with the
     * instance of RBroker.
     */
    public void simulateApp(RBroker rBroker) {

        log.info("Simulation begins.");

        for(int t = 0; t < taskSize; t++) {
            try {
                RTask task = buildTask();
                rBroker.submit(task);
            } catch(Exception tex) {
                log.warn("Task submit error: ex=" + tex);
                latch.countDown();
            }
        }
    }

    /*
     * RTaskListener Interface method: onTaskCompleted.
     *
     * An asynchronous callback listener on RTask completion.
     */
    public void onTaskCompleted(RTask rTask, RTaskResult rTaskResult) {

        if(rTaskResult.isSuccess()) {

            /*
             * Extract fraud score from RTaskResult.
             */
            List<RData> rObjects = rTaskResult.getGeneratedObjects();
            double score = ((RNumeric)rObjects.get(0)).getValue();

            log.info("Task [ " + (taskSize - latch.getCount() + 1) +
                                        " ] scored " + score + ".");
        }

        /*
         * Decrement latch so FraudScoreExample main thread can
         * release resources and exit once all tasks on the
         * simulation have completed.
         */
        latch.countDown();
    }

    /*
     * RTaskListener Interface method: onTaskError.
     *
     * An asynchronous callback listener on RTask error.
     */
    public void onTaskError(RTask rTask, Throwable throwable) {
        log.info("onTaskError: task=" + rTask +
                                        " cause=" + throwable);
        /*
         * Decrement latch so FraudScoreExample main thread can
         * release resources and exit once all tasks on the
         * simulation have completed.
         */
        latch.countDown();
    }

    /*
     * RBrokerListener Interface method: onRuntimeError.
     *
     * An asynchronous callback listener on RBroker runtime errors.
     */
    public void onRuntimeError(Throwable throwable) {
        log.info("onRuntimeError: cause=" + throwable);
    }

    /*
     * RBrokerListener Interface method: onRuntimeStats.
     *
     * An asynchronous callback listener on RBroker statistics events.
     */
    public void onRuntimeStats(RBrokerRuntimeStats stats,
                                        int maxConcurrency) {
        /*
         * Log/profile runtime RBroker stats as desired...
         */
    }

    /*
     * Support method: buildTask
     *
     * Builds an instance of RTask based on a repository-managed
     * R script that takes a set of 3 required inputs: bal, trans, credit.
     */
    public RTask buildTask() {

        RTask rTask = null;

        try {

            PooledTaskOptions taskOptions = new PooledTaskOptions();
            taskOptions.routputs = Arrays.asList("x");

            int bal = Math.abs((new Random()).nextInt() % 25000);
            int trans = Math.abs((new Random()).nextInt() % 100);
            int credit = Math.abs((new Random()).nextInt() % 75);

            taskOptions.rinputs = Arrays.asList(
                (RData) RDataFactory.createNumeric("bal", bal),
                (RData) RDataFactory.createNumeric("trans", trans),
                (RData) RDataFactory.createNumeric("credit", credit)
            );

            rTask = RTaskFactory.pooledTask(
                                System.getProperty("repository-script"),
                                System.getProperty("repository-directory"),
                                System.getProperty("username"),
                                null, taskOptions);

        } catch(Exception ex) {
            log.warn("FraudScoreExample build task error: ex=" + ex);
        }

        return rTask;
    }

    private static Logger log = Logger.getLogger(FraudScoreExample.class);
    private static String endpoint =
                    System.getProperty("connection.protocol") +
                    System.getProperty("connection.endpoint");
    private RAuthentication rAuth = new RBasicAuthentication(
                    System.getProperty("username"),
                    System.getProperty("password"));
    private static int taskSize =
        Integer.valueOf(System.getProperty("simulate-task-size"));
    private static CountDownLatch latch = new CountDownLatch(taskSize);

}
