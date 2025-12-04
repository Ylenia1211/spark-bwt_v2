package com.randazzo.mario.sparkbwt;

import org.apache.commons.cli.MissingOptionException;
import com.randazzo.mario.sparkbwt.SparkBWTCli;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkBWT {
    public static void main(String[] args) {
        SparkBWTCli cli = new SparkBWTCli();
        SparkSession session = null;

        try {
            // 1) Parse CLI prima di toccare Spark
            cli.setup(args);
            if (cli.isHelp()) {
                cli.printHelpMessage();
                return;
            }

            // 2) Crea la sessione (master viene da spark-submit)
            session = SparkSession.builder()
                    .appName("SparkBWT2")
                    .config("spark.hadoop.validateOutputSpecs", false)
                    .getOrCreate();

            // 3) Verifiche "fail-fast"
            SparkContext sc = session.sparkContext();
            if (sc == null || sc.isStopped()) {
                throw new IllegalStateException("SparkContext non inizializzato o già stoppato.");
            }

            String master = sc.master(); // es. "yarn", "yarn-client", "local[*]"
            if (master == null || !master.toLowerCase().startsWith("yarn")) {
                throw new IllegalStateException("Atteso master YARN, trovato: " + master);
            }

            // (Opzionale) Pretendi deploy-mode=cluster
            SparkConf conf = sc.getConf();
            String deployMode = conf.get("spark.submit.deployMode", "client"); // "cluster" o "client"
            if (!"cluster".equalsIgnoreCase(deployMode)) {
                throw new IllegalStateException("Atteso deploy mode 'cluster', trovato: " + deployMode);
            }

            // Logging più pulito
            sc.setLogLevel("ERROR");

            // 4) Esegui il calcolo
            cli.getBWTCalculatorBuilder()
                    .setSession(session)
                    .build()
                    .run();

        } catch (MissingOptionException e) {
            cli.printHelpMessage("Missing parameter.");
            System.exit(2);
        } catch (ParseException | IllegalArgumentException e) {
            cli.printHelpMessage("Error in parsing command line arguments. " + e.getMessage());
            System.exit(2);
        } catch (Exception e) {
            // Fallisci subito se non sei su YARN / cluster o se SparkContext non c’è
            System.err.println("Errore di inizializzazione: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        } finally {
            if (session != null) session.stop();
        }
    }
}

