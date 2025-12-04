package com.randazzo.mario.sparkbwt

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, FileWriter}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import java.io.OutputStreamWriter

abstract class BWTCalculator(session: SparkSession,
                         verbose: Boolean,
                         inputFilePath: String,
                         outputFilePath: String) extends Serializable {

val LOG: Log = LogFactory.getLog(getClass)
val sc: SparkContext = session.sparkContext
if (!verbose) sc.setLogLevel("Error")



    def run() {
        print("Output file path: ", outputFilePath +"\n")
        print("Input file path: ", inputFilePath +"\n")
        val runtime = Runtime.getRuntime
        runtime.gc() // Suggerito per una misurazione più precisa

        val startMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
        val start: Long = System.currentTimeMillis()

        val result = calculate()



        val elapsed: Long = System.currentTimeMillis() - start
        val endMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
        val usedMem = endMem - startMem

        print(s"Elapsed time: $elapsed ms")
        print(s"Approx. memory used: $usedMem MB")

        // Salvataggio in file di testo
        val timeWriter = new BufferedWriter(new FileWriter(outputFilePath + "/elapsed_time.txt"))

        //val fs = FileSystem.get(sc.hadoopConfiguration)
        //val out = fs.create(new Path(outputFilePath + "/elapsed_time.txt"))

        //val timeWriter = new BufferedWriter(new OutputStreamWriter(out))
        timeWriter.write(s"Elapsed time: $elapsed ms\nMemory used: $usedMem MB")
        timeWriter.close()

        // Salvataggio in CSV per plot/scalabilità
        val csvWriter = new BufferedWriter(new FileWriter(outputFilePath + "/metrics.csv", true))
        csvWriter.write("runtime_ms,memory_mb\n")
        csvWriter.write(s"$elapsed,$usedMem\n")


        val csvPath = new Path(outputFilePath + "/metrics.csv")
        val append = fs.exists(csvPath)
        val outCsv = if (append) fs.append(csvPath) else fs.create(csvPath)
        val csvWriter = new BufferedWriter(new OutputStreamWriter(outCsv))
        if (!append) {
            csvWriter.write("runtime_ms,memory_mb\n")
        }
        csvWriter.write(s"$elapsed,$usedMem\n")
        csvWriter.close()
    }


def calculate(): RDD[String]
}


abstract class BWTCalculator(session: SparkSession,
                             verbose: Boolean,
                             inputFilePath: String,
                             outputFilePath: String) extends Serializable {

    val LOG: Log = LogFactory.getLog(getClass)
    val sc: SparkContext = session.sparkContext
    if (!verbose) sc.setLogLevel("Error")


    def run() {
        val start: Long = System.currentTimeMillis()
        calculate();
        val elapsed: Long = System.currentTimeMillis() - start
        LOG.info("Elapsed time: " + (elapsed) + "ms")

        // Save elapsed time to a file
        val writer = new BufferedWriter(new FileWriter(outputFilePath + "/elapsed_time.txt"))
        writer.write("Elapsed time: " + elapsed + " ms")
        writer.close()
    }

    def calculate()
}

