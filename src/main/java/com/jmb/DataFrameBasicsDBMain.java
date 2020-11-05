package com.jmb;

import com.jmb.persistence.SalesTransactionsDbManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

import static com.jmb.persistence.SalesTransactionsDbManager.DB_URL;
import static org.apache.spark.sql.functions.lit;

public class DataFrameBasicsDBMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFrameBasicsDBMain.class);
    private static final String SPARK_FILES_FORMAT = "csv";


    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        DataFrameBasicsDBMain app = new DataFrameBasicsDBMain();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {

        LOGGER.info("Bootstrapping DB Resources");
        //Start Embedded DB Server from within this application's process by connecting (Derby)
        SalesTransactionsDbManager salesTransactionsDbManager = new SalesTransactionsDbManager();
        salesTransactionsDbManager.startDB();

        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("DataFrameBasicsDB")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a DataFrame
        Dataset<Row> df = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load("src/main/resources/spark-data/electronic-card-transactions.csv");

        //Apply a Transformation - Add a new Column to the dataframe for final transactions values
        Dataset<Row> results = df.withColumn("total_units_value",
                lit(df.col("Data_value").multiply(df.col("magnitude"))));


        //Persist to DB through Spark JDBC methods
        Properties prop = salesTransactionsDbManager.buildDBProperties();
        df.write().mode(SaveMode.Overwrite)
                .jdbc(DB_URL,
                        SalesTransactionsDbManager.TABLE_NAME, prop);

        //Print results in DB
        salesTransactionsDbManager.displayTableResults(15);

        //Create CSV Output file
        df.write().format(SPARK_FILES_FORMAT)
                .save("src/main/resources/spark-data/enriched_transactions");

        //Print first 15 rows to output
        results.show(15);
    }
}
