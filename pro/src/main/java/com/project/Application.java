package com.project;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class Application {
    public static void main(String[] args) throws InterruptedException {

        System.out.println("hej");

        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to db")
                .master("local")
                .getOrCreate();

        // Read csv file
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");

        // Add new column full_name and as a value concat last_name and first_name
        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

        // Filter by col where is at least one digit and order by last_name
        df = df.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc());

        // Create connection to postgresql and insert data into existing data
        String dbConnectionUrl = "jdbc:postgresql://localhost/course_data";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "Semafor4!");

        // show data
        df.show();

        // write to postgresql server
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);

    }
}
