package SparkWorks;

//Apache Spark Includes
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class Global_Queries{
    private static Dataset<Row> df;
    private static SparkSession sparkSession;

    /*
        Function: Queries
        Author: Gerardo Castro Mata
        Editors:
        Input: String, Sparksession
        Output: None
        Summary: Constructor that sets the private data field Dataset<Row>
        and sparkSession. Afterwards, creates the temporary view of the csv file that will be looked at.
    */
    public Global_Queries(String filepath, Sparksession sparksession)
    {
        this.sparkSession = sparksession;
        this.df = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);
        df.createOrReplaceTempView("GLOBAL");
    }

    /*
       Function: verifyState
       Author: Gerardo Castro Mata
       Input: String
       Output: boolean
       Summary:
    */
    public static boolean verifyState(String state) throws Exception{
        File f = new File("/home/gera7/Downloads/Global_Names.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br  new BufferedReader(fr);
        String read;
    }
}