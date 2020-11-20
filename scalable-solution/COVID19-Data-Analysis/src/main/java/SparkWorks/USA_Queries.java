package SparkWorks;

//Apache Spark Includes
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class USA_Queries {
    private static Dataset<Row> df;
    private static SparkSession sparkSession;

    /*
        Function: Queries
        Author: Dominic Renales
        Editors:
        Input: String, SparkSession
        Output: None
        Summary: Constructor that sets the private data field Dataset<Row>
            and SparkSession. Afterwards, create the temporary view of the
            csv file that will be looked at.

            LINE 1: Initialize SparkSession private data
            LINE 2-7: Load the csv file from the hdfs file path provided
                from SparkMainApp
            LINE 8: Create temporary view that will be used by spark SQL
    */
    public USA_Queries(String filePath, SparkSession sparkSession)
    {
        this.sparkSession = sparkSession;
        this.df = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        df.createOrReplaceTempView("USA");
    }

    /*
        Function: verifyState
        Author: Dominic Renales
        Input: String
        Output: boolean
        Summary: Creates a buffered reader for the hdfs filepath to determine the
            validity of the state name chosen by a user.

            LINE 1-3: Creates the necessary objects to load a buffered reader object
            Line 4: Creates variable to hold value being read from the buffered reader
            Line 6-8: While there are lines being read from the file, if an instance of
                the state string is found in the file return true
            Line 9: Return false if not able to find instance of state
    */
    public static boolean verifyState(String state) throws Exception {
        File f = new File("/home/gera7/Downloads/USA_States.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String read;

        while((read = br.readLine()) != null)
            if(read.toUpperCase().contains(state))
                return true;
        return false;
    }

    /*  OPTION 2 COMPLETE [Query translated from non-scalable PSQL version]
        Function: getNumOfTestsAdministeredByState
        Author: Daniel Murphy
        Editors: Dominic Renales
        Input: None
        Output: None
        Summary: Scans the USA data in the HDFS to print information regarding
            the number of tests conducted in a state

            Line 1: Initialize scanner for input
            Line 2-3: Prompt input and store it
            Line 4-7: While the user is inputting invalid state names, keep
                asking for input.
            Line 8-9: If the state name used is abbreviation length search by
                abbreviation (state) and then print the results
            Line 10-11: Otherwise search by full state name (state_name) and
                then print the results
    */
    public static void getNumOfTestsAdministeredByState() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();

        while(!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }
        if(state.length() == 2)
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state =\'" + state + "\';").show();
        else
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state_name =\'" + state + "\';").show();
    }

    /* OPTION 1 COMPLETE [Query translated from non-scalable PSQL version]
        Function: getNumSpecifiedOutcomesByState
        Author: Daniel Murphy
        Editors: Dominic Renales
        Input: None
        Output: None
        Summary: Prints out the result of the specified outcome on a desired state.
            LINE 1: Initializes Scanner for user input
            LINE 2-3: Prompt the user for input and store it
            LINE 4-7: While the user input does not equal "Positive," "Negative,"
                or "Inconclusive" keep prompting for user input
            LINE 8-13: While the user input is not a valid state keep prompting the
                user for input
            LINE 14-15: Run query on state abbreviation
            LINE 16-17: Run query on full state name

    */
    public static void getNumOfSpecifiedOutcomesByState() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Which result would you like to view (Positive/Negative/Inconclusive): ");
        String choice = input.nextLine();

        while(!choice.equals("Positive") && !choice.equals("Negative") && !choice.equals("Inconclusive")) {
            System.out.println("Invalid Input");
            System.out.print("Which result would you like to view (Positive/Negative/Inconclusive: ");
            choice = input.nextLine();
        }

        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while(!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }

        if(state.length() == 2)
            sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = \'"
                    + choice + "\' AND state = \'" + state + "\';").show(100,false);
        else
            sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = \'"
                    + choice + "\' AND state_name = \'" + state + "\';").show(100,false);
    }
}
