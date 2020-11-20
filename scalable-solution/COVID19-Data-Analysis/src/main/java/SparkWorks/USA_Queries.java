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
    private static Scanner input = new Scanner(System.in);

    /*
        Function: Queries
        Author: Dominic Renales
        Editors:
        Input: String, SparkSession
        Output: None
        Summary: Constructor that sets the private data field Dataset<Row>
            and SparkSession. Afterwards, create the temporary view of the
            csv file that will be looked at.
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
    */
    private static boolean verifyState(String state) throws Exception {
        File f = new File("/home/hdfs/USA_States.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String read;

        while((read = br.readLine()) != null)
            if(read.toUpperCase().contains(state))
                return true;
        return false;
    }

    /*
        Function: verifyCase
        Author: Dominic Renales
        Editors:
        Input: String
        Output: boolean
        Summary: Verifies that the chosen case result is valid
    */
    private static boolean verifyCase(String caseResult) {
        switch(caseResult.toUpperCase()) {
            case "POSITIVE":     case "P":
            case "NEGATIVE":     case "N":
            case "INCONCLUSIVE": case "I":
            case "ALL":          case "A": return true;
            default: return false;
        }
    }

    /*
        Function: reformatInput
        Author: Dominic Renales
        Editors:
        Input: String
        Output: String
        Summary: Returns properly formatted input for flexibility's sake
    */
    private static String reformatInput(String state) {
        if (state.length() == 2) return state.toUpperCase();
        if(state.length() == 1)
        {
            if(state.toUpperCase().equals("P")) return "Positive";
            else if(state.toUpperCase().equals("N")) return "Negative";
            else if(state.toUpperCase().equals("I")) return "Inconclusive";
            else return "All";
        }
        return state.substring(0,1).toUpperCase() + state.substring(1).toLowerCase();
    }

    /* OPTION 1 COMPLETE [Query translated from non-scalable PSQL version]
            Function: getNumSpecifiedOutcomesByState
            Author: Daniel Murphy
            Editors: Dominic Renales
            Input: None
            Output: None
            Summary: Prints out the result of the specified outcome on a desired state.
    */
    public static void getNumOfSpecifiedOutcomesByState() throws Exception {
        System.out.print("Which result would you like to view ([P]ositive/[N]egative/[I]nconclusive)/[A]ll): ");
        String caseResult = input.nextLine();
        while(!verifyCase(caseResult)) {
            System.out.println("Invalid Input");
            System.out.print("Which result would you like to view (Positive/Negative/Inconclusive: ");
            caseResult = input.nextLine();
        }
        caseResult = reformatInput(caseResult);

        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while(!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }
        state = reformatInput(state);

        if(!caseResult.equals("All")) {
            if (state.length() == 2)
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '"
                        + caseResult + "' AND state = '" + state + "';").show();
            else
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '"
                        + caseResult + "' AND state_name = '" + state + "';").show();
        }
        else {
            if (state.length() == 2) {
                System.out.println("POSITIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Positive" + "' AND state = '" + state + "';").show();
                System.out.println("NEGATIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Negative" + "' AND state = '" + state + "';").show();
                System.out.println("INCONCLUSIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Inconclusive" + "' AND state = '" + state + "';").show();
            }
            else {
                System.out.println("POSITIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Positive" + "' AND state_name = '" + state + "';").show();
                System.out.println("NEGATIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Negative" + "' AND state_name = '" + state + "';").show();
                System.out.println("INCONCLUSIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Inconclusive" + "' AND state_name = '" + state + "';").show();
            }
        }
    }
    /*  OPTION 2 COMPLETE [Query translated from non-scalable PSQL version]
        Function: getNumOfTestsAdministeredByState
        Author: Daniel Murphy
        Editors: Dominic Renales
        Input: None
        Output: None
        Summary: Scans the USA data in the HDFS to print information regarding
            the number of tests conducted in a state
    */
    public static void getNumOfTestsAdministeredByState() throws Exception {
        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while(!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }
        state= reformatInput(state);

        if(state.length() == 2)
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state ='" + state + "';").show();
        else
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state_name ='" + state + "';").show();
    }

    /*

    */
    public static void getTotalNumOfSpecifiedCasesByDateRange() {
        System.out.print("Which result would you like to view ([P]ositive/[N]egative/[I]nconclusive)/[A]ll): ");
        String caseResult = input.nextLine();
        while(!verifyCase(caseResult)) {
            System.out.println("Invalid Input");
            System.out.print("Which result would you like to view (Positive/Negative/Inconclusive: ");
            caseResult = input.nextLine();
        }
        caseResult = reformatInput(caseResult);

        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter a ending date (YYYY-MM-DD): ");
        String endDate = input.nextLine();

        if(!caseResult.equals("All")) {
            sparkSession.sql("SELECT COUNT(overall_outcome) AS total, date" +
                    " FROM USA" +
                    " WHERE '" + startDate + "' <= date and date <= '" + endDate + "' and overall_outcome = '" + caseResult +
                    "' GROUP BY date" +
                    " ORDER BY total DESC;").show(1000, false);
        }
        else {
            sparkSession.sql("SELECT COUNT(overall_outcome) AS total, date" +
                    " FROM USA" +
                    " WHERE '" + startDate + "' <= date and date <= '" + endDate +
                    "' GROUP BY date" +
                    " ORDER BY total DESC;").show(1000, false);
            }
    }

}
