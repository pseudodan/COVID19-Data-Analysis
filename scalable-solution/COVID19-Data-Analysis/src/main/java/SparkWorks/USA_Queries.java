package SparkWorks;

//Apache Spark Includes

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.*;

//Java Includes
import javax.xml.crypto.Data;
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class USA_Queries {
    private static Dataset<Row> df;
    private static SparkSession sparkSession;
    //private static Scanner input = new Scanner(System.in);

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
    public USA_Queries(String filePath, SparkSession sparkSession) {
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
        Function: printValidStates()
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary: Prints valid list of states the user can use
    */
    public static void printValidStates() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("/home/hdfs/USA_States.txt")));
        String read;

        System.out.println("\n[State Name : State Abbreviation]");
        while ((read = br.readLine()) != null) { System.out.println(read); }
        System.out.println();
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
        BufferedReader br = new BufferedReader(new FileReader(new File("/home/hdfs/USA_States.txt")));
        String read;

        while ((read = br.readLine()) != null)
            if (read.toUpperCase().contains(state))
                return true;
        return false;
    }

    /*
        Function: verifyCase
        Author: Dominic Renales
        Editors:
        Input: String
        Output: boolean
        Summary: Verifies that the chosen case result is valid.
    */
    private static boolean verifyCase(String caseResult) {
        switch (caseResult.toUpperCase()) {
            case "POSITIVE":
            case "P":
            case "NEGATIVE":
            case "N":
            case "INCONCLUSIVE":
            case "I":
            case "ALL":
            case "A":
                return true;
            default:
                return false;
        }
    }

    /*
        Function: getCase
        Author: Dominic Renales
        Editors:
        Input: None
        Output: String
        Summary: Prompts the user to enter the case result they want to query.
    */
    private static String getCase() {
        Scanner input = new Scanner(System.in);
        System.out.print("Which result would you like to view ([P]ositive/[N]egative/[I]nconclusive)/[A]ll): ");
        String caseResult = input.nextLine();
        while (!verifyCase(caseResult)) {
            System.out.println("Invalid Input");
            System.out.print("Which result would you like to view (Positive/Negative/Inconclusive: ");
            caseResult = input.nextLine();
        }

        return caseResult;
    }

    /*
        Function: getState
        Author: Dominic Renales
        Editors:
        Input: None
        Output: String
        Summary: Prompts the user to enter the state they want to query.
    */
    private static String getState() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while (!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }

        return state;
    }

    /*
        Function: getQuarter
        Author: Dominic Renales
        Editors:
        Input: None
        Output: int
        Summary: Prompts the user to enter a number between one and four.
    */
    private static int getQuarter() {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter a quarter of the year you wish to evaluate (1-4): ");
        int quarter = input.nextInt();

        while (quarter < 1 || quarter > 4) {
            System.out.println("Invalid Value");
            System.out.print("Enter a quarter of the year you wish to evaluate: ");
            quarter = input.nextInt();
        }

        return quarter;
    }

    /*
        Function: reformatInput
        Author: Dominic Renales
        Editors:
        Input: String
        Output: String
        Summary: Returns properly formatted input for flexibility's sake.
    */
    private static String reformatInput(String state) {
        if (state.length() == 2) return state.toUpperCase();
        if (state.length() == 1) {
            if (state.toUpperCase().equals("P")) return "Positive";
            else if (state.toUpperCase().equals("N")) return "Negative";
            else if (state.toUpperCase().equals("I")) return "Inconclusive";
            else return "All";
        }
        return state.substring(0, 1).toUpperCase() + state.substring(1).toLowerCase();
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
        String caseResult = getCase();
        caseResult = reformatInput(caseResult);

        String state = getState();
        state = reformatInput(state);

        if (!caseResult.equals("All")) {
            if (state.length() == 2)
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '"
                        + caseResult + "' AND state = '" + state + "';").show();
            else
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '"
                        + caseResult + "' AND state_name = '" + state + "';").show();
        } else {
            if (state.length() == 2) {
                System.out.println("POSITIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Positive" + "' AND state = '" + state + "';").show();
                System.out.println("NEGATIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Negative" + "' AND state = '" + state + "';").show();
                System.out.println("INCONCLUSIVE DATA:");
                sparkSession.sql("SELECT COUNT(overall_outcome) FROM USA WHERE overall_outcome = '" + "Inconclusive" + "' AND state = '" + state + "';").show();
            } else {
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
            the number of tests conducted in a state.
    */
    public static void getNumOfTestsAdministeredByState() throws Exception {
        String state = getState();
        state = reformatInput(state);

        if (state.length() == 2)
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state ='" + state + "';").show();
        else
            sparkSession.sql("SELECT COUNT(*) FROM USA WHERE state_name ='" + state + "';").show();
    }

    /*
        Function: getTotalNumOfSpecifiedCasesByDateRange
        Author: Daniel Murphy
        Editors: Dominic Renales
        Input: None
        Output: None
        Summary: Scans the USA data to output the number of tests recorded between the date range.
    */
    public static void getTotalNumOfSpecifiedCasesByDateRange() {
        Scanner input = new Scanner(System.in);

        String caseResult = getCase();
        caseResult = reformatInput(caseResult);

        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter a ending date (YYYY-MM-DD): ");
        String endDate = input.nextLine();

        if (!caseResult.equals("All")) {
            sparkSession.sql("SELECT COUNT(overall_outcome) AS total, date" +
                    " FROM USA" +
                    " WHERE '" + startDate + "' <= date and date <= '" + endDate + "' and overall_outcome = '" + caseResult +
                    "' GROUP BY date" +
                    " ORDER BY total DESC;").show(1000, false);
        } else {
            sparkSession.sql("SELECT COUNT(overall_outcome) AS total, date" +
                    " FROM USA" +
                    " WHERE '" + startDate + "' <= date and date <= '" + endDate +
                    "' GROUP BY date" +
                    " ORDER BY total DESC;").show(1000, false);
        }
    }

    /*
        Function: quarterOne
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the one quarter of the year
    */
    private static Dataset<Row> quarterOne(String state, String caseResult) throws Exception {
        if(state.length() == 2)
            return sparkSession.sql("SELECT * FROM USA " +
                "WHERE '2020-01-01' <= date and '2020-03-31' >= date " +
                "and overall_outcome = '" + caseResult + "' " +
                "and state = '" + state + "' ORDER BY date;");
        else
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-01-01' <= date and '2020-03-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date;");

        /*
        if (state.length() == 2) {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-01-01' <= date and '2020-01-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-02-01' <= date and '2020-02-28' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-03-01' <= date and '2020-03-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        } else {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-01-01' <= date and '2020-01-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-02-01' <= date and '2020-02-28' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " +
                    "WHERE '2020-03-01' <= date and '2020-03-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        }*/
    }

    /*
        Function: quarterTwo
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the two quarter of the year
    */
    private static Dataset<Row> quarterTwo(String state, String caseResult) throws Exception {
        if(state.length() == 2)
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-04-01' <= date and '2020-06-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date;");
        else
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-04-01' <= date and '2020-06-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date;");
    }

    /*
        Function: quarterThree
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the third quarter of the year
    */
    private static Dataset<Row> quarterThree(String state, String caseResult) throws Exception {
        if(state.length() == 2)
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-07-01' <= date and '2020-09-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date;");
        else
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-07-01' <= date and '2020-09-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date;");
    }

    /*
        Function: quarterFour
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: Dataset<Row>
        Summary: Runs sequel queries on the fourth quarter of the year
    */
    private static Dataset<Row> quarterFour(String state, String caseResult) throws Exception {
        if(state.length() == 2)
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-10-01' <= date and '2020-12-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state = '" + state + "' ORDER BY date;");
        else
            return sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '2020-10-01' <= date and '2020-12-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + state + "' ORDER BY date;");
    }

    /*
        Function: getNumOfSpecifiedOutcomesByQuarterOfYear()
        Author: Daniel Murphy
        Editors: Dominic Renales
        Input: None
        Output: None
        Summary: Displays all the information on a single state with a desired case
            result for some quarter of the year. This shit written like a fucking
            .$0.35/hour Indian Call Center Code Monkey. Things to be learned from this
            flying fucking spaghetti monster...: Programming with the sense of "How the
            fuck is dip-shit user going to try to break this?" sucks if you do not
            properly parse input to only use one form of a schema should multiple forms
            of the same thing exist...

            EDIT: The nightmare has been fixed.......
    */
    public static void getNumOfSpecifiedOutcomesByQuarterOfYear() throws Exception {
        String state = getState();
        state = reformatInput(state);

        String caseResult = getCase();
        caseResult = reformatInput(caseResult);

        int quarter = getQuarter();
        if (!caseResult.equals("All")) {
            if (quarter == 1) {
                Dataset<Row> df = quarterOne(state,caseResult);
                if(state.length()==2) {
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-03-01","2020-03-31"))
                            .show(35,false); }
                else {
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                        .filter(df.col("date").between("2020-03-01","2020-03-31"))
                        .show(35,false); }
            }
            else if (quarter == 2) {
                Dataset<Row> df = quarterTwo(state,caseResult);
                if(state.length() == 2) {
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-04-01", "2020-04-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-05-01", "2020-05-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-06-01", "2020-06-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
                else {
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-04-01", "2020-04-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-05-01", "2020-05-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-06-01", "2020-06-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
            }
            else if (quarter == 3) {
                Dataset<Row> df = quarterThree(state,caseResult);
                if(state.length() == 2) {
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-07-01", "2020-07-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-08-01", "2020-08-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-09-01", "2020-09-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
                else {
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-07-01", "2020-07-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-08-01", "2020-08-31"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);

                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                            .filter(df.col("date").between("2020-09-01", "2020-09-30"))
                            .show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
            }
            else {
                Dataset<Row> df = quarterFour(state,caseResult);
                if(state.length() == 2) {
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-10-01", "2020-10-30")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-11-01", "2020-11-30")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                    df.select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-12-01", "2020-12-31")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
                else {
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-10-01", "2020-10-30")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-11-01", "2020-11-30")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                    df.select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).filter(df.col("date").between("2020-12-01", "2020-12-31")).show(35, false);
                    TimeUnit.SECONDS.sleep(3);
                }
            }
        } else {
            if (quarter == 1) {
                if (state.length() == 2) {
                    quarterOne(state, "Positive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterOne(state, "Negative").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterOne(state, "Inconclusive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
                else {
                    quarterOne(state, "Positive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterOne(state, "Negative").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterOne(state, "Inconclusive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }

                /*quarterOne(state, "Positive").show(35,false);
                TimeUnit.SECONDS.sleep(5);
                quarterOne(state, "Negative").show(35,false);
                TimeUnit.SECONDS.sleep(5);
                quarterOne(state, "Inconclusive").show(35,false);
                TimeUnit.SECONDS.sleep(5);*/
            }
            else if (quarter == 2) {
                if(state.length() == 2) {
                    quarterTwo(state, "Positive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterTwo(state, "Negative").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterTwo(state, "Inconclusive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
                else {
                    quarterTwo(state, "Positive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterTwo(state, "Negative").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterTwo(state, "Inconclusive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
            } //QUARTER 2 DATE RANGE
            else if (quarter == 3) {
                if (state.length() == 2) {
                    quarterThree(state, "Positive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterThree(state, "Negative").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterThree(state, "Inconclusive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
                else {
                    quarterThree(state, "Positive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterThree(state, "Negative").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterThree(state, "Inconclusive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
            } //QUARTER 3 DATE RANGE
            else {
                if(state.length() == 2) {
                    quarterFour(state, "Positive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterFour(state, "Negative").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterFour(state, "Inconclusive").select(df.col("date"), df.col("state"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
                else {
                    quarterFour(state, "Positive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterFour(state, "Negative").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                    quarterFour(state, "Inconclusive").select(df.col("date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported")).show(100, false);
                    TimeUnit.SECONDS.sleep(5);
                }
            } //QUARTER 4 DATE RANGE
        }
    }

    /*
        Function: topKListWithDate
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary: Lists the top K items of a chosen case result and date chosen by the user
    */
    public static void topKListWithDate() {
        Scanner input = new Scanner(System.in);

        String caseResult = getCase();
        caseResult = reformatInput(caseResult);

        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String date = input.nextLine();

        System.out.print("Enter the list size you want to see: ");
        int K = input.nextInt();
        while (K < 1) {
            System.out.println("Invalid Input");
            K = input.nextInt();
        }

        if (caseResult.equals("All")) {
            sparkSession.sql("SELECT state_name, overall_outcome, total_results_reported FROM USA WHERE '" + date + "' = date " +
                    "and overall_outcome = 'Positive' ORDER BY total_results_reported DESC;").show(K);
            sparkSession.sql("SELECT state_name, overall_outcome, total_results_reported FROM USA WHERE '" + date + "' = date " +
                    "and overall_outcome = 'Negative' ORDER BY total_results_reported DESC;").show(K);
            sparkSession.sql("SELECT state_name, overall_outcome, total_results_reported FROM USA WHERE '" + date + "' = date " +
                    "and overall_outcome = 'Inconclusive' ORDER BY total_results_reported DESC;").show(K);
        }
        else sparkSession.sql("SELECT state_name, overall_outcome, total_results_reported FROM USA WHERE '" + date + "' = date " +
                "and overall_outcome = '" + caseResult + "' ORDER BY total_results_reported DESC;").show(K);
    }

    /*
        Function: recentEvents
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary: Outputs the recent events of each state in the US
    */
    public static void recentEvents() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("/home/hdfs/USA_States.txt")));
        String read;
        while ((read = br.readLine()) != null) {
            sparkSession.sql("SELECT state_name, date, overall_outcome, new_results_reported, total_results_reported FROM USA " +
                    "WHERE state_name = '" + read.substring(0, read.indexOf(',')) + "' and overall_outcome = 'Positive' " +
                    "ORDER BY date DESC;").show(5);
            TimeUnit.MILLISECONDS.sleep(500);
            sparkSession.sql("SELECT state_name, date, overall_outcome, new_results_reported, total_results_reported FROM USA " +
                    "WHERE state_name = '" + read.substring(0, read.indexOf(',')) + "' and overall_outcome = 'Negative' " +
                    "ORDER BY date DESC;").show(5);
            TimeUnit.MILLISECONDS.sleep(500);
            sparkSession.sql("SELECT state_name, date, overall_outcome, new_results_reported, total_results_reported FROM USA " +
                    "WHERE state_name = '" + read.substring(0, read.indexOf(',')) + "' and overall_outcome = 'Inconclusive' " +
                    "ORDER BY date DESC;").show(5);
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }

    public static void listQuarterlyReportsByCase() throws Exception {
        String state = getState();
        state = reformatInput(state);

        String caseResult = getCase();
        caseResult = reformatInput(caseResult);
        Dataset<Row> df1 = quarterOne(state,caseResult),
                        df2 = quarterTwo(state,caseResult),
                        df3 = quarterThree(state,caseResult),
                        df4 = quarterFour(state,caseResult);

            Dataset<Row> df1Max = df1.select(functions.sum("new_results_reported").cast("BIGINT").as("Quarterly Reports"));
            df1Max = df1Max.withColumn("state", functions.lit(state)).withColumn("Quarter", functions.lit(1));

            Dataset<Row> df2Max = df2.select(functions.sum("new_results_reported").cast("BIGINT").as("Quarterly Reports"));
            df2Max = df2Max.withColumn("state", functions.lit(state)).withColumn("Quarter", functions.lit(2));

            Dataset<Row> df3Max = df3.select(functions.sum("new_results_reported").cast("BIGINT").as("Quarterly Reports"));
            df3Max = df3Max.withColumn("state", functions.lit(state)).withColumn("Quarter", functions.lit(3));

            Dataset<Row> df4Max = df4.select(functions.sum("new_results_reported").cast("BIGINT").as("Quarterly Reports"));
            df4Max = df4Max.withColumn("state", functions.lit(state)).withColumn("Quarter", functions.lit(4));

            Dataset<Row> MAX = df1Max.union(df2Max.union(df3Max.union(df4Max)));
            MAX.orderBy(MAX.col("Quarterly Reports").desc()).show(false);

        /*
        Dataset<Row> df1 = sparkSession.sql("SELECT SUM(new_results_reported) " +
                "FROM USA " +
                "WHERE state = '" + state + "' AND overall_outcome = '" + caseResult + "' " +
                "AND date >= '2020-01-01' AND date <= '2020-03-31';").withColumn("Quarter", functions.lit("1"));
        Dataset<Row> df2 = sparkSession.sql("SELECT SUM(new_results_reported) " +
                "FROM USA " +
                "WHERE state = '" + state + "' AND overall_outcome = '" + caseResult + "' " +
                "AND date >= '2020-04-01' AND date <= '2020-06-30';").withColumn("Quarter", functions.lit("2"));
        Dataset<Row> df3 = sparkSession.sql("SELECT SUM(new_results_reported) " +
                "FROM USA " +
                "WHERE state = '" + state + "' AND overall_outcome = '" + caseResult + "' " +
                "AND date >= '2020-07-01' AND date <= '2020-09-30';").withColumn("Quarter", functions.lit("3"));
        Dataset<Row> df4 = sparkSession.sql("SELECT SUM(new_results_reported) " +
                "FROM USA " +
                "WHERE state = '" + state + "' AND overall_outcome = '" + caseResult + "' " +
                "AND date >= '2020-10-01' AND date <= '2020-12-31';").withColumn("Quarter", functions.lit("4"));

        Dataset<Row> dfMax = df1.union(df2.union(df3.union(df4)));
        //dfMax.orderBy()
        dfMax.orderBy(dfMax.col("sum(new_results_reported)").desc()).show(1,false);
        */
    }

}

/* SAVE FOR USE WITH K TYPE QUESTIONS AND INFO DUMP
        BufferedReader br = new BufferedReader(new FileReader(new File("/home/hdfs/USA_States.txt")));
        String read;
        while ((read = br.readLine()) != null) {
            sparkSession.sql("SELECT * FROM USA " +
                    "WHERE '" + startDate + "' <= date and '" + endDate + "' >= date " +
                    "and overall_outcome = '" + caseResult + "' " +
                    "and state_name = '" + read.substring(0, read.indexOf(',')) + "';").show(100);
            TimeUnit.SECONDS.sleep(5);
        }
*/
