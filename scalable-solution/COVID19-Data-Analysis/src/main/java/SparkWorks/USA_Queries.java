package SparkWorks;

//Apache Spark Includes

import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/*
 * USA Queries class which pulls from our HDFS dir COVID19/USA.csv
 * HDFS is used in conjunction with a spark instance and dataframe
 * Dataset solely operates on states within the US
 */
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
        Function: verifyState
        Author: Dominic Renales
        Modifier: Dan Murphy
        Input: String
        Output: boolean
        Summary: Creates a buffered reader for the hdfs filepath to determine the
            validity of the state name chosen by a user.
    */
    private static boolean verifyState(String state) throws Exception {
        String rootDir = System.getProperty("user.home"); // "dir => /root/file_name_here"
        BufferedReader br = new BufferedReader(new FileReader(new File(rootDir + "/USA_States.txt")));
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
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> String reformatInput()
     * Purpose  -> Converts string to title case.0
     *             united states -> United States
     * -----------------------------------------------------------------------
     * Receives -> String
     * Returns  -> String
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static String reformatInput(String state) {
        Scanner scan = new Scanner(state);
        String upperCase = "";
        if (state.length() == 2) return state.toUpperCase();
        if (state.length() == 1) {
            if (state.toUpperCase().equals("P")) return "Positive";
            else if (state.toUpperCase().equals("N")) return "Negative";
            else if (state.toUpperCase().equals("I")) return "Inconclusive";
            else return "All";
        }
        while(scan.hasNext()){
            String fix = scan.next();
            upperCase += Character.toUpperCase(fix.charAt(0))+ fix.substring(1) + " ";
        }
        return(upperCase.trim());
    } // ---------------------------------------------------------------------



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

        if (!caseResult.equals("All")) {
            if (state.length() == 2)
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' LIMIT 1);").show();
            else
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' LIMIT 1);").show();
        } else {
            if (state.length() == 2) {
                System.out.println("POSITIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' LIMIT 1);").show();
                System.out.println("NEGATIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' LIMIT 1);").show();
                System.out.println("INCONCLUSIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state = '" + state + "' LIMIT 1);").show();
            } else {
                System.out.println("POSITIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' LIMIT 1);").show();
                System.out.println("NEGATIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' LIMIT 1);").show();
                System.out.println("INCONCLUSIVE DATA:");
                sparkSession.sql("SELECT total_results_reported, date FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' AND date = (SELECT MAX(date) FROM USA WHERE overall_outcome = '" + caseResult + "' AND state_name = '" + state + "' LIMIT 1);").show();
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
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Modifier -> Dominic Renales
     * Method   -> void getTotalNumOfSpecifiedCasesByDateRange()
     * Purpose  -> Method to return the number of case results within a
     *			   specified date range within the USA.csv dataset.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 3 /// OPTION 3 /// OPTION 3 /// OPTION 3 /// OPTION 3 /// */
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
    private static void quarterOne(String state, String caseResult) throws Exception {
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
        }
    }

    /*
        Function: quarterTwo
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the two quarter of the year
    */
    private static void quarterTwo(String state, String caseResult) throws Exception {
        if (state.length() == 2) {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-04-01' <= date and '2020-01-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-05-01' <= date and '2020-05-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-06-01' <= date and '2020-06-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        } else {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-04-01' <= date and '2020-04-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-05-01' <= date and '2020-05-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-06-01' <= date and '2020-06-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        }
    }

    /*
        Function: quarterThree
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the third quarter of the year
    */
    private static void quarterThree(String state, String caseResult) throws Exception {
        if (state.length() == 2) {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-07-01' <= date and '2020-07-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-08-01' <= date and '2020-08-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-09-01' <= date and '2020-09-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        } else {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-07-01' <= date and '2020-07-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-08-01' <= date and '2020-08-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-09-01' <= date and '2020-09-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        }
    }

    /*
        Function: quarterFour
        Author: Dominic Renales
        Editors:
        Input: String, String
        Output: None
        Summary: Runs sequel queries on the fourth quarter of the year
    */
    private static void quarterFour(String state, String caseResult) throws Exception {
        if (state.length() == 2) {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-10-01' <= date and '2020-10-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-11-01' <= date and '2020-11-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-12-01' <= date and '2020-12-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        } else {
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-10-01' <= date and '2020-10-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-11-01' <= date and '2020-11-30' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
            sparkSession.sql("SELECT date, overall_outcome, total_results_reported FROM USA " + "WHERE '2020-12-01' <= date and '2020-12-31' >= date " +
                    "and overall_outcome = '" + caseResult + "' " + "and state_name = '" + state + "' ORDER BY date DESC;").show(35);
            TimeUnit.SECONDS.sleep(5);
        }
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Modifier -> Dominic Renales
     * Method   -> void getNumOfSpecifiedOutcomesByQuarterOfYear()
     * Purpose  -> Method to return the number of results given a state name,
     *			   case outcome and quarter of the year (1-4).
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    /* /// OPTION 4 /// OPTION 4 /// OPTION 4 /// OPTION 4 /// OPTION 4 /// */
    public static void getNumOfSpecifiedOutcomesByQuarterOfYear() throws Exception {
        String state = getState();
        state = reformatInput(state);

        String caseResult = getCase();
        caseResult = reformatInput(caseResult);

        int quarter = getQuarter();
        if (!caseResult.equals("All")) {
            if (quarter == 1) { quarterOne(state, caseResult); }
            else if (quarter == 2) { quarterTwo(state,caseResult); }
            else if (quarter == 3) { quarterThree(state, caseResult); }
            else { quarterFour(state, caseResult); }
        } else {
            if (quarter == 1) {
                quarterOne(state, "Positive");
                quarterOne(state, "Negative");
                quarterOne(state, "Inconclusive");
            }
            else if (quarter == 2) {
                quarterTwo(state, "Positive");
                quarterTwo(state, "Negative");
                quarterTwo(state, "Inconclusive");
            } //QUARTER 2 DATE RANGE
            else if (quarter == 3) {
                quarterThree(state, "Positive");
                quarterThree(state, "Negative");
                quarterThree(state, "Inconclusive");
            } //QUARTER 3 DATE RANGE
            else {
                quarterFour(state, "Positive");
                quarterFour(state, "Negative");
                quarterFour(state, "Inconclusive");
            } //QUARTER 4 DATE RANGE
        }
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Modifier -> Dominic Renales
     * Method   -> void topKResultsReportedByState()
     * Purpose  -> Method to return the top K results given a case outcome,
     *			   start date (until last recorded date) and the value for K.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    /* /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// */
    public static void topKResultsReportedByState() {
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
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void getTotalNumOfCasesByDateRange()
     * Purpose  -> Method to get the total number of total results reported
     *			   by a specified state.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// */
    public static void getTotalNumOfCasesByDateRange() throws Exception {
        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while (!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }
        System.out.print("Enter start date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter end date (YYYY-MM-DD: ");
        String endDate = input.nextLine();
        state = reformatInput(state);

        sparkSession.sql("SELECT COUNT(total_results_reported) AS totalCases, date " +
                         "FROM USA " +
                         "WHERE '" + startDate + "' <= date AND date <= '" + endDate + "'" +
                         "GROUP BY date " +
                         "ORDER BY totalCases DESC LIMIT 1;").show();
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void getTotalNumOfNewCasesByDateRange()
     * Purpose  -> Method to get the total number of new results reported
     *			   by a specified state.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// */
    public static void getTotalNumOfNewCasesByDateRange() throws Exception {
        System.out.print("Enter the desired state name: ");
        String state = input.nextLine();
        while (!verifyState(state.toUpperCase())) {
            System.out.println("Invalid State Name.");
            System.out.print("Enter the desired state name: ");
            state = input.nextLine();
        }
        System.out.print("Enter start date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter end date (YYYY-MM-DD: ");
        String endDate = input.nextLine();
        state = reformatInput(state);

        sparkSession.sql("SELECT COUNT(new_results_reported) AS newCases, date " +
                         "FROM USA " +
                         "WHERE '" + startDate + "' <= date AND date <= '" + endDate + "'" +
                         "GROUP BY date " +
                         "ORDER BY newCases DESC LIMIT 1;").show();
    } // ---------------------------------------------------------------------

    /*
        Function: recentEvents
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary: Outputs the recent events of each state in the US
    */
    public static void recentEvents() throws Exception {
        String homePath = System.getProperty("user.home"); // "dir => /root/file_name_here"
        BufferedReader br = new BufferedReader(new FileReader(new File(homePath + "/USA_States.txt")));
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
