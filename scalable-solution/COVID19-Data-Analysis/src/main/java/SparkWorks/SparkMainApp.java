package SparkWorks;

//Apache Spark Includes
import org.apache.spark.sql.*;

//Java Includes
import java.util.Scanner;

public class SparkMainApp {
    /*
        Function: clearScreen
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary: Read the function name...like damn son...
    */
    public static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void greeting()
     * Purpose  -> Method to print a greeting to the console menu.
     *             Purely aesthetic.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void greeting(){
        System.out.println(
                "\n\n**************************************************\n" +
                "             COVID-19 Data Analysis                   \n" +
                "**************************************************\n");
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dominic Renales
     * Modifier -> Dan Murphy
     * Method   -> void mainMenu(SparkSession sparkSession)
     * Purpose  -> Method to initialize the main menu.
     *             Allows the user to choose which dataset to run queries on.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void mainMenu(SparkSession sparkSession) throws Exception {
        Scanner input = new Scanner(System.in);
        int choice;

        clearScreen();
        greeting();
        System.out.println("Please choose a dataset you would like to run\n" +
                           "0. EXIT\n" +
                           "1. USA\n" +
                           "2. GLOBAL\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1:
                    System.out.println("\nAccessing USA Database...\n");
                    System.out.println("Please wait while the data is pre-processed...\n\n");
                    queryUSA(sparkSession);
                    break;
                case 2:
                    System.out.println("\nAccessing Global Database...\n");
                    System.out.println("Please wait while the data is pre-processed...\n\n");
                    queryGlobal(sparkSession);
                    break;
                default:
                    System.out.println("Invalid Input");
                    mainMenu(sparkSession);
            }

            clearScreen();
            greeting();
            System.out.println("Please choose a dataset you would like to run\n" +
                               "0. EXIT\n" +
                               "1. USA\n" +
                               "2. GLOBAL\n");
        }
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dominic Renales
     * Modifier -> Dan Murphy
     * Method   -> void mainMenu(SparkSession sparkSession)
     * Purpose  -> Method to permit queries to run on the USA.csv dataset.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void queryUSA(SparkSession sparkSession) throws Exception {
        USA_Queries db = new USA_Queries("hdfs://localhost:9000/COVID19/USA.csv", sparkSession);
        Scanner input = new Scanner(System.in);
        int choice;

        clearScreen();
        greeting();
        System.out.println("0. None, Go Back\n" +
                "1. Number of Case Outcomes By State\n" +
                "2. Number of Total Tests Administered\n" +
                "3. Number of Specified Tests By Date Range\n" +
                "4. Total Results Reported Filtered By State and Quarter of the Year\n" +
                "5. Top 'K' Results Reported By State\n" +
                "6. Total Number of Cases By Date Range\n" +
                "7. Total Number of New Cases By Date Range\n" +
                "8. List Total Quarterly New Results Reported By Case Outcome\n" +
                "9. List Total Quarterly Case Outcomes By State\n" +
                "10. COVID-19 Recent Statistics -> All 50 States\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1: db.getNumOfSpecifiedOutcomesByState(); break;
                case 2: db.getNumOfTestsAdministeredByState(); break;
                case 3: db.getTotalNumOfSpecifiedCasesByDateRange(); break;
                case 4: db.getNumOfSpecifiedOutcomesByQuarterOfYear(); break;
                case 5: db.topKResultsReportedByState(); break;
                case 6: db.getTotalNumOfCasesByDateRange(); break;
                case 7: db.getTotalNumOfNewCasesByDateRange(); break;
                case 8: db.listTotalQuarterlyReportsByCase(); break;
                case 9: db.listTotalQuarterlyReportsByState(); break;
                case 10: db.recentEvents(); break;
                default: System.out.println("Invalid Input");
            }
            greeting();
            System.out.println("0. None, Go Back\n" +
                    "1. Number of Case Outcomes By State\n" +
                    "2. Number of Total Tests Administered\n" +
                    "3. Number of Specified Tests By Date Range\n" +
                    "4. Total Results Reported Filtered By State and Quarter of the Year\n" +
                    "5. Top 'K' Results Reported By State\n" +
                    "6. Total Number of Cases By Date Range\n" +
                    "7. Total Number of New Cases By Date Range\n" +
                    "8. List Total Quarterly New Results Reported By Case Outcome\n" +
                    "9. List Total Quarterly Case Outcomes By State\n" +
                    "10. COVID-19 Recent Statistics -> All 50 States\n");
        }
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void queryGlobal(SparkSession sparkSession)
     * Purpose  -> Method to permit queries to run on the Global.csv dataset.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void queryGlobal(SparkSession sparkSession) throws Exception {
        Global_Queries db = new Global_Queries("hdfs://localhost:9000/COVID19/Global.csv", sparkSession);
        Scanner input = new Scanner(System.in);
        int choice;

        clearScreen();
        greeting();
        System.out.println("0. None, Go Back\n" +
                "1. Number of Tests Administered By Continent\n" +
                "2. Number of Tests Administered By Country\n" +
                "3. Number of Total Cases By Country\n" +
                "4. Number of Total Cases By Continent\n" +
                "5. Number of Total Cases Globally\n" +
                "6. Average Life Expectancy Once Tested Positive\n" +
                "7. Average Amount of New Cases By Country\n" +
                "8. Most Recent Deaths By Country\n" +
                "9. Top 'K' Countries By Total Cases on a Specific Date\n" +
                "10. Top 'K' Countries By Total Deaths on a Specific Date\n" +
                "11. Top 'K' Hospitalized Patients In Europe Based On Total Cases\n" +
                "12. Top 'K' ICU Patients In Europe Based On Total Cases\n" +
                "13. Total Number of Positive Cases Per Month\n" +
                "14. What Month Saw the Greatest Number of Cases?\n" +
                "99. [INCOMPLETE] Predict Number of Cases For The Following Month\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {

                case 1: db.getNumOfTestsAdministeredByContinent(); break;
                case 2: db.getNumOfTestsAdministeredByCountry(); break;
                case 3: db.getMaxNumOfCasesByCountry(); break;
                case 4: db.getMaxNumOfCasesByContinent(); break;
                case 5: db.getMaxNumOfCasesGlobally(); break;
                case 6: db.getAvgLifeExpectancy(); break;
                case 7: db.getAvgNewCases(); break;
                case 8: db.getLatestCasesDeaths(); break;
                case 9: db.topKTotalCasesReportedByCountry(); break;
                case 10: db.topKDeathsReportedByCountry(); break;
                case 11: db.listTopKHospitalizedPatientDataInEurope(); break;
                case 12: db.listTopKICUPatientDataInEurope(); break;
                case 13: db.totalNumberOfPositiveCasesPerMonth(); break;
                case 14: db.monthNumWithGreatestNumberOfCases(); break;
                case 99: db.predictTotalCasesForFollowingMonth(); break;

                default: System.out.println("Invalid Input");
            }
            greeting();
            System.out.println("0. None, Go Back\n" +
                    "1. Number of Tests Administered By Continent\n" +
                    "2. Number of Tests Administered By Country\n" +
                    "3. Number of Total Cases By Country\n" +
                    "4. Number of Total Cases By Continent\n" +
                    "5. Number of Total Cases Globally\n" +
                    "6. Average Life Expectancy Once Tested Positive\n" +
                    "7. Average Amount of New Cases By Country\n" +
                    "8. Most Recent Deaths By Country\n" +
                    "9. Top 'K' Countries By Total Cases on a Specific Date\n" +
                    "10. Top 'K' Countries By Total Deaths on a Specific Date\n" +
                    "11. Top 'K' Hospitalized Patients In Europe Based On Total Cases\n" +
                    "12. Top 'K' ICU Patients In Europe Based On Total Cases\n" +
                    "13. Total Number of Positive Cases Per Month\n" +
                    "14. What Month Saw the Greatest Number of Cases?\n" +
                    "99. [INCOMPLETE] Predict Number of Cases For The Following Month\n");
        }
    } // ---------------------------------------------------------------------

    /* MAIN TEST HARNESS */
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CSV Test App")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        mainMenu(sparkSession);

        System.out.println("Session Shutting Down");
    } // ---------------------------------------------------------------------

} // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
