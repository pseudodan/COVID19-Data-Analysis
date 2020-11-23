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

    public static void greeting(){
        System.out.println(
        "\n\n**************************************************\n" +
        "             COVID-19 Data Analysis                   \n" +
        "**************************************************\n");
    }

    /*
        Function: mainMenu
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary:
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
                    queryUSA(sparkSession);
                    System.out.println("Accessing USA Database");
                    break;
                case 2:
                    queryGlobal(sparkSession);
                    System.out.println("Accessing Global Database");
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
    }

    /*
        Function: queryUSA
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary:
    */
    public static void queryUSA(SparkSession sparkSession) throws Exception {
        USA_Queries db = new USA_Queries("hdfs://localhost:9000/COVID19/USA.csv", sparkSession);
        Scanner input = new Scanner(System.in);
        int choice;

        clearScreen();
        greeting();
        System.out.println("0. None, Go Back\n" +
                "1. Specified Outcomes By State\n" +
                "2. Number of Tests Administered\n" +
                "3. Number of Specified Tests By Date Range\n" +
                "4. Total Results Reported Filtered By State and Quarter of the Year\n" +
                "5. Top 'K' Results Reported By State\n" +
                "10. COVID-19 Recent Statistics -> All 50 States\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1: db.getNumOfSpecifiedOutcomesByState(); break;
                case 2: db.getNumOfTestsAdministeredByState(); break;
                case 3: db.getTotalNumOfSpecifiedCasesByDateRange(); break;
                case 4: db.getNumOfSpecifiedOutcomesByQuarterOfYear(); break;
                case 5: db.topKResultsReportedByState(); break;
                case 10: db.recentEvents(); break;
                default: System.out.println("Invalid Input");
            }
            greeting();
            System.out.println("0. None, Go Back\n" +
                    "1. Specified Outcomes By State\n" +
                    "2. Number of Tests Administered\n" +
                    "3. Number of Specified Tests By Date Range\n" +
                    "4. Total Results Reported Filtered By State and Quarter of the Year\n" +
                    "5. Top 'K' Results Reported By State\n" +
                    "10. COVID-19 Recent Statistics -> All 50 States\n");
        }
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void queryGlobal(SparkSession sparkSession)
     * Purpose  -> Method to return the top K results given a case outcome,
     *			   start date (until last recorded date) and the value for K.
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
        System.out.println("Please choose a query to run Global dataset\n" +
                "0. None, Go Back\n" +
                "1. Number of Tests Administered By Country\n" +
                "2. Number of Specified Tests By Date Range\n" +
                "3. Average Life Expectancy Once Tested Positive\n" +
                "4. Average Amount of New Cases By Country\n" +
                "5. Top 'K' Results Reported By Continent\n" +
                "6. Most Recent Deaths By Country\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {

                case 1: db.getNumOfTestsAdministeredByCountry(); break;
                case 2: db.getLargestNumOfCasesInAnOrderedList(); break;
                case 3: db.getAvgLifeExpectancy(); break;
                case 4: db.getAvgNewCases(); break;
                case 5: db.topKResultsReportedByCountry(); break;
                case 6: db.getLatestCasesDeaths(); break;

                default: System.out.println("Invalid Input");
            }

            System.out.println("Please choose a query to run Global dataset\n" +
                            "0. None, Go Back\n" +
                            "1. Number of Tests Administered By Country\n" +
                            "2. Number of Specified Tests By Date Range\n" +
                            "3. Average Life Expectancy Once Tested Positive\n" +
                            "4. Average Amount of New Cases By Country\n" +
                            "5. Top 'K' Results Reported By Continent\n" +
                            "6. Most Recent Deaths By Country\n");
        }
    }

    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CSV Test App")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        mainMenu(sparkSession);

        System.out.println("Session Shutting Down");
    }
}
