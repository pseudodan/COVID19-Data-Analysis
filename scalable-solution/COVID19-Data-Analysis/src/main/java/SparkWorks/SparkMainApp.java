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

        System.out.println("Welcome to the Covid-19 Data Scanner\n" +
                "Which study would you like to view data on?\n" +
                "0. None\n" +
                "1. USA\n");/* +
                "2. GLOBAL\n");*/

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1:
                    System.out.println("Accessing Database");
                    queryUSA(sparkSession);
                    break;
                //case 2: queryGlobal(sparkSession); break;
                default:
                    System.out.println("Invalid Input");
                    mainMenu(sparkSession);
            }

            clearScreen();
            System.out.println("Welcome to the Covid-19 Data Scanner\n" +
                    "Which study would you like to view data on?\n" +
                    "0. None\n" +
                    "1. USA\n");/* +
                "2. GLOBAL\n");*/
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
        System.out.println("Which query would you like to run on the US data?\n" +
                "0. None\n" +
                "1. Specified Outcomes By State\n" +
                "2. Number of Tests Administered\n" +
                "3. Number of Specified States By Date Range\n" +
                "4. Number of Specified Outcomes By Quarter\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1: db.getNumOfSpecifiedOutcomesByState(); break;
                case 2: db.getNumOfTestsAdministeredByState(); break;
                case 3: db.getTotalNumOfSpecifiedCasesByDateRange(); break;
                case 4: db.getNumOfSpecifiedOutcomesByQuarterOfYear(); break;
                case 5: break;
                default: System.out.println("Invalid Input");
            }

            System.out.println("Which query would you like to run on the US data?\n" +
                    "0. None\n" +
                    "1. Specified Outcomes By State\n" +
                    "2. Number of Tests Administered\n" +
                    "3. Number of Specified States By Date Range\n" +
                    "4. Number of Specified Outcomes By Quarter\n");
        }
    }

    /*
        Function:
        Author: Dominic Renales
        Editors:
        Input: None
        Output: None
        Summary:
    */
    // public static void queryGlobal(SparkSession sparkSession) {}

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
