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
        Function:
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
                "1. Number of Tests Administered\n" +
                "2. Specified Outcomes By State\n");

        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1: db.getNumOfTestsAdministeredByState(); break;
                case 2: db.getNumOfSpecifiedOutcomesByState(); break;
                default: System.out.println("Invalid Input");
            }

            System.out.println("Which query would you like to run on the US data?\n" +
                    "0. None\n" +
                    "1. Number of Tests Administered\n" +
                    "2. Specified Outcomes By State\n");
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
