package SparkWorks;

//Apache Spark Includes
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class Global_Queries {
    private static Dataset<Row> df;
    private static SparkSession sparkSession;
    private static Scanner input = new Scanner(System.in);

    /*
        Function: Queries
        Author: Dominic Renales
        Editors: Gerardo Castro Mata
        Input: String, Sparksession
        Output: None
        Summary: Constructor that sets the private data field Dataset<Row>
        and sparkSession. Afterwards, creates the temporary view of the csv file that will be looked at.
    */
    public Global_Queries(String filepath, SparkSession sparksession) {
        this.sparkSession = sparksession;
        this.df = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filepath);
        df.createOrReplaceTempView("GLOBAL");
    }

    /*
       Function: verifyCountry
       Author: Dominic Renales
       Editors: Gerardo Castro Mata, Dan Murphy
       Input: String
       Output: boolean
       Summary: Creates a buffered reader for the hdfs filepath to determine the validity of the state name
       chosen by a user.
    */
    public static boolean verifyCountry(String countryName) throws Exception {
        String rootDir = System.getProperty("user.home"); // "dir => /root/file_name_here"
        File f = new File(rootDir + "/Global_Names.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String read;

        while ((read = br.readLine()) != null)
            if (read.toUpperCase().contains(countryName))
                return true;
        return false;
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
        Function: verifyCase
        Author: Dominic Renales
        Editors:
        Input: String
        Output: boolean
        Summary: Verifies that the chosen case result is valid
    */
    private static boolean verifyCase(String caseResult) {
        switch (caseResult.toUpperCase()) {
            case "A":
            case "B":
                return true;
            default:
                return false;
        }
    }

    private static String getCountry() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter the desired country name: ");
        String country = input.nextLine();
        while (!verifyCountry(country.toUpperCase())) {
            System.out.println("Invalid Country Name.");
            System.out.print("Enter the desired country name: ");
            country = input.nextLine();
        }
        return country;
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
        if (state.length() == 1 || state.length() == 2) return state.toUpperCase();
        return state.substring(0, 1).toUpperCase() + state.substring(1).toLowerCase();
    }

    /*  OPTION 2 COMPLETE [Query translated from non-scalable PSQL version]
        Function: getNumOfTestsAdministeredByCountry
        Author: Daniel Murphy
        Editors: Dominic Renales, Gerardo Castro Mata
        Input: None
        Output: Executed Query
        Summary: Scans the GLOBAL data in the HDFS to print information regarding
            the number of tests conducted in a country
        NOTE: Does not properly count for some countries that have an empty datapoint such as 'United States'
    */
    public static void getNumOfTestsAdministeredByCountry() throws Exception {
        String country = getCountry();
        country = reformatInput(country);
        if (country.length() == 3)
            sparkSession.sql("SELECT COUNT(*) " + "FROM GLOBAL " + "WHERE iso_code = '" + country + "';").show();
        else
            sparkSession.sql("SELECT COUNT(*) " + "FROM GLOBAL " + "WHERE location = '" + country + "';").show();

    }

    /* OPTION 3 COMPLETE [Query translated from non-scalable PSQL version]
       Function: getLargestNumOfCasesInAnOrderedList
       Author: Daniel Murphy
       Editors: Gerardo Castro Mata
       Input: None
       Output: Executed Query
       Summary: Scans the GLOBAL data in the HDFS to print the information regarding the
       largest number of cases in an ordered list within a country.

    */
    public static void getLargestNumOfCasesInAnOrderedList() throws Exception {
        System.out.print("Enter the desired country name: ");
        String country = input.nextLine();
        while (!verifyCountry(country.toUpperCase())) {
            System.out.println("Invalid Country Name.");
            System.out.print("Enter the desired country name: ");
            country = input.nextLine();
        }
        System.out.print("Enter start date (yyyy-mm-dd): ");
        String startDate = input.nextLine();
        System.out.print("Enter end date (yyyy-mm-dd): ");
        String endDate = input.nextLine();
        country = reformatInput(country);

        sparkSession.sql("SELECT COUNT(total_cases) AS total, date " +
                "FROM GLOBAL " +
                "WHERE '" + startDate + "' <= date AND date <= '" + endDate + "'" +
                "GROUP BY date " +
                "ORDER BY total DESC;").show(1000, false);
    }

    /* OPTION 4 COMPLETE
       Function: getAvgLifeExpectancy
       Author: Gerardo Castro Mata
       Editors:
       Input: None
       Output: Expected Query
       Summary: Scans the GLOBAL data in the HDFS to retrieve the average life expectancy of a country based on the
       most recent date.
    */
    public static void getAvgLifeExpectancy() throws Exception {
        System.out.print("Enter the desired country name: ");
        String country = input.nextLine();
        while (!verifyCountry(country.toUpperCase())) {
            System.out.println("Invalid Country Name.");
            System.out.print("Enter the desired country name: ");
            country = input.nextLine();
        }
        sparkSession.sql("SELECT life_expectancy AS Average_Life_Expectancy " +
                "FROM GLOBAL " +
                "WHERE location = '" + country + "' " +
                "AND date = (SELECT date " +
                "FROM GLOBAL " +
                "GROUP BY date " +
                "ORDER BY date DESC LIMIT 1);").show();
    }

    private static String getContinent() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter the desired continent name: ");
        String continent = input.nextLine();
        while (!verifyCountry(continent.toUpperCase())) {
            System.out.println("Invalid Continent Name.");
            System.out.print("Enter the desired continent name: ");
            continent = input.nextLine();
        }
        return continent;
    }

    private static boolean verifyContinent(String continentName) throws Exception {
        String rootDir = System.getProperty("user.home"); // "dir => /root/file_name_here"
        File f = new File(rootDir + "/Continent_Names.txt");
        FileReader fr = new FileReader(f);
        BufferedReader br = new BufferedReader(fr);
        String read;

        while ((read = br.readLine()) != null)
            if (read.toUpperCase().contains(continentName))
                return true;
        return false;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Modifier -> Dominic Renales
     * Method   -> void topKResultsReportedByCountry()
     * Purpose  -> Method to return the top K results given a case outcome,
     *			   start date (until last recorded date) and the value for K.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    /* /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// */
    public static void topKResultsReportedByCountry() throws Exception {
        Scanner input = new Scanner(System.in);
        String continent = getContinent();
        continent = reformatInput(continent);
        //String country = getCountry();
        //country = reformatInput(country);

        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String date = input.nextLine();

        System.out.print("Enter the list size you want to see: ");
        int K = input.nextInt();
        while (K < 1) {
            System.out.println("Invalid Input");
            K = input.nextInt();
        }
        sparkSession.sql("SELECT location, total_cases, new_cases, total_cases_per_million, new_cases_per_million FROM Global WHERE '" + date + "' = date " +
                "and continent = '" + continent + "' ORDER BY total_cases DESC;").show(K);
    }

    /* OPTION 6 COMPLETE
       Function: getAvgNewCases
       Author: Gerardo Castro Mata
       Editors:
       Input: None
       Output: Expected Query
       Summary: Scans the GLOBAL data in the HDFS and return the average number of new cases based on a country
       or from all countries.
    */
    public static void getAvgNewCases() throws Exception {
        Scanner keyboard = new Scanner(System.in);
        System.out.print("Would you like to view average new cases per specific country(A) or from all countries(B)?: ");
        String caseResult = keyboard.nextLine();
        caseResult = caseResult.toUpperCase();
        while (!verifyCase(caseResult)) {
            System.out.println("Invalid Input");
            System.out.print("View average new cases by specific country(A) or from all countries(B): ");
            caseResult = keyboard.nextLine();
        }
        if (caseResult.equals("A")) {
            System.out.print("Enter the desired country name: ");
            String country = keyboard.nextLine();
            while (!verifyCountry(country.toUpperCase())) {
                System.out.println("Invalid Country Name.");
                System.out.print("Enter the desired country name: ");
                country = keyboard.nextLine();
            }
            sparkSession.sql("SELECT AVG(new_cases) AS Average_New_Cases " +
                    "FROM GLOBAL " +
                    "WHERE location = '" + country + "';").show();
        } else {
            sparkSession.sql("SELECT location AS Country, AVG(new_cases) AS Average_New_Cases  " +
                    "FROM GLOBAL " +
                    "GROUP BY location " +
                    "ORDER BY Average_New_Cases DESC;").show(1000, false);
        }
    }

    /* OPTION 7 INCOMPLETE
       Function: getLatestCasesDeaths
       Author: Gerardo Castro Mata
       Editors:
       Input: None
       Output: Expected Query
       Summary: Scan the GLOBAL data in the HDFS and prints the latest cases and deaths based on a country.
    */
    public static void getLatestCasesDeaths() throws Exception {
        System.out.print("Enter the desired country: ");
        String country = input.nextLine();
        while (!verifyCountry(country.toUpperCase())) {
            System.out.println("Invalid Country Name.");
            System.out.print("Enter the desired country: ");
            country = input.nextLine();
        }
        sparkSession.sql("SELECT location AS Country, new_cases AS Latest_Cases, total_deaths AS Latest_Deaths " +
                "FROM GLOBAL " +
                "WHERE location = '" + country + "' " +
                "AND date = (SELECT date " +
                "FROM GLOBAL " +
                "GROUP BY date " +
                "ORDER BY date DESC LIMIT 1);").show();

    }
}


