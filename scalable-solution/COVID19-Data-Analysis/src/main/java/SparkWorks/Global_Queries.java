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
        Input: String, SparkSession
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
        Editors: Dan Murphy
        Input: None
        Output: String
        Summary: Prompts the user to enter the case result they want to query.
    */
    private static String getCase() {
        Scanner input = new Scanner(System.in);
        System.out.print("Which result would you like to view ([P]ositive/[N]egative/[I]nconclusive)/[A]ll): ");
        String choice = input.nextLine();
        while (!verifyChoice(choice)) {
            System.out.println("Invalid Input");
            System.out.print("Which result would you like to view (Positive/Negative/Inconclusive: ");
            choice = input.nextLine();
        }

        return choice;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> bool verifyChoice(String choice)
     * Purpose  -> Helper method to determine valid input choice.
     * -----------------------------------------------------------------------
     * Receives -> String choice
     * Returns  -> boolean
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static boolean verifyChoice(String choice) {
        switch(choice) {
            case "1":
            case "2":
                return true;
            default:
                return false;
        }
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> String getCountry()
     * Purpose  -> Helper method to determine valid input choice.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> String country
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
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
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> String getContinent()
     * Purpose  -> Method to grab continent user input.
     * -----------------------------------------------------------------------
     * Receives -> String
     * Returns  -> String
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static String getContinent() throws Exception {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter the desired continent name: ");
        String continent = input.nextLine();
        while (!verifyContinent(continent.toUpperCase())) {
            System.out.println("Invalid Continent Name.");
            System.out.print("Enter the desired continent name: ");
            continent = input.nextLine();
        }
        return continent;
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> String verifyContinent()
     * Purpose  -> Method to verify continent user input through checking the
     *             list of valid continents in a text file.
     * -----------------------------------------------------------------------
     * Receives -> String
     * Returns  -> String
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
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
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> String reformatInput()
     * Purpose  -> Converts string to title case.
     *             united states -> United States
     * -----------------------------------------------------------------------
     * Receives -> String
     * Returns  -> String
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static String reformatInput(String state) {
        Scanner scan = new Scanner(state);
        String upperCase = "";
        if (state.length() == 1 || state.length() == 2) return state.toUpperCase();
        while(scan.hasNext()){
            String fix = scan.next();
            upperCase += Character.toUpperCase(fix.charAt(0))+ fix.substring(1) + " ";
        }
        return(upperCase.trim());
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void getNumOfTestsAdministeredByCountry()
     * Purpose  -> Method to get the greatest number of positive, negative or
     *             inconclusive COVID-19 cases in a specific country.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void getNumOfTestsAdministeredByCountry() throws Exception {
        String country = getCountry();
        country = reformatInput(country);
        if (country.length() == 3)
            sparkSession.sql("SELECT COUNT(*) " + "FROM GLOBAL " + "WHERE iso_code = '" + country + "';").show();
        else
            sparkSession.sql("SELECT COUNT(*) " + "FROM GLOBAL " + "WHERE location = '" + country + "';").show();

    } // ---------------------------------------------------------------------

    /* OPTION 2 COMPLETE
       Function: getLargestNumOfCasesInAnOrderedList
       Author: Daniel Murphy
       Editors: Gerardo Castro Mata
       Input: None
       Output: Executed Query
       Summary: Scans the GLOBAL data in the HDFS to print the information regarding the
       largest number of cases in an ordered list within a country.

    */

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Modifier -> Gerardo Castro Mata
     * Method   -> void getLargestNumOfCasesInAnOrderedList()
     * Purpose  -> Method to get the greatest number of tests administered
     *			   by a specified country.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
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

    /* OPTION 3 COMPLETE
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



    /* OPTION 4 COMPLETE
   Function: getAvgNewCases
   Author: Gerardo Castro Mata, Dan Murphy
   Input: None
   Output: Expected Query
   Summary: Scans the GLOBAL data in the HDFS and return the average number of new cases based on a country
   or from all countries.
*/
    public static void getAvgNewCases() throws Exception {
        Scanner keyboard = new Scanner(System.in);
        System.out.print("View average new cases per:\n1. Specific country\nor\n2. All countries\n ");
        String choice = keyboard.nextLine();
        choice = choice.toUpperCase();
        while (!verifyChoice(choice)) {
            System.out.println("Invalid Input");
            System.out.print("View average new cases by specific country(A) or from all countries(B): ");
            choice = keyboard.nextLine();
        }
        if (choice.equals("1")) {
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
        } else if (choice.equals("2")){
            sparkSession.sql("SELECT location AS Country, AVG(new_cases) AS Average_New_Cases  " +
                    "FROM GLOBAL " +
                    "GROUP BY location " +
                    "ORDER BY Average_New_Cases DESC;").show(1000, false);
        }
        else{
            System.out.println("Invalid input.\n\n");
        }
    }

    /* OPTION 5 COMPLETE
   Function: getLatestCasesDeaths
   Author: Gerardo Castro Mata, Dan Murphy
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

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void topKTotalCasesReportedByCountry()
     * Purpose  -> Method to return the top K results given a case outcome,
     *			   specified date and the value for K countries to list.
     *             Filters by total_cases in descending order.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    /* /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// */
    public static void topKTotalCasesReportedByCountry() throws Exception {
        Scanner input = new Scanner(System.in);
        String continent = getContinent();
        //continent = reformatInput(continent);
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
        sparkSession.sql("SELECT location, total_cases, new_cases, total_cases_per_million, new_cases_per_million" +
                                "FROM Global WHERE '" + date + "' = date " +
                                "and continent = '" + continent +
                                "' ORDER BY total_cases DESC;").show(K);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void topKDeathsReportedByCountry()
     * Purpose  -> Method to return the top K results given a case outcome,
     *			   specified date and the value for K countries to list.
     *             Filters by total_deaths in descending order.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    /* /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// */
    public static void topKDeathsReportedByCountry() throws Exception {
        Scanner input = new Scanner(System.in);
        String continent = getContinent();

        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String date = input.nextLine();

        System.out.print("Enter the list size you want to see: ");
        int K = input.nextInt();
        while (K < 1) {
            System.out.println("Invalid Input");
            K = input.nextInt();
        }
        sparkSession.sql("SELECT location, total_deaths, new_cases, total_cases_per_million, new_cases_per_million" +
                                "FROM Global WHERE '" + date + "' = date " +
                                "and continent = '" + continent + "' ORDER BY total_deaths DESC;").show(K);
    }

}


