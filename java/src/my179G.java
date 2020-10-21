/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class my179G{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	/*
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 * Author   -> Dr. Mariam Salloum
	 * Modifier -> Dan Murphy
	 * Method   -> my179G (String dbname, String dbport, String user,
	 *                        String passwd) throws SQLException
	 * Purpose  -> Method which creates a new instance of the DB Project and
	 *             serves as an initializing intermediary between the
	 *             server/localhost and the database.
	 * -----------------------------------------------------------------------
	 * @param hostname PSQL server hostname
	 * @param database Name of the database
	 * @param username the user name used to login to the database
	 * @param password the user login password
	 * @throws java.sql.SQLException when failed to make a connection.
	 * -----------------------------------------------------------------------
	 * Receives -> dbname, dbport, user, passwd
	 * Returns  -> NONE
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 */
	public my179G(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");

			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}

	/*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> void executeUpdate (String sql) throws SQLException
   * Purpose  -> Method to execute an update SQL statement.
   *             Update SQL instruction includes the following:
   *             CREATE, INSERT, UPDATE, DELETE, DROP
   * -----------------------------------------------------------------------
   * @param sql the input SQL string
   * @throws java.sql.SQLException when update failed
   * -----------------------------------------------------------------------
   * Receives -> [String] sql
   * Returns  -> NONE
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public void executeUpdate (String sql) throws SQLException {
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> int executeQueryAndPrintResult (String query)
   *                                            throws SQLException
   * Purpose  -> Method to execute an input query SQL instruction (i.e. SELECT).
   *             This method issues the query to the DBMS and outputs the
   *             results to standard out.
   * -----------------------------------------------------------------------
   * @param query the input query string
   * @return the number of rows returned
   * @throws java.sql.SQLException when failed to execute the query
   * -----------------------------------------------------------------------
   * Receives -> [String] sql
   * Returns  -> [int] rowCount
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/*
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 * Author   -> Dr. Mariam Salloum
	 * Modifier -> Dan Murphy
	 * Method   -> List<List<String>> executeQueryAndReturnResult(String query)
	 *                                                       throws SQLException
	 * Purpose  -> Method to execute an input query SQL instruction
	 *             (i.e. SELECT).
	 *             This method issues the query to the DBMS and returns the
	 *             results as a list of records.
	 *             Each record is a list of attribute values.
	 * -----------------------------------------------------------------------
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 * -----------------------------------------------------------------------
	 * Receives -> [String] query
	 * Returns  -> List<List<String>> result
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 * obtains the metadata object for the returned result set.  The metadata
		 * contains row and column info.
		*/
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		/* ------------------------------------------------- */

		/* Iterates through the result set and saves the data
		 * returned by the query.
		 */
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>();
		while (rs.next()){
			List<String> record = new ArrayList<String>();
			for (int i=1; i<=numCol; ++i)
				record.add(rs.getString (i));
			result.add(record);
		}//end while
		stmt.close ();
		return result;
	}//end executeQueryAndReturnResult

	/*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> int executeQuery (String query) throws SQLException
   * Purpose  -> Method to execute an input query SQL instruction
   *             (i.e. SELECT).
   *             This method issues the query to the DBMS and returns the
   *             number of results.
   * -----------------------------------------------------------------------
   * @param query the input query string
   * @return the number of rows returned
   * @throws java.sql.SQLException when failed to execute the query
   * -----------------------------------------------------------------------
   * Receives -> [String] query
   * Returns  -> [int] rowCount
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/*
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 * Author   -> Dr. Mariam Salloum
	 * Modifier -> Dan Murphy
	 * Method   -> int getCurrSeqVal(String sequence) throws SQLException
	 * Purpose  -> Method to fetch the last value from the sequence.
	 *             This method issues the query to the DBMS and returns the
	 *             current value of sequence used for the autogenerated keys.
	 * -----------------------------------------------------------------------
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 * -----------------------------------------------------------------------
	 * Receives -> [String] query
	 * Returns  -> [int] 1 || [int] -1
	 * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	 */
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();

		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

  /*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> void cleanup()
   * Purpose  -> Method to close the physical connection if it is open.
   * -----------------------------------------------------------------------
   * Receives -> NONE
   * Returns  -> NONE
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 *
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */

  /*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> public static void main(String[] args)
   * Purpose  -> Driver method to permit PSQL execution.
   * -----------------------------------------------------------------------
   * Receives -> String[] args
   * Returns  -> NONE
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + my179G.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if

		my179G esql = null;

		try{
			System.out.println("(1)");

			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}

			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];

			esql = new my179G(dbname, dbport, user, "");

			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("0. < EXIT");
				System.out.println("1. Largest number of positive cases by state");
				/* Add further options here for terminal menu input --- */


				switch (readChoice()){
					case 0: keepon = false; break; /* Exit --- */
					case 1: getNumOfSpecifiedOutcomesByState(esql); break;
					/* Add further cases here for respective function calls --- */
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if
			}catch(Exception e){
				// ignored.
			}
		}
	}

	/*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> int readChoice()
   * Purpose  -> Method to read the users choice from the terminal menu.
	 *             Returns only if a correct value is given.
   * -----------------------------------------------------------------------
   * Receives -> NONE
   * Returns  -> NONE
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */
	public static int readChoice() {
		int input;
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	/*
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   * Author   -> Dr. Mariam Salloum
   * Modifier -> Dan Murphy
   * Method   -> void getNumOfSpecifiedOutcomesByState(my179G esql)
   * Purpose  -> Method to add a plane to the DB by reading the users choice
	 *             from the terminal menu.
	 *             Returns only if a correct value is given.
   * -----------------------------------------------------------------------
   * Receives -> my179G esql
   * Returns  -> NONE
   * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
   */

	 /* /// OPTION 1 /// OPTION 1 /// OPTION 1 /// OPTION 1 /// OPTION 1 /// */
	public static void getNumOfSpecifiedOutcomesByState(my179G esql) {

		/* Grab desired case outcome from user to prime the loop --- */
		/* Catch user input to determine if case_outcome is valid --- */

		/*
		while(true) {
			try {
				System.out.println("\tEnter desired case outcome (Positive, Negative, Inconclusive): ");
				case_outcome = in.readLine();
				// IF outcome is valid, proceed to query ---
				if(  !case_outcome.equals("Positive")     && !case_outcome.equals("Negative")
					&& !case_outcome.equals("Inconclusive") && !case_outcome.equals("P")
				  && !case_outcome.equals("N")            && !case_outcome.equals("I")) {
				throw new RuntimeException("Input must be Positive, Negative, Inconclusive, P, N or I");
				}
				break;
			}catch (Exception e) {
				System.out.println(e);
				continue;
			}
		}

		/////////////////////////////////////////////////////////////
		Implement pulling states and state abbreviations from a file
		/////////////////////////////////////////////////////////////

		*/


		/* Try the following query ...
		 * IF valid query, call the method to execute and print the query results
		 * ELSE, exception handle is caught
		 */

	try{
			String query = "SELECT COUNT(S.overall_outcome) FROM Study S WHERE S.overall_outcome LIKE 'In%' AND S.sName LIKE 'Mi%'";

		 System.out.println("\n\n --- EXECUTING QUERY --- \n\n");
		 esql.executeQueryAndPrintResult(query);
		 System.out.println("\n\n --- END OF QUERY RESULTS --- \n\n");
		 }catch(Exception e) {
		 System.err.println(e.getMessage());
	 }// End of try/catch to run the query --- //

 }/* End of method ----------------------------------------------------- */

}/* End of my179G !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! */
