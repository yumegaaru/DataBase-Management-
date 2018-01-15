import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.util.Map;
import java.util.HashMap;

/**
 * Runs queries against a back-end database
 */
public class Query {

	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;
	//This is used to keep track with flight by using itineraries numbers
	private Map<Integer, String[]> searchResult = new HashMap<Integer, String[]>();

	// DB Connection
	private Connection conn;

	// Logged In User
	private String username;
    private int cid; // Unique customer ID

	// Canned queries

       // search (one hop) -- This query ignores the month and year entirely. You can change it to fix the month and year
       // to July 2015 or you can add month and year as extra, optional, arguments
	private static final String SEARCH_ONE_HOP_SQL =
			"SELECT TOP (?) year,month_id,day_of_month,fid,carrier_id,flight_num,origin_city,dest_city, actual_time "
					+ "FROM Flights "
					+ "WHERE year = 2015 AND month_id = 7 AND origin_city = ? AND dest_city = ? AND day_of_month = ? "
					+ "ORDER BY actual_time ASC";
	private PreparedStatement searchOneHopStatement;

	private static final String LOGIN_SQL =
			"SELECT * FROM Customer WHERE name = ? and password = ? ";
	private PreparedStatement loginStatement;

	private static final String SEARCH_TWO_HOP_SQL =
			"SELECT TOP (?) f1.year,f1.month_id,f1.day_of_month,f1.carrier_id as first_carrier,"
					+ "f1.flight_num as first_number,f1.origin_city,f1.actual_time as first_time," 
					+ "f1.fid as first_fid, f2.carrier_id second_carrier,f2.flight_num second_number,"
					+ "f2.origin_city as stop, f2.actual_time as second_time, f2.dest_city, f2.fid as second_fid "
					+ "FROM Flights f1, Flights f2 "
					+ "WHERE f1.year = 2015 AND f1.month_id = 7 "
					+ "AND f1.origin_city = ? " 
					+ "AND f1.dest_city = f2.origin_city AND f1.year = f2.year AND f1.month_id = f2.month_id "
					+ "AND f2.dest_city = ? AND f1.day_of_month = ? AND f2.day_of_month = f1.day_of_month "
					+ "AND f1.actual_time is NOT NULL AND f2.actual_time is NOT NULL "
					+ "ORDER BY f1.actual_time + f2.actual_time ASC";
	private PreparedStatement searchTwoHopStatement;

	//Check the number of passager in the given flight
	private static final String CAPACITY_STATUS_SQL =
			"SELECT * FROM Reservation WHERE first_flight_fid = ? or second_flight_fid = ? ";
	private PreparedStatement capacityStatusStatement;

	//Check whether the user has already book a flight in that given day
	private static final String ONE_PER_DAY_SQL =
			"SELECT * FROM Reservation WHERE cid = ? and day_of_month = ? ";
	private PreparedStatement onePerDayStatement;

	//return how many reservations are exsit 
	private static final String RESERVATION_ID_SQL = 
			"SELECT max(rid) as id FROM Reservation ";
	private PreparedStatement reservationIdStatement;

	//book a valid flight
	private static final String BOOK_SQL = 
			"INSERT INTO Reservation VALUES(?,?,?,?,?,?,?,?,?,?,?)";
	private PreparedStatement bookStatement;

	//list all the reservations of a given user
	private static final String RESERVATION_LIST_SQL = 
			"SELECT * FROM Reservation WHERE cid = ? ";
	private PreparedStatement reservationListStatement;

	//Check if the given reservations id is the the user's reservation
	private static final String VALID_RESERVATION_SQL = 
			"SELECT * FROM Reservation WHERE cid = ? AND rid = ? ";
	private PreparedStatement validReservationStatement;

	//cancel the flight
	private static final String CANCEL_SQL = 
			"DELETE FROM Reservation WHERE cid = ? AND rid = ? ";
	private PreparedStatement cancelStatement;


	// transactions
	private static final String BEGIN_TRANSACTION_SQL =  
			"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;"; 
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;


	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

	/**********************************************************/
	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("flightservice.url");
		jSQLUser	   = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(true); //by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

	}

	public void closeConnection() throws Exception {
		conn.close();
	}

	/**********************************************************/
	/* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {
		searchOneHopStatement = conn.prepareStatement(SEARCH_ONE_HOP_SQL);
 		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		loginStatement = conn.prepareStatement(LOGIN_SQL);
		searchTwoHopStatement = conn.prepareStatement(SEARCH_TWO_HOP_SQL);
		capacityStatusStatement = conn.prepareStatement(CAPACITY_STATUS_SQL);
		onePerDayStatement = conn.prepareStatement(ONE_PER_DAY_SQL);
		bookStatement = conn.prepareStatement(BOOK_SQL);
		reservationIdStatement = conn.prepareStatement(RESERVATION_ID_SQL);
		reservationListStatement = conn.prepareStatement(RESERVATION_LIST_SQL);
		validReservationStatement = conn.prepareStatement(VALID_RESERVATION_SQL);
		cancelStatement = conn.prepareStatement(CANCEL_SQL);
	}
	
	public void transaction_login(String username, String password) throws Exception {
        beginTransaction();
        loginStatement.clearParameters();
        loginStatement.setString(1, username);
        loginStatement.setString(2, password);
        ResultSet userResult = loginStatement.executeQuery();
        if (userResult.next()) {
        	this.username = userResult.getString("name");
        	this.cid = userResult.getInt("cid");
        	System.out.println("\n\nWelcome back " + username);
        } else {
        	System.out.println("\n\nSorry, please type in the valid username and password");
        	rollbackTransaction();
        	return;
        }
        userResult.close();
        commitTransaction();
	}

	/**
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If "directFlight" is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops". Only searches for up to the number of
	 * itineraries given.
	 * Prints the results found by the search.
	 */
	//search command(example) : search "Seattle WA" "Boston MA" 1 14 10
	public void transaction_search_safe(String originCity, String destinationCity, 
		boolean directFlight, int dayOfMonth, int numberOfItineraries) throws Exception {
		
		beginTransaction();
		int itinerariesNumber = 0;
		// one hop itineraries
		searchOneHopStatement.clearParameters();
		searchOneHopStatement.setInt(1, numberOfItineraries);
		searchOneHopStatement.setString(2, originCity);
		searchOneHopStatement.setString(3, destinationCity);
		searchOneHopStatement.setInt(4, dayOfMonth);
		ResultSet oneHopResults = searchOneHopStatement.executeQuery();
		System.out.println("Here are direct Flights! \n");
		while (oneHopResults.next()) {
			itinerariesNumber++;
            int result_year = oneHopResults.getInt("year");
            int result_monthId = oneHopResults.getInt("month_id");
            int result_dayOfMonth = oneHopResults.getInt("day_of_month");
            String result_carrierId = oneHopResults.getString("carrier_id");
            String result_flightNum = oneHopResults.getString("flight_num");
            String result_originCity = oneHopResults.getString("origin_city");
            String result_destCity = oneHopResults.getString("dest_city");
            int result_time = oneHopResults.getInt("actual_time");
            int result_fid = oneHopResults.getInt("fid");
            String result[] = new String[1];
            result[0] = result_fid + "," + result_dayOfMonth + "," + result_year + "," 
            	+ result_monthId + "," + result_carrierId + "," + result_flightNum + "," 
            	+ result_originCity + "," + result_destCity + "," + result_time;
            System.out.println("Flight: " + itinerariesNumber + ","+ result[0]);
            searchResult.put(itinerariesNumber, result);	                 
  		}
		oneHopResults.close();

		// two hop itineraries
		if (!directFlight) {
			System.out.println("\n\nHere are Flights with one stop! \n");
			searchTwoHopStatement.clearParameters();
			searchTwoHopStatement.setInt(1, numberOfItineraries - itinerariesNumber);
			searchTwoHopStatement.setString(2, originCity);
			searchTwoHopStatement.setString(3, destinationCity);
			searchTwoHopStatement.setInt(4, dayOfMonth);
			ResultSet twoHopResults = searchTwoHopStatement.executeQuery();
			while (twoHopResults.next()) {
				itinerariesNumber++;
                int result_year = twoHopResults.getInt("year");
                int result_monthId = twoHopResults.getInt("month_id");
                int result_dayOfMonth = twoHopResults.getInt("day_of_month");
                String result_carrierId_one = twoHopResults.getString("first_carrier");
                String result_carrierId_two = twoHopResults.getString("second_carrier");
                String result_flightNum_one = twoHopResults.getString("first_number");
                String result_flightNum_two = twoHopResults.getString("second_number");
                String result_originCity = twoHopResults.getString("origin_city");
                String result_stop = twoHopResults.getString("stop");
                int result_fid_one = twoHopResults.getInt("first_fid");
                int result_fid_two = twoHopResults.getInt("second_fid");
                int result_time_one = twoHopResults.getInt("first_time");
                int result_time_two = twoHopResults.getInt("second_time");
                String result_dest = twoHopResults.getString("dest_city");
                String result[] = new String[2];
                result[0] = result_fid_one + "," + result_dayOfMonth + "," + result_originCity 
                	+ "," + result_carrierId_one + "," + result_flightNum_one + "," 
                	+ result_stop + "," + result_time_one + "," + result_year + "," + result_monthId;
                result[1] = result_fid_two + "," + result_dayOfMonth + "," + result_stop + "," 
                	+ result_carrierId_two + "," + result_flightNum_two + "," + result_dest 
                	+ "," + result_time_two + "," + result_year + "," + result_monthId;
                System.out.println("First Flight : " + itinerariesNumber + ","+ result[0] + "\n");
                System.out.println("Second Flight: " + itinerariesNumber + ","+ result[1] + "\n\n");
                searchResult.put(itinerariesNumber, result);
	  		}
			twoHopResults.close();
		} 
		commitTransaction();
	}
	
	//book command(example) : book 1 (is to book the first flight)
	public void transaction_book(int itineraryId) throws Exception {
		if (cid == 0 || username == null) {
			System.out.println("Sorry, my dear customer, please Login first");
			return;
		}
		if (searchResult.isEmpty()) { // 
			System.out.println("Sorry " + username + ", Please search the flights first");
			return;
		}
		if (!searchResult.containsKey(itineraryId)) {
			System.out.println("Sorry " + username 
				+ ", please type a valid itineraryId in the most recent search result");
			return;
		}
		beginTransaction();
		String[] flight = searchResult.get(itineraryId);
		for (int i = 0; i < flight.length; i++) {
			String[] token = flight[i].split(",");
			int fid = Integer.parseInt(token[0]);
			int day_of_month = Integer.parseInt(token[1]);
			//check the current capacity status
			if (fullCapacity(fid)) {
				System.out.println("Sorry, this flight has already fully booked. Please book others");
				rollbackTransaction();
				return;
			}
			//check whether at most one reservation at any given day
			//what about the second flight?
			if (manyPerDay(day_of_month)) {
				System.out.println("Sorry, you have already booked one flight in this day");
				rollbackTransaction();
				return;
			}
		}

		bookStatement.clearParameters();
		int rid = reservationId() + 1;
		bookStatement.setInt(1,rid);
		bookStatement.setInt(2,cid);
		//book one hop flight
		if (flight.length == 1) {
			String[] token = flight[0].split(",");
			int fid = Integer.parseInt(token[0]);
			int day_of_month = Integer.parseInt(token[1]);
			bookStatement.setInt(3,fid);
			bookStatement.setNull(4, java.sql.Types.INTEGER);
			bookStatement.setString(5,token[4]);
			bookStatement.setNull(6, java.sql.Types.VARCHAR);
			bookStatement.setInt(7,day_of_month);				
			bookStatement.setString(8,token[6]);
			bookStatement.setNull(9, java.sql.Types.VARCHAR);
			bookStatement.setString(10,token[7]);
			bookStatement.setString(11,token[8]);

		} else {
		//book two hop flight
			String[] token0 = flight[0].split(",");
			String[] token1 = flight[1].split(",");
			int first_fid = Integer.parseInt(token0[0]);
			int second_fid = Integer.parseInt(token1[0]);
			int day_of_month = Integer.parseInt(token0[1]);
			int first_time = Integer.parseInt(token0[6]);
			int second_time = Integer.parseInt(token1[6]);
			int time = first_time + second_time;
			bookStatement.setInt(3,first_fid);
			bookStatement.setInt(4,second_fid);
			bookStatement.setString(5,token0[3]);
			bookStatement.setString(6,token1[3]);
			bookStatement.setInt(7,day_of_month);				
			bookStatement.setString(8,token0[2]);
			bookStatement.setString(9,token0[5]);
			bookStatement.setString(10,token1[5]);
			bookStatement.setInt(11,time);
		}
		bookStatement.executeUpdate();
		commitTransaction();
		System.out.println("Congradulations! You have successfully book a flight, enjoy your trip");           
	}

	public void transaction_reservations() throws Exception {
		if (cid == 0 || username == null) {
			System.out.println("Sorry, my dear customer, please Login first");
			return;
		}
		beginTransaction();
		reservationListStatement.clearParameters();
		reservationListStatement.setInt(1, cid);
		ResultSet list = reservationListStatement.executeQuery();
		System.out.println("Here are the reservations\n\n");
		int number = 0;
		while (list.next()) {
			number++;
			int result_rid = list.getInt("rid");
			int result_fid_one = list.getInt("first_flight_fid");
			int result_fid_two = list.getInt("second_flight_fid");
			String result_carrierId_one = list.getString("first_carrier_id");
			String result_carrierId_two = list.getString("second_carrier_id");	
            int result_dayOfMonth = list.getInt("day_of_month");
			String result_originCity = list.getString("origin_city");
			String result_stop = list.getString("stop");
			String result_destCity = list.getString("dest_city");
			int result_time = list.getInt("actual_time");
			System.out.println("Reservation: " + result_rid + "(rid), " + result_fid_one + "(fid1), " 
				+ result_fid_two + "(fid2), " + result_carrierId_one + "(carrier_id1), " + result_carrierId_one 
				+ "(carrier_id2), " + result_dayOfMonth + "(day of month), " +  result_originCity + "(originCity), " 
				+result_stop + "(stop), " + result_destCity + "(destCity), " +result_time + "(time)");

		}
		if (number == 0) {
			System.out.println("Sorry, you do not have any reservations yet");
			rollbackTransaction();
			return;
		}
		list.close();
		commitTransaction();
	}

	//cancel command (ex) : cancel 12      (12 is the rid)
	public void transaction_cancel(int reservationId) throws Exception {
		if (cid == 0 || username == null) {
			System.out.println("Sorry, my dear customer, please Login first");
			return;
		}
		beginTransaction();
		if (validReservation(reservationId)) {
			cancelStatement.clearParameters();
			cancelStatement.setInt(1, cid);
			cancelStatement.setInt(2, reservationId);
			cancelStatement.executeUpdate();
			commitTransaction();
			System.out.println("You cancel the reservation successfully!");
		} else {
			System.out.println("Sorry, please provide the valid reservation Id");
			rollbackTransaction();
		}

	}

	private boolean validReservation(int reservationId) throws Exception {
		validReservationStatement.clearParameters();
		validReservationStatement.setInt(1, cid);
		validReservationStatement.setInt(2, reservationId);
		ResultSet result = validReservationStatement.executeQuery();
		boolean valid = result.next();
		result.close();
		return valid;
	}

	private boolean fullCapacity(int fid) throws Exception {
		capacityStatusStatement.clearParameters();
		capacityStatusStatement.setInt(1, fid);
		capacityStatusStatement.setInt(2, fid);
		ResultSet capacityStatus = capacityStatusStatement.executeQuery();
		int num = 0;
		while (capacityStatus.next()) {
			num++;
		}
		capacityStatus.close();
		return num >= 3;

	}

	private boolean manyPerDay(int day_of_month) throws Exception {
		onePerDayStatement.clearParameters();
		onePerDayStatement.setInt(1,cid);
		onePerDayStatement.setInt(2,day_of_month);
		ResultSet number_per_day = onePerDayStatement.executeQuery();
		boolean manyday = number_per_day.next();
		number_per_day.close();
		return manyday;
	}

	private int reservationId() throws Exception {
		int maxId = 0;
		reservationIdStatement.clearParameters();
		ResultSet id = reservationIdStatement.executeQuery();
		while(id.next()) {
			maxId = id.getInt("id");
		}	
		id.close();
		return maxId;
	}

    
    public void beginTransaction() throws Exception {
        conn.setAutoCommit(false);
        beginTransactionStatement.executeUpdate();  
    }

    public void commitTransaction() throws Exception {
        commitTransactionStatement.executeUpdate(); 
        conn.setAutoCommit(true);
    }
    public void rollbackTransaction() throws Exception {
        rollbackTransactionStatement.executeUpdate();
        conn.setAutoCommit(true);
    } 

}
