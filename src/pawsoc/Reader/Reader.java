package pawsoc.Reader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import pawsoc.Config.ConfigLoader;

/**
 * Diese Klasse kann sowohl fuer postgesql, als auch fuer mysql konfiguriert<br>
 * werden und liest alle in dem uebergebenen ConfigLoader durch die<br>
 * Konfigurationsdatei spezifizierten Spalten und Tabellen aus und speichert<br>
 * diese in eine ArrayList aus String[][], wobei jedes String[][] eine Tabelle<br>
 * repräsentiert.
 * 
 * @author Gabriel Pawlowsky
 * 
 */
public class Reader {

	// ConfigLoader, in dem gespeichert ist welche Tabellen und Spalten gelesen
	// werden sollen.
	private ConfigLoader cl;

	// Die Treiber fuer die Verbindung zu postgresql und mysql
	static final String JDBC_DRIVER_MYSQL = "com.mysql.jdbc.Driver";
	static final String JDBC_DRIVER_POSTGRESQL = "org.postgresql.Driver";

	// In diesem boolean wird gespeichert, ob man gerade mit der Datenbank
	// verbunden ist oder nicht
	private boolean connectedToDatabase = false;

	// Verbindung und Statement für die DB-Verbindung
	private Connection connection;
	private Statement statement;

	// Hierin werden die kompletten ausgelsenen Daten gespeichert.
	private ArrayList<String[][]> fullData;

	/**
	 * Konstruktor, der die Parameter speichert und je nach spezifiziertem dbms<br>
	 * die Lesenoperation mit den richtigen Daten startet.
	 * 
	 * @param cl
	 *            ConfigLoader der die Daten enthaelt
	 * @param dbms
	 *            spezifiziertes DBMS. Moeglich sind "postgresql" und "mysql"
	 * @throws ClassNotFoundException
	 *             Exception wird ausgeloest, wenn die Treiberklasse nicht<br>
	 *             gefunden werden kann
	 * @throws SQLException
	 *             wird ausgeloest, wenn die DB-Verbindung nicht ordnungsgemaess<br>
	 *             aufgebaut werden kann
	 */
	public Reader(ConfigLoader cl, String dbms) throws ClassNotFoundException,
			SQLException {
		// Speichern des Paramters, um Daten aus dem Config-File buntzen zu
		// koennen
		this.cl = cl;

		this.fullData = new ArrayList<String[][]>();

		// fuerhre je nach uebergabeparameter die lese-Methode mit den richtigen
		// daten(usernmae, passwort, ...) zum spezifizierten dbms aus
		if (dbms.equalsIgnoreCase("mysql"))
			readData(JDBC_DRIVER_MYSQL, "jdbc:mysql://" + cl.getMyAddress()
					+ "/" + cl.getMyDatabase(), cl.getMyUser(),
					cl.getMyPassword(), 0);
		else if (dbms.equalsIgnoreCase("postgresql"))
			readData(
					JDBC_DRIVER_POSTGRESQL,
					"jdbc:postgresql://" + cl.getPosAddress() + "/"
							+ cl.getPosDatabase(), cl.getPosUser(),
					cl.getPosPassword(), 1);
	}

	/**
	 * Diese Methode liest mit den uebergebenen Daten, alle spezifizierten Daten<br>
	 * aus der Datenbank und speichert diese in das Attribut fullData
	 * 
	 * @param driver
	 *            Treiberklassenbezeichnung fuer den Treiber des spezifizierten<br>
	 *            DBMS
	 * @param url
	 *            url zur Datenbank
	 * @param username
	 *            Usernamen auf der Datenbank
	 * @param password
	 *            Passwort zum angegeben Usernamen auf der Datenbank
	 * @param postgresqlIncrementor
	 *            Da im configLoader alle Daten im Index um einen hoeher sind<br>
	 *            als bei den gespeicherten mysql Daten, muss man bei der<br>
	 *            Benutzung dieser Methode, um von einer postgresql-Datenbank zu<br>
	 *            lesen, muss man 1 spezifizieren, damit in der Methode di<br>
	 *            richtigen Daten gelesen werden. Bei mysql muss 0 uebergeben<br>
	 *            werden
	 * @throws ClassNotFoundException
	 *             Exception wird ausgeloest, wenn die Treiberklasse nicht<br>
	 *             gefunden werden kann
	 * @throws SQLException
	 *             wird ausgeloest, wenn die DB-Verbindung nicht ordnungsgemaess<br>
	 *             aufgebaut werden kann
	 */
	private void readData(String driver, String url, String username,
			String password, int postgresqlIncrementor)
			throws ClassNotFoundException, SQLException {
		// Gibt die Klasse des Treibers zurueck
		Class.forName(driver);

		// Erstellen der Datenbankverbindung mit hilfe der uebergebenen Werte
		connection = DriverManager.getConnection(url, username, password);

		// Erzeugen eines Statements mithilfe der Verbindung
		statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_READ_ONLY);

		// Speichern, dass die Datenbank nun Verbunden ist, falls alles
		// fehlerfrei abgelaufen ist
		connectedToDatabase = true;

		// Itteriert durch jede Tabelle und selected deren relevante Inhalte
		for (int i = 0; i < cl.getCountTable(); i++) {
			// Speichert mit Beistrich getrennt alle Spalten die gelesen werden
			// sollen
			String columnsToBeSelected = "";
			for (int j = 0; j < cl.getVars().length; j++) {
				if (cl.getVars()[j][0 + postgresqlIncrementor * 2].equals(cl
						.getTables()[i][0 + postgresqlIncrementor])
						&& !cl.getVars()[j][1].equalsIgnoreCase("-")
						&& !cl.getVars()[j][3].equalsIgnoreCase("-")) {
					columnsToBeSelected += cl.getVars()[j][1 + postgresqlIncrementor * 2];
					columnsToBeSelected += ",";
				}
			}
			// bei dem letzten Eintrag soll noch der letzte Beistrich entfernt
			// werden
			columnsToBeSelected = columnsToBeSelected.substring(0,
					columnsToBeSelected.length() - 1);
			// Ausfuehren der entstehenden Query, die die Inhalte richtigen
			// Spalten jeder Tabelle einzeln
			// bei den Schleifendurchlaufen ermitteln soll
			fullData.add(convertResultSetToArray(setQuery("SELECT "
					+ columnsToBeSelected + " FROM "
					+ cl.getTables()[i][0 + postgresqlIncrementor])));
		}
		statement.close();
		connection.close();
	}

	/**
	 * Methode, die eine nicht-Datenbank-verändernde Query ausfuehrt
	 * 
	 * @param query
	 *            Query, die ausgefuehrt werden soll
	 * @throws SQLException
	 *             Exception, die bei SQL Fehlern geworfen wird
	 * @throws IllegalStateException
	 *             Exception, die geworfen wird wenn die Datenbank nicht<br>
	 *             Verbunden ist
	 */
	private ResultSet setQuery(String query) throws SQLException,
			IllegalStateException {
		// Ueberpruefung, ob die Datenbank-Verbindung in Ordnung ist,
		// anderenfalls werfen einer Exception
		if (!connectedToDatabase)
			throw new IllegalStateException("Not Connected to Database");

		// Ausfuehren der Query
		ResultSet resultSet = statement.executeQuery(query);

		// Setzen des Cursors auf das letzte Elemement
		// resultSet.last();

		return resultSet;
	}

	/**
	 * @return the fullData
	 */
	public ArrayList<String[][]> getFullData() {
		return fullData;
	}

	/**
	 * Diese Methode konvertiert ein ResultSet zu einem String[][] Array damit <br>
	 * man es in die uebergeordnete ArrayList speichern kann
	 * 
	 * @param rs
	 *            Das ResultSet das konvertiert werden soll
	 * @return ein Array fuer das Gesamtergebnis
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public String[][] convertResultSetToArray(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		@SuppressWarnings("rawtypes")
		List rows = new ArrayList();
		while (rs.next()) {
			String[] row = new String[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				row[i - 1] = rs.getString(i);
			}
			rows.add(row);
		}

		rs.close();

		return (String[][]) rows.toArray(new String[rows.size()][columnCount]);
	}
}
