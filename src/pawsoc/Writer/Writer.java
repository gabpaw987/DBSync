package pawsoc.Writer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import pawsoc.Config.ConfigLoader;

/**
 * Diese Klasse kann sowohl fuer postgesql, als auch fuer mysql konfiguriert<br>
 * werden und loescht zuerst alle Daten aus allen durch den ConfigLoader<br>
 * spezifizierten Tabellen. Anschliessend werden alle in der ArrayList fullData<br>
 * beschriebenen Daten in die leeren Tabellen und wird mit dem Reader insgesamt<br>
 * zum Ausfuerhen der eigentlichen Synchronisation benutzt.
 * 
 * @author Gabriel Pawlowsky & Josef Sochovsky
 */
public class Writer {
	// ConfigLoader, in dem gespeichert ist welche Tabellen und Spalten
	// geloescht und geschrieben werden sollen.
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

	// Hierin sind die kompletten Daten gespeichert, die in alle Tabellen
	// gespeichert werden sollen
	private ArrayList<String[][]> fullData;

	/**
	 * Konstruktor, der die Parameter speichert und je nach spezifiziertem dbms<br>
	 * die Schreiboperation mit den richtigen Daten startet.
	 * 
	 * @param cl
	 *            ConfigLoader der die Daten enthaelt
	 * @param dbms
	 *            spezifiziertes DBMS. Moeglich sind "postgresql" und "mysql"
	 * @param fullData
	 *            Die Daten die geschrieben werden sollen
	 * @throws ClassNotFoundException
	 *             Exception wird ausgeloest, wenn die Treiberklasse nicht<br>
	 *             gefunden werden kann
	 * @throws SQLException
	 *             wird ausgeloest, wenn die DB-Verbindung nicht ordnungsgemaess<br>
	 *             aufgebaut werden kann
	 */
	public Writer(ConfigLoader cl, String dbms, ArrayList<String[][]> fullData)
			throws ClassNotFoundException, SQLException {
		// Speichern des Paramters, um Daten aus dem Config-File buntzen zu
		// koennen
		this.cl = cl;

		this.fullData = fullData;

		// fuerhre je nach uebergabeparameter die schreib-Methode mit den
		// richtigen daten(usernmae, passwort, ...) zum spezifizierten dbms aus
		if (dbms.equalsIgnoreCase("mysql"))
			writeData(JDBC_DRIVER_MYSQL, "jdbc:mysql://" + cl.getMyAddress()
					+ "/" + cl.getMyDatabase(), cl.getMyUser(),
					cl.getMyPassword(), 0);
		else if (dbms.equalsIgnoreCase("postgresql"))
			writeData(
					JDBC_DRIVER_POSTGRESQL,
					"jdbc:postgresql://" + cl.getPosAddress() + "/"
							+ cl.getPosDatabase(), cl.getPosUser(),
					cl.getPosPassword(), 1);
	}

	/**
	 * Diese Methode wird benutzt, um mit den uebergeben Daten in eine Datenbank<br>
	 * die im Attribut fullData angegebenen Daten zu schreiben. Ausserdem wird<br>
	 * zuvor die Tabelle geleert, bevor die neuen Daten hinein geschrieben<br>
	 * werden.
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
	private void writeData(String driver, String url, String username,
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
			// Delete
			setUpdateQuery("DELETE FROM "
					+ cl.getTables()[i][0 + postgresqlIncrementor]);

			// Speichert mit Beistrich getrennt alle Spalten die gelesen werden
			// sollen
			String columnsToBeInserted = "";
			// In dieser Liste werden bei jedem Wert noch jeweils die richtigen
			// Datentypen mit gespeichert.
			ArrayList<String> datatype = new ArrayList<String>();
			for (int j = 0; j < cl.getVars().length; j++) {
				if (cl.getVars()[j][0 + postgresqlIncrementor * 2].equals(cl
						.getTables()[i][0 + postgresqlIncrementor])
						&& !cl.getVars()[j][1].equalsIgnoreCase("-")
						&& !cl.getVars()[j][3].equalsIgnoreCase("-")) {
					datatype.add(cl.getVars()[j][4]);
					columnsToBeInserted += cl.getVars()[j][1 + postgresqlIncrementor * 2];
					columnsToBeInserted += ",";
				}
			}
			// bei dem letzten Eintrag soll noch der letzte Beistrich entfernt
			// werden
			columnsToBeInserted = columnsToBeInserted.substring(0,
					columnsToBeInserted.length() - 1);

			// In dieser Schleife werden alle Inserts einer Tabelle
			// durchgefuehrt
			for (int j = 0; j < fullData.get(i).length; j++) {
				String values = "";
				// Diese Schleife sorgt dafuer, alle Werte, die zu Inserten sind
				// mit dem richtigen Datentyp zu assoziieren
				// und somit die richtigen Schreibweise z.B. beoi string mit ''
				// zu waehlen
				for (int j2 = 0; j2 < fullData.get(i)[j].length; j2++) {
					System.out.println(fullData.get(i)[j][j2]);
					if ((fullData.get(i)[j][j2]) == null)
						values += fullData.get(i)[j][j2] + ",";
					else if (datatype.get(j2).startsWith("number")) {
						values += fullData.get(i)[j][j2] + ",";
					} else if (datatype.get(j2).startsWith("string")) {
						values += "'" + fullData.get(i)[j][j2] + "',";
					}
				}
				// Der letzte Beistrich muss entfernt werden
				values = values.substring(0, values.length() - 1);

				// Insert durchfuehren
				System.out.println("INSERT INTO "
						+ cl.getTables()[i][0 + postgresqlIncrementor] + " ("
						+ columnsToBeInserted + ") VALUES(" + values + ")");
				setUpdateQuery("INSERT INTO "
						+ cl.getTables()[i][0 + postgresqlIncrementor] + " ("
						+ columnsToBeInserted + ") VALUES(" + values + ")");

			}
		}
		statement.close();
	}

	/**
	 * Methode, die eine Datenbank-verändernde Query ausfuehrt
	 * 
	 * @param updateQuery
	 *            Query, die ausgefuehrt werden soll
	 * @throws SQLException
	 *             Exception, die bei SQL Fehlern geworfen wird
	 * @throws IllegalStateException
	 *             Exception, die geworfen wird wenn die Datenbank nicht<br>
	 *             Verbunden ist
	 */
	public void setUpdateQuery(String updateQuery) throws SQLException,
			IllegalStateException {
		// Ueberpruefung, ob die Datenbank-Verbindung in Ordnung ist,
		// anderenfalls werfen einer Exception
		if (!connectedToDatabase)
			throw new IllegalStateException("Not Connected to Database");

		// Ausfuehren der updateQuery
		int affectedRows = statement.executeUpdate(updateQuery);

		// Ausgabe der Anzahl an bearbeiteten Reihen
		System.out.println(affectedRows + " Row/s are affected!");
	}

}
