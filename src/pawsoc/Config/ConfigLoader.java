package pawsoc.Config;

/**
 * Diese Klasse ist dazu da den Inhalt der config Datei zu interpretieren dafuer <br>
 * muss man zunaechst den Inhalt auslesen (Klasse GetConfig) hat man den Inhalt <br>
 * muss diese Klasse alle Informationen ueber den Inhalt abrufbar und <br>
 * vernuenftig an die anderen Klassen weitergeben koennen.<br>
 * 
 * In der Dokumentation des Programmes findet sich die genaue Anleitung wie man<br>
 * die Config Datei schreibt, daraus laesst sich dann auch direkt ableiten wie<br>
 * der Interpreter damit umgehen muss
 * 
 * @author Josef Sochovsky
 * @version 1.0
 */
public class ConfigLoader {
	// der Inhalt der Config Datei
	private String[] inhalt;

	// in diesen Variablen werden die Anmeldeinformationen fuer MySQL
	// gespeichert
	private String myAddress;
	private String myUser;
	private String myPassword;
	private String myDatabase;

	// in diesen Variablen werden die Anmeldeinformationen fuer PostgreSQL
	// gespeichert
	private String posAddress;
	private String posUser;
	private String posPassword;
	private String posDatabase;

	// hier wird die Anzahl an Tabellen gepsiechert
	private int countTable;

	// hier werden alle Tabellennamen in einem String Array gespeichert
	// zusatzlich befinden sich hier Informationen wie synchronisiert werden
	// Eintrag 0 ist immer der Name der Tabelle in mySQL dann in PostgreSQL, und
	// danach die Art der Synchronisation
	private String[][] tables;

	// in diesem Array findet man Informationen ueber die Variablen und deren
	// Datentypen in spezifischen Tabellen
	// Hier ist der 0 Eintrag in welcher Tabelle man sich in mysql befindet dann
	// der Name aus MySQL
	// dann der Tabellenname in PostgreSQL und der Name dort und zuletzt der
	// Datentyp
	private String[][] vars;

	/**
	 * Dieser Konstruktor erhaelt den Inhalt der Datei in einem String Array<br>
	 * leitet dann weiter an die Methode interpret <br>
	 * Ausserdem muessen die Klassenvariablen initialisiert werden <br>
	 */
	public ConfigLoader(String file) {
		GetConfig getConfig = new GetConfig(file);
		inhalt = getConfig.getResult();
		countTable = 0;
		tables = new String[0][3];
		vars = new String[0][5];
		interpret();
	}

	/**
	 * Diese Methode interpretiert den gesamten Inhalt der Datei die ausgelesen <br>
	 * wurde und speichert die Informationen in abrufbare Variablen <br>
	 */
	private void interpret() {
		// erste Zeile muss [mysql entsprechen]
		if (!inhalt[0].equalsIgnoreCase("[MySQL]"))
			System.err.println("Das ConfigFile ist falsch geschrieben");

		// nun muessen die Anmeldeinformationen fuer mysql stehen
		String[] splitLine = inhalt[1].split(" ");
		myAddress = splitLine[0];
		myUser = splitLine[1];
		myPassword = splitLine[2];
		myDatabase = splitLine[3];

		// erste Zeile muss [postgreSQL entsprechen]
		if (!inhalt[2].equalsIgnoreCase("[PostgreSQL]"))
			System.err.println("Das ConfigFile ist falsch geschrieben");

		// nun muessen die AnmeldeInformation fuer postgresql stehen
		splitLine = inhalt[3].split(" ");
		posAddress = splitLine[0];
		posUser = splitLine[1];
		posPassword = splitLine[2];
		posDatabase = splitLine[3];

		// suchen aller Strichpunkte damit man weiss wie viele Tabellen
		// konfiguriert sind
		for (int i = 4; i < inhalt.length; i++) {
			if (inhalt[i].equals(";"))
				countTable++;
		}

		// nachdem man jetzt weiss wie viele Tabellen existieren kann man diese
		// trennen und danach durchgehen
		// in dieser Variable werden die TeilArrays gepsiechert
		String[][] tableSplit = new String[0][];
		String[] teilTable = new String[0];
		for (int j = 4; j < inhalt.length; j++) {
			// wenn ein ; gefunden wird dann kann man das als eine Tabelle
			// ansehen
			if (inhalt[j].equals(";")) {
				tableSplit = incrementStringStringArray(tableSplit, teilTable);
				teilTable = new String[0];
				// damit der ; uebersprungen wird
				j++;
			}
			if (j < inhalt.length)
				// in diesem Array wird immer eine Tabelle zusammengebastelt
				teilTable = incrementStringArray(teilTable, inhalt[j]);
		}
		for (int i = 0; i < tableSplit.length; i++) {
			// hier befinden sich die Namen der Tabelle in den verschiedenen
			// Datenbanken und danach steht die sync art
			String[] namen = tableSplit[i][0].split(" ");
			String[] tableInfo = { namen[0], namen[1], namen[2] };
			tables = incrementStringStringArray(tables, tableInfo);

			for (int j = 1; j < tableSplit[i].length; j++) {
				String[] columns = tableSplit[i][j].split(" ");
				String[] varInfo = { tableInfo[0], columns[0], tableInfo[1],
						columns[1], columns[2] };
				vars = incrementStringStringArray(vars, varInfo);
			}
		}
	}

	/**
	 * Diese Methode erhoeht ein 2dstring Array um einen Eintrag
	 * 
	 * @param array
	 *            das Array das vorhanden war
	 * @param text
	 *            das mit dem erhoeht wird
	 * @return das neue Array
	 */
	public String[] incrementStringArray(String[] array, String text) {
		// erstellen eines neuen Arrays das um eins groesser ist als das
		// vorherige
		String[] tempArray = new String[array.length + 1];
		// Schleife uebertraegt jede Spanlte des Arrays in den Zwischenspeicher
		for (int i = 0; i < array.length; i++) {
			tempArray[i] = array[i];
		}
		// der Zwischenspeicher erhealt die neue Zeile
		tempArray[tempArray.length - 1] = text;
		// tables wird mit dem Zwischenspeicher ueberschrieben
		array = tempArray;
		return array;
	}

	/**
	 * Diese Methode erhoeht ein 2dstring Array um einen Eintrag
	 * 
	 * @param array
	 *            das Array das vorhanden war
	 * @param text
	 *            das mit dem erhoeht wird
	 * @return das neue Array
	 */
	public String[][] incrementStringStringArray(String[][] array, String[] text) {
		// erstellen eines neuen Arrays das um eins groesser ist als das
		// vorherige
		String[][] tempArray = new String[array.length + 1][];
		// Schleife uebertraegt jede Spanlte des Arrays in den Zwischenspeicher
		for (int i = 0; i < array.length; i++) {
			tempArray[i] = array[i];
		}
		// der Zwischenspeicher erhealt die neue Zeile
		tempArray[tempArray.length-1] = text;
		// tables wird mit dem Zwischenspeicher ueberschrieben
		array = tempArray;
		return array;
	}

	/**
	 * @return the myAddress
	 */
	public String getMyAddress() {
		return myAddress;
	}

	/**
	 * @return the myUser
	 */
	public String getMyUser() {
		return myUser;
	}

	/**
	 * @return the myPassword
	 */
	public String getMyPassword() {
		return myPassword;
	}

	/**
	 * @return the myDatabase
	 */
	public String getMyDatabase() {
		return myDatabase;
	}

	/**
	 * @return the posAddress
	 */
	public String getPosAddress() {
		return posAddress;
	}

	/**
	 * @return the posUser
	 */
	public String getPosUser() {
		return posUser;
	}

	/**
	 * @return the posPassword
	 */
	public String getPosPassword() {
		return posPassword;
	}

	/**
	 * @return the posDatabase
	 */
	public String getPosDatabase() {
		return posDatabase;
	}

	/**
	 * @return the countTable
	 */
	public int getCountTable() {
		return countTable;
	}

	/**
	 * @return the tables
	 */
	public String[][] getTables() {
		return tables;
	}

	/**
	 * @return the vars
	 */
	public String[][] getVars() {
		return vars;
	}
}
