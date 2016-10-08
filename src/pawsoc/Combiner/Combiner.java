package pawsoc.Combiner;

import java.util.ArrayList;
import pawsoc.Config.ConfigLoader;

/**
 * Diese Klasse synchronisiert die logischen Datenbanken die von den Readern <br>
 * gelesen werden, dabei ist darauf zu achten welche Art der Synchronisation <br>
 * angewandt wird, also MM oder SM/MS
 * 
 * @author Gabriel Pawlowsky & Josef Sochovsky
 * @version 1.0
 */
public class Combiner {
	// hier wird die kombinierte Tabelle gespeichert also alle Zeilen die
	// spaeter in beide Datenbanken gespeichert werden sollen
	private ArrayList<String[][]> combinedList;

	public Combiner(ArrayList<String[][]> mySQLList,
			ArrayList<String[][]> postGreSQLList, ConfigLoader cl) {
		// deklarieren der kombinierten Liste
		this.combinedList = new ArrayList<String[][]>();
		// Diese Schleife ueberprueft die Master/Slaves
		for (int i = 0; i < cl.getCountTable(); i++) {
			// wenn einer der beiden Tabellennamen in der config an der dritten
			// Stelle steht wird MasterSlave angewendet
			if (cl.getTables()[i][2].equalsIgnoreCase(cl.getTables()[i][0]))
				combineMasterSlave(mySQLList.get(i), postGreSQLList.get(i), 0);
			else if (cl.getTables()[i][2]
					.equalsIgnoreCase(cl.getTables()[i][1]))
				combineMasterSlave(mySQLList.get(i), postGreSQLList.get(i), 1);
			// wenn eine Klammer am Anfag des ersten Eintrages erkannt wird muss
			// man MM aufrufen, herausfinden wo die PrimaryKeys sind und wo der
			// Timestamp
			else if (cl.getTables()[i][2].startsWith("(")) {
				combineMasters(
						mySQLList.get(i),
						postGreSQLList.get(i),
						searchForTimeStamp(
								cl.getTables()[i][0],
								cl.getTables()[i][2].split(",")[0].substring(1),
								cl.getVars()),
						searchForPrimary(cl.getTables()[i][0], cl.getVars()));
			} else
				System.err.println("Config-file fehlerhaft!");
		}
	}

	/**
	 * Diese Methode kombiniert die Eintraege wenn MS/SM gefragt ist, es wird <br>
	 * also im Prinzip der Inhalt einer Tabelle komplett uebernommen
	 * 
	 * @param myTable
	 *            die Tabelleneintraege der MySQL Tabelle
	 * @param posTable
	 *            die Tabelleneintraege der PostgreSQL Tabelle
	 * @param master
	 *            entweder 0 oder 1 0 fuer MySQL ist Master oder 1 fuer <br>
	 *            PostgreSQL
	 */
	private void combineMasterSlave(String[][] myTable, String[][] posTable,
			int master) {
		if (master == 0) {
			combinedList.add(myTable);
		} else {
			combinedList.add(posTable);
		}
	}

	/**
	 * Diese Methode wird eingesetzt wenn als Synchronisationsart MM mit <br>
	 * timestamp ausgewaehlt wurde
	 * 
	 * @param myTable
	 *            die Tabelleneintraege der MySQL Tabelle
	 * @param posTable
	 *            die Tabelleneintraege der PostgreSQL Tabelle
	 * @param timeStampIndex
	 *            Der Index an dem sich der timestamp befindet
	 * @param primaries
	 *            Der/Die Indizes an denen sich die PrimaryKeys befinden
	 */
	private void combineMasters(String[][] myTable, String[][] posTable,
			int timeStampIndex, int[] primaries) {
		// deklarieren eines leeren String[][] in das nach und nach die
		// richtigen Zeilen gespeichert werden
		String[][] newTable = new String[0][];
		// Diese Schleife ist dazu da herauszufinden welcher timestamp hoeher
		// oder niedriger ist, aber im Prinzip sucht sie gleiche Primary Keys
		// und vergleicht dann die Timestamps
		for (int i = 0; i < myTable.length; i++) {
			for (int j = 0; j < posTable.length; j++) {
				// die Primarykeys werden hier in Strings zusammenkopiert damit
				// sie nicht einzeln auf gleichheit ueberprueft werden sondern
				// als gesamtheit
				String myprime = "";
				String posprime = "";
				for (int k = 0; k < primaries.length; k++) {
					myprime += myTable[i][primaries[k]];
					posprime += posTable[j][primaries[k]];
				}
				// diese if ueberprueft nun ob sich die Primarykeys gleichen
				if (myprime.equals(posprime)) {
					// nun wird unterschieden zwischen gleichen Timestamps
					// aelteren oder juengeren, jenachdem was herauskommt wird
					// entschieden welche gespeichert wird
					if (Integer.parseInt(myTable[i][timeStampIndex]) == Integer
							.parseInt(posTable[j][timeStampIndex]))
						newTable = incrementStringStringArray(newTable,
								myTable[i]);
					else if (Integer.parseInt(myTable[i][timeStampIndex]) < Integer
							.parseInt(posTable[j][timeStampIndex]))
						newTable = incrementStringStringArray(newTable,
								posTable[j]);
					else if (Integer.parseInt(myTable[i][timeStampIndex]) > Integer
							.parseInt(posTable[j][timeStampIndex]))
						newTable = incrementStringStringArray(newTable,
								myTable[i]);
				}
			}
		}
		// diese Schleife geht nocheinmal alle MySQL Eintraege durch, wenn die
		// Primarykey eintraege von denen noch nicht vorhanden sind wird dann
		// eine neue Zeile in das Array eingefuegt
		for (int i = 0; i < myTable.length; i++) {
			if (!searchForStringArray(myTable[i], newTable, primaries)) {
				newTable = incrementStringStringArray(newTable, myTable[i]);
				System.out.println("ich schreibe:" + myTable[i][0]);
			}
		}
		// diese Schleife geht nocheinmal alle PostgreSQL Eintraege durch, wenn
		// die
		// Primarykey eintraege von denen noch nicht vorhanden sind wird dann
		// eine neue Zeile in das Array eingefuegt
		for (int i = 0; i < posTable.length; i++) {
			if (!searchForStringArray(posTable[i], newTable, primaries)) {
				newTable = incrementStringStringArray(newTable, posTable[i]);
				System.out.println("ich schreibe:" + posTable[i][0]);
			}
		}
		// am Ende der Methode wird dann noch das String[][] Array in die
		// ArrayList combinedList hinzugefuegt
		combinedList.add(newTable);
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
	private String[][] incrementStringStringArray(String[][] array,
			String[] text) {
		// erstellen eines neuen Arrays das um eins groesser ist als das
		// vorherige
		String[][] tempArray = new String[array.length + 1][];
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
	 * Diese Methode sucht in einem 2d Array nach einem eintrag und wenn dieser <br>
	 * gefunden wird, wird true zurueckgegeben
	 * 
	 * @param object
	 *            das Array nach dem gesucht wird
	 * @param searchPool
	 *            das 2d Array in dem nach object gesucht wird
	 * @param primaries
	 * @return true wenn es gefunden wurde
	 */
	private boolean searchForStringArray(String[] object,
			String[][] searchPool, int[] primaries) {
		boolean found = false;
		for (int i = 0; i < searchPool.length && !found; i++) {
			found = compareStringArray(object, searchPool[i], primaries);
		}
		return found;
	}

	/**
	 * Diese Methode ist dazu da 2 String Arrays zu vergleichen und <br>
	 * wenn sich diese beiden gleichen wird true zurueckgegeben <br>
	 * wenn sie nicht gleich sind wird false zurueckgegeben
	 * 
	 * @param s1
	 *            das erste String Array
	 * @param s2
	 *            das zweite String Array
	 * @param primaries
	 * @return ob sich die beiden gleichen
	 */
	private boolean compareStringArray(String[] s1, String[] s2, int[] primaries) {
		boolean equal = true;
		if (s1.length != s2.length)
			equal = false;
		String temp1 = "";
		String temp2 = "";
		for (int i = 0; i < primaries.length && equal; i++) {
			temp1 += s1[primaries[i]];
			temp2 += s2[primaries[i]];
		}
		if (!temp1.equals(temp2))
			equal = false;
		return equal;
	}

	/**
	 * sucht nach den Index der Timestamps in der momentanen Tabelle
	 * 
	 * @param myTableName
	 *            name der momentanen Tabelle
	 * @param mytime
	 *            name des Attributs auf der mysql seite
	 * @param tableInfo
	 * @return -1 wenn er nicht gefunden wurde Index wenn er gefunden wurde
	 */
	private int searchForTimeStamp(String myTableName, String mytime,
			String[][] tableInfo) {
		// man benoetigt ein Minus weil uns immer nur die Eintraege aus der
		// aktuellen Tabelle interessieren und nicht die aus allen
		int minus = 0;
		for (int j = 0; j < tableInfo.length; j++) {
			if (tableInfo[j][0].equals(myTableName)
					&& tableInfo[j][1].equals(mytime))
				return j - minus;
			if (!tableInfo[j][0].equals(myTableName))
				//wenn es nicht zutrifft wird minus erhoeht
				minus++;
		}
		return -1;
	}

	/**
	 * Sucht in der TableInfoTabelle nach eintraegen mit + am ende und gibt die <br>
	 * Indizes dieser zurueck
	 * 
	 * @param myTableName
	 *            damit man nicht alle indizes bekommt
	 * @param tableInfo
	 *            hier wird gesucht
	 * @return indizes der primary keys
	 */
	private int[] searchForPrimary(String myTableName, String[][] tableInfo) {
		int[] indexes = new int[0];
		// man benoetigt ein Minus weil uns immer nur die Eintraege aus der
		// aktuellen Tabelle interessieren und nicht die aus allen
		int minus = 0;
		for (int i = 0; i < tableInfo.length; i++) {
			if (tableInfo[i][0].equals(myTableName)
					&& tableInfo[i][4].contains("+")) {
				indexes = incrementIntArray(indexes, i - minus);
			} else
				minus++;
		}
		return indexes;
	}

	/**
	 * Erhoeht ein int[] um einen neuen Eintrag <br>
	 * 
	 * @param array
	 *            altes Array
	 * @param neu
	 *            wert um den man das Array erhoeht
	 * @return das neue groessere Array
	 */
	private int[] incrementIntArray(int[] array, int neu) {
		// erstellen eines neuen Arrays das um eins groesser ist als das
		// vorherige
		int[] tempArray = new int[array.length + 1];
		// Schleife uebertraegt jede Spanlte des Arrays in den Zwischenspeicher
		for (int i = 0; i < array.length; i++) {
			tempArray[i] = array[i];
		}
		// der Zwischenspeicher erhealt die neue Zeile
		tempArray[tempArray.length - 1] = neu;
		// tables wird mit dem Zwischenspeicher ueberschrieben
		return tempArray;
	}

	/**
	 * returns CombinedList
	 * 
	 * @return combinedList
	 */
	public ArrayList<String[][]> getCombinedList() {
		return combinedList;
	}
}
