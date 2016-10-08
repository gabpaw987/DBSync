package pawsoc.Config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

/**
 * Diese Klasse ist dazu da das File in dem alle config Anweisungen vom Benutzer
 * stehen <br>
 * auszulesen um den Inhalt der Datei an einen Interpreter weiterzugeben <br>
 * 
 * @author Josef Sochovsky
 * @version 1.0
 */
public class GetConfig {
	// hier wird die Datei eingelesen
	private File settingsFile;
	// in diesem Array wird dann jede Zeile des Files abgespeichert
	private String[] lines;

	/**
	 * Der Konstruktor definiert das File Objekt mit dem Standartnamen der <br>
	 * configDatei ausserdem sieht er nach ob das Fle ueberhaupt existiert und <br>
	 * danach startet er das auslesen und speichern der Datei
	 */
	public GetConfig(String file) {
		if(file==null||file.isEmpty())
			file = "config";
		// Das File muss immer neben der .jar Datei gespeichert werden also
		// ist es hier zulaessing wenn man es "hardcoded"
		settingsFile = new File(file);
		// Das Array ist zu beginn 0 gross und muss immer um eins erhoeht werden
		// wenn eine neue Zeile gelesen wird
		lines = new String[0];
		// man uerberprueft ob die Datei ueberhaupt existiert
		if (!settingsFile.isFile())
			System.err
					.println("es gibt kein config File aus dem gelesen werden kann");
		startRead();
	}

	/**
	 * Diese Methode ist dafuer da die Datei Zeile fuer Zeile auszulesen und
	 * ihren Inhalt in ein String Array zu speichern
	 */
	private void startRead() {
		// Das File wird mit dem LineNumberreader ausgelesen
		LineNumberReader r = null;
		try {
			// definieren den LineNumberReaders
			r = new LineNumberReader(new FileReader(settingsFile));
			// diese Schleife wird intern dann abgebrochen wenn keine Zeile mehr
			// eingelesen werden kann
			while (true) {
				// in diese Variable wird jede naechste Zeile gespeichert
				String zwischenSpeicher = r.readLine();
				// wen die gelesene Zeile null ist soll die Schleife beendet
				// werden
				if (zwischenSpeicher == null)
					break;
				else {
					incrementArray(zwischenSpeicher);
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("die Datei konnte nicht gefunden werden");
		} catch (IOException e) {
			System.err
					.println("Beim Lesen der Datei ist ein Fehler aufgetreten!");
		} finally {
			// natuerlich muss man am Ende der Methode auch noch die Streams
			// schliessen
			try {
				r.close();
			} catch (IOException e) {
				System.err
						.println("Der Stream zum auslesen der Datei konnte nicht geschlossen werden!");
			}
		}
	}

	/**
	 * Diese Methode erhaelt einen neuen Satz/Text/wort und haengt ihn am Array <br>
	 * hinten dran, es wird also das Array um einen Index erhoeht um einen neuen <br>
	 * Eintrag hizuzufuegen <br>
	 * 
	 * @param newText
	 *            in dem Parameter wird der Text der gepeichert werden soll <br>
	 *            uebergeben
	 */
	public void incrementArray(String newText) {
		// erstellen eines neuen Arrays das um eins groesser ist als das
		// vorherige
		String[] tempArray = new String[lines.length + 1];
		// Schleife uebertraegt jede Spanlte des Arrays in den Zwischenspeicher
		for (int i = 0; i < lines.length; i++) {
			tempArray[i] = lines[i];
		}
		// der Zwischenspeicher erhealt die neue Zeile
		tempArray[tempArray.length-1] = newText;
		// lines wird mit dem Zwischenspeicher ueberschrieben
		lines = tempArray;
	}

	/**
	 * Diese Methode gibt den gesamten Inhalt der Datei zurueck
	 * 
	 * @return alle gelesenen Lines
	 */
	public String[] getResult() {
		return lines;
	}
}
