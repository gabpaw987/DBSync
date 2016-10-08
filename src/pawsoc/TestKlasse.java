package pawsoc;

import pawsoc.Combiner.Combiner;
import pawsoc.Config.ConfigLoader;
import pawsoc.Config.GetConfig;
import pawsoc.Reader.Reader;
import pawsoc.Writer.Writer;

/**
 * Diese Klasse ruft einmal den gesamten Programmablauf auf
 * 
 * @author Gabriel Pawlowsky & Josef Sochovsky
 * @version 1.0
 */
public class TestKlasse {
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		try{
			// Testen des Einlesens
			GetConfig gc = new GetConfig(args[0]);
			String[] test = gc.getResult();
			for (int i = 0; i < test.length; i++) {
				System.out.println(test[i]);
			}
			// Testen des Interpreters
			ConfigLoader cl = new ConfigLoader(args[0]);
	
			// Starten der Reader fuer MySQL und PostgreSQL
			Reader readerMySQL = new Reader(cl, "mysql");
			Reader readerPostGreSQL = new Reader(cl, "postgresql");
	
			// Aufrufen der Klasse die fuer das Kombinieren der beiden Datenbanken
			// zustaendig ist
			Combiner combiner = new Combiner(readerMySQL.getFullData(),
					readerPostGreSQL.getFullData(), cl);
			// Starten der beiden Klassen die in beide Datenbanken alle Daten
			// schreiben
			Writer myWriter = new Writer(cl, "mysql", combiner.getCombinedList());
			Writer posWriter = new Writer(cl, "postgresql",
					combiner.getCombinedList());
		} catch(Exception e){
			System.err.println("Es ist ein Fehler aufgetreten!");
			e.printStackTrace();
		}
	}
}