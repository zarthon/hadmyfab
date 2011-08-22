
import java.sql.*;

public class Hadmyfab
{
    public static Connection CONN = null;
    private static void createSampleTable() throws SQLException{
        Statement sample = null;
		sample = CONN.createStatement();
		
		int count;
        sample.executeUpdate("DROP TABLE IF EXISTS sample");
        sample.executeUpdate("CREATE TABLE sample ( id INT UNSIGNED NOT NULL AUTO_INCREMENT,PRIMARY KEY (id),name CHAR(40))");
        count = sample.executeUpdate ("INSERT INTO sample (name)"
                                    + " VALUES"
                                    + "('haha')");
        sample.close();
        System.out.println(count);
    }

    public static void main (String[] args)
    {
        try{
            String userName = "haduser";
            String password = "pass";
            String url = "jdbc:mysql://localhost/junkdb";
            Class.forName ("com.mysql.jdbc.Driver").newInstance ();
            CONN = DriverManager.getConnection (url, userName, password);
            System.out.println ("Database connection established");
            createSampleTable();
        }
        catch (Exception e)
        {
            System.err.println (e);
        }
        finally
        {
            if (CONN != null)
            {
                try
                {
                    CONN.close ();
                    System.out.println ("Database connection terminated");
                }
                catch (Exception e) { /* ignore close errors */ }
            }
        }
    }

}
