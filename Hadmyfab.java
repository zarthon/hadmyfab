
import java.sql.*;

public class Hadmyfab
{
    public static Connection CONN = null;
    public static final String URL="jdbc:mysql://localhost/junkdb";
    public static final String USERNAME = "haduser";
    public static final String PASSWORD = "pass";
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    
    private static void init()throws Exception{	
    	Class.forName (DRIVER_CLASS).newInstance ();
        CONN = DriverManager.getConnection (URL, USERNAME, PASSWORD);
        System.out.println ("Database connection established");
    }
    
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
            init();
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
