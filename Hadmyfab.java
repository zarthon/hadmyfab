
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
    
    private static void dropTables()throws Exception{
    	Statement drop = null;
    	drop = CONN.createStatement();
    	try{
    		drop.executeUpdate("DROP TABLE IF EXISTS USERS");
    	}
    	finally{
    		drop.close();
    	}
    	
    }
    
    private static void createTables()throws Exception{
    	Statement create = null;
    	create = CONN.createStatement();
    	
    	String query = "CREATE TABLE USERS (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, username varchar(40), password varchar(40))";
    	try{
    		create.executeUpdate(query);
    	}
    	finally{
    		
    		create.close();
    	}
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
            dropTables();
            createTables();
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
