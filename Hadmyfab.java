
import java.sql.*;

public class Hadmyfab
{
    public static void main (String[] args)
    {
        Connection conn = null;
        try{
            String userName = "haduser";
            String password = "pass";
            String url = "jdbc:mysql://localhost/junkdb";
            Class.forName ("com.mysql.jdbc.Driver").newInstance ();
            conn = DriverManager.getConnection (url, userName, password);
            System.out.println ("Database connection established");
        }
        catch (Exception e)
        {
            System.err.println (e);
        }
        finally
        {
            if (conn != null)
            {
                try
                {
                    conn.close ();
                    System.out.println ("Database connection terminated");
                }
                catch (Exception e) { /* ignore close errors */ }
            }
        }
    }

}
