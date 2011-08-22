
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
    		drop.executeUpdate("DROP TABLE IF EXISTS USERPROFILE");
    		drop.executeUpdate("DROP TABLE IF EXISTS FRIENDS");
    		drop.executeUpdate("DROP TABLE IF EXISTS WALLPOSTS");
    		drop.executeUpdate("DROP TABLE IF EXISTS COMMENTS");
    		System.out.println("Tables Dropped");
    	}
    	finally{
    		
    		drop.close();
    	}
    	
    }
    
    private static void createTables()throws Exception{
    	Statement create = null;
    	create = CONN.createStatement();
    	
    	String users = "CREATE TABLE USERS (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, username varchar(40), password varchar(40))";
    	String userProfile = "CREATE TABLE USERPROFILE (user_id INT REFERENCES USERS(id), first_name varchar(40), last_name varchar(40), age INT, relation varchar(10))";
    	String friends = "CREATE TABLE FRIENDS (user_id INT REFERENCES USERS(id), friend_id INT REFERENCES USERS(id), PRIMARY KEY(user_id,friend_id))";
    	String wallPosts = "CREATE TABLE WALLPOSTS (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, user_id INT REFERENCES USERS(id), body VARCHAR(200), timestamp TIMESTAMP )";
    	String comments = "CREATE TABLE COMMENTS (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, user_id INT REFERENCES USERS(id), wall_id INT REFERENCES WALLPOSTS(id), body VARCHAR(200), timestamp TIMESTAMP )";
    	try{
    		create.executeUpdate(users);
    		create.executeUpdate(userProfile);
    		create.executeUpdate(friends);
    		create.executeUpdate(wallPosts);
    		create.executeUpdate(comments);
    		System.out.println("Tables created");
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
