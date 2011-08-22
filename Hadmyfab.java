
import java.sql.*;
import java.util.*;
import java.lang.Integer;
/**
 * This is a sample application has basically 2 usage:-
 * 1) Demonstrate the use of jdbc driver for mysql by connecting to server, droping tables and creating tables and also
 * populate them
 * 2) Also run a mapred job using hadoop framework buy generating something with the above data and then output something
 * relevant.
 * 
 */

public class Hadmyfab
{
    public static Connection CONN = null;
    public static final String URL="jdbc:mysql://localhost/junkdb";
    public static final String USERNAME = "haduser";
    public static final String PASSWORD = "pass";
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    
    //Sample Data for insertion
    public static Hashtable<String,Integer> UserId = new Hashtable<String,Integer>();
    static{UserId.put("mohit",1);UserId.put("nikhil",2);UserId.put("naman",3);UserId.put("maullik",4);UserId.put("kedar",5);UserId.put("viranch",6);UserId.put("swair",7);};
    
    public static Hashtable<String,String> Users = new Hashtable<String,String>();
    static{Users.put("mohit","asda");Users.put("nikhil","sadf");Users.put("naman", "qweos");Users.put("maullik", "werqw");Users.put("kedar", "394iurw");Users.put("viranch","r23jl");Users.put("swair","34hjk");};
    
    public static Hashtable<String,String> UserProfile = new Hashtable<String,String>();
    static{UserProfile.put("mohit", "Mohit Kothari 23 Single");UserProfile.put("nikhil","Nikhil Marathe 32 Single");UserProfile.put("naman","Naman Muley 20 Single");UserProfile.put("maullik","Maullik Padia 20 Single");UserProfile.put("kedar", "Kedar Bhatt 25 Single");UserProfile.put("viranch","Viranch Mehta 20 Comitted");UserProfile.put("swair","Swair Shah 22 Single");}; 
    
    public static Hashtable<String,String> Friends = new Hashtable<String,String>();
    static{Friends.put("mohit","2 3 4");Friends.put("nikhil","1 3 4");Friends.put("naman","1 2 4");Friends.put("kedar","1 2 3 ");Friends.put("maullik","6 7");Friends.put("viranch","5 7");Friends.put("swair","5 6");};
    
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
    
    public static void shutdown() throws Exception{
    	try{
    		CONN.close();
    	}
    	catch(Exception x){
    		
    		System.out.println("Error occured in closing the mysql server connection: "+ x.toString());
    	}
    	
    	
    }
    private static void createTables()throws Exception{
    	Statement create = null;
    	create = CONN.createStatement();
    	
    	String users = "CREATE TABLE USERS (id INT UNSIGNED PRIMARY KEY, username varchar(40), password varchar(40))";
    	String userProfile = "CREATE TABLE USERPROFILE (user_id INT REFERENCES USERS(id), first_name varchar(40), last_name varchar(40), age INT, relation varchar(10))";
    	String friends = "CREATE TABLE FRIENDS (user_id INT REFERENCES USERS(id), friend_id INT REFERENCES USERS(id), PRIMARY KEY(user_id,friend_id))";
    	String wallPosts = "CREATE TABLE WALLPOSTS (id INT UNSIGNED PRIMARY KEY, user_id INT REFERENCES USERS(id), body VARCHAR(200), timestamp TIMESTAMP )";
    	String comments = "CREATE TABLE COMMENTS (id INT UNSIGNED PRIMARY KEY, user_id INT REFERENCES USERS(id), wall_id INT REFERENCES WALLPOSTS(id), body VARCHAR(200), timestamp TIMESTAMP )";
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
    
    public static void insertData() throws SQLException{
    		PreparedStatement insert = null;
    		String insertUsers = "INSERT INTO USERS(id,username, password) VALUES(?,?,?)";
    		String insertUserProfile = "INSERT INTO USERPROFILE(user_id, first_name, last_name, age, relation) VALUES(?,?,?,?,?)";
    		String insertFriend = "INSERT INTO FRIENDS(user_id,friend_id) VALUES(?,?)";
    		try{
    			insert = CONN.prepareStatement(insertUsers);
    			Enumeration<String> user = Users.keys();
    			
    			while(user.hasMoreElements()){
    				String temp = user.nextElement();
    				insert.setString(2, temp);
    				insert.setString(3,Users.get(temp));
    				insert.setString(1,UserId.get(temp).toString());
    				insert.execute();
    			}
    			
    			insert = CONN.prepareStatement(insertUserProfile);
    			Enumeration<String> userProf = UserProfile.keys();
    			
    			while(userProf.hasMoreElements()){
    				String temp = userProf.nextElement();
    				insert.setString(1,UserId.get(temp).toString());
    				String data = UserProfile.get(temp);
    				String[] fields = data.split(" ");
    				insert.setString(2, fields[0]);
    				insert.setString(3, fields[1]);
    				insert.setString(4, fields[2]);
    				insert.setString(5, fields[3]);
    				insert.execute();
    			}
    			
    			insert = CONN.prepareStatement(insertFriend);
    			Enumeration<String> friend = Friends.keys();
    			
    			while(friend.hasMoreElements()){
    				String temp = friend.nextElement();
    				insert.setString(1,UserId.get(temp).toString());
    				String data = Friends.get(temp);
    				String[] friends_id = data.split(" ");
    				for(int i=0;i<friends_id.length;i++){
    					insert.setString(2, friends_id[i]);
    					insert.execute();
    				}
    			}
    		}
    		catch(SQLException ex){
    			//Reverse the statements as something went wrong
    			CONN.rollback();
    			throw ex;
    		}
    		finally{
    			if(insert != null)
    				insert.close();
    		}
    	
    }
    public static void main (String[] args)
    {
        try{
            init();
            dropTables();
            createTables();
            insertData();
        }
        catch (Exception e)
        {
            System.err.println (e);
        }
        finally
        {
            try{
            	shutdown();
            }
            catch (Exception e){/*ignore all */}
        }
    }

}
