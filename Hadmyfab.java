
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;

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
    
    public static Hashtable<String,String> UserRId = new Hashtable<String,String>();
    static{UserRId.put("1","mohit");UserRId.put("2","nikhil");UserRId.put("3","naman");UserRId.put("4","maullik");UserRId.put("5","kedar");UserRId.put("6","viranch");UserRId.put("7","swair");};
    
    public static Hashtable<String,String> WallId = new Hashtable<String,String>();
    static{WallId.put("mohit", "1,2,3");WallId.put("nikhil","4,5,6");WallId.put("naman","7,8,9");WallId.put("maullik","10,11,12");WallId.put("kedar", "13,14,15");WallId.put("viranch","16,17,18");WallId.put("swair","19,20,21");};
    
    public static Hashtable<String,String> Users = new Hashtable<String,String>();
    static{Users.put("mohit","asda");Users.put("nikhil","sadf");Users.put("naman", "qweos");Users.put("maullik", "werqw");Users.put("kedar", "394iurw");Users.put("viranch","r23jl");Users.put("swair","34hjk");};
    
    public static Hashtable<String,String> UserProfile = new Hashtable<String,String>();
    static{UserProfile.put("mohit", "Mohit Kothari 23 Single");UserProfile.put("nikhil","Nikhil Marathe 32 Single");UserProfile.put("naman","Naman Muley 20 Single");UserProfile.put("maullik","Maullik Padia 20 Single");UserProfile.put("kedar", "Kedar Bhatt 25 Single");UserProfile.put("viranch","Viranch Mehta 20 Comitted");UserProfile.put("swair","Swair Shah 22 Single");}; 
    
    public static Hashtable<String,String> Friends = new Hashtable<String,String>();
    static{Friends.put("mohit","2 3 4");Friends.put("nikhil","1 3 4");Friends.put("naman","1 2 4");Friends.put("kedar","1 2 3 ");Friends.put("maullik","6 7");Friends.put("viranch","5 7");Friends.put("swair","5 6");};
    
    public static Hashtable<String,String> WallPosts = new Hashtable<String,String>();
    static{WallPosts.put("mohit","First post,Second pst,sadkjlhsad");WallPosts.put("nikhil", "nikhil post one,post 3,sample post");WallPosts.put("naman","my name is naman,hellp all,hi friends");WallPosts.put("maullik","maullik is my name,maullik loves everyone,love everyone");WallPosts.put("kedar","kedar loves all,i rock,asdlkhr");WallPosts.put("viranch","I am committed,I love all,hell is here");WallPosts.put("swair", "swair is here,helldogs well,asdjlhsdsdf");};

    public static Hashtable<String,String> Comments = new Hashtable<String,String>();
    static{Comments.put("mohit","4,8,10#first comment,second comment, third");Comments.put("nikhil","1,7,11#nikhil cimment,heko asd,asdf sd");Comments.put("naman","2,6,12#hahaga,my dsas,aejasdfj");Comments.put("maullik","16,19#Maullik first comment,my second comment");Comments.put("swair","13,17#swair is foreign return,swair attended DS");};
    
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
    		drop.executeUpdate("DROP TABLE IF EXISTS AGGREGATE");
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
    	String wallPosts = "CREATE TABLE WALLPOSTS (id INT UNSIGNED PRIMARY KEY, user_id INT REFERENCES USERS(id), body VARCHAR(200), timestamp VARCHAR(100) )";
    	String comments = "CREATE TABLE COMMENTS (id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, user_id INT REFERENCES USERS(id), wall_id INT REFERENCES WALLPOSTS(id), body VARCHAR(200), timestamp VARCHAR(100) )";
    	String aggregate = "CREATE TABLE AGGREGATE (id INT UNSIGNED PRIMARY KEY, aggregate VARCHAR(1000))";
    	try{
    		create.executeUpdate(users);
    		create.executeUpdate(userProfile);
    		create.executeUpdate(friends);
    		create.executeUpdate(wallPosts);
    		create.executeUpdate(comments);
    		create.executeUpdate(aggregate);
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
    		String insertWallPost = "INSERT INTO WALLPOSTS(id,user_id,body,timestamp) VALUES(?,?,?,?)";
    		String insertComment = "INSERT INTO COMMENTS(user_id,wall_id,body,timestamp) VALUES(?,?,?,?)";
    		
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
    			
    			insert = CONN.prepareStatement(insertWallPost);
    			Enumeration<String> wallpost = WallPosts.keys();   			
    			while(wallpost.hasMoreElements()){
    				String temp = wallpost.nextElement();
    				insert.setString(2,UserId.get(temp).toString());
    				String data = WallPosts.get(temp);
    				String[] posts = data.split(",");
    				String[] wallid = WallId.get(temp).split(",");
    				for(int i=0;i<posts.length;i++){
    					insert.setString(1,wallid[i]);
    					insert.setString(3, posts[i]);
    					long timestamp = System.currentTimeMillis();
    					insert.setString(4,String.valueOf(timestamp));
    					insert.execute();
    				}
    			}
    			
    			insert = CONN.prepareStatement(insertComment);
    			Enumeration<String> comment = Comments.keys();
    			
    			while(comment.hasMoreElements()){
    				String temp = comment.nextElement();
    				insert.setString(1, UserId.get(temp).toString());
    				String data = Comments.get(temp);
    				
    				String[] fields = data.split("#");
    				
    				String[] wallid = fields[0].split(",");
    				String[] comments = fields[1].split(",");
    				
    				for(int i=0;i<wallid.length;i++){
    					insert.setString(2,wallid[i]);
    					insert.setString(3,comments[i]);
    					long timestamp = System.currentTimeMillis();
    					insert.setString(4,String.valueOf(timestamp));
    					insert.execute();
    				}
    			}
    		}
    		catch(SQLException ex){
    			System.out.println(ex);
    			throw ex;
    		}
    		finally{
    			if(insert != null)
    				insert.close();
    		}
    	
    }
    
    public static void displayData() throws SQLException{
    	Statement display = CONN.createStatement();
    	Statement display_com = CONN.createStatement();
    	Statement obt_frnd = CONN.createStatement();
    	String getFriends = "Select friend_id from FRIENDS where user_id=";
    	String getWall = "Select id, body, timestamp from WALLPOSTS where user_id=";
    	String getComment = "Select body,user_id,timestamp from COMMENTS where wall_id=";
    	String insertAggregate = "INSERT INTO AGGREGATE(id,aggregate) VALUES(?,?)";
    	PreparedStatement insert = CONN.prepareStatement(insertAggregate);
    	Enumeration<String> user = UserId.keys();
    	while(user.hasMoreElements()){
    		String aggregate = "";
    		String username = user.nextElement();
    		String user_id = UserId.get(username).toString();
    		System.out.println("USER: "+username+" WALL!!");
    		obt_frnd.executeQuery(getFriends+user_id);
    		ResultSet frnd_set = obt_frnd.getResultSet();
    		while(frnd_set.next()){
    			String frnd_id = frnd_set.getString("friend_id");
    			display.executeQuery(getWall+frnd_id);
        		ResultSet res = display.getResultSet();
        		while(res.next()){
        			String wall_id = res.getString("id");
        			System.out.println(UserRId.get(frnd_id)+" posted: "+res.getString("body"));
        			aggregate += res.getString("body")+" ";
        			display_com.executeQuery(getComment+wall_id);
        			ResultSet coment_set = display_com.getResultSet();
        			while(coment_set.next()){
        				System.out.println("\t"+UserRId.get(coment_set.getString("user_id"))+" commented: "+coment_set.getString("body"));
        				aggregate += coment_set.getString("body") + " ";
        			}
        			System.out.println("-------------------------------------------------------------------------");
        		}
    		}
    		display.executeQuery(getWall+user_id);
    		ResultSet res = display.getResultSet();
    		while(res.next()){
    			String wall_id = res.getString("id");
    			System.out.println("You posted: "+res.getString("body"));
    			aggregate += res.getString("body");
    			display_com.executeQuery(getComment+wall_id);
    			ResultSet coment_set = display_com.getResultSet();
    			while(coment_set.next()){
    				System.out.println("\t"+UserRId.get(coment_set.getString("user_id"))+" commented: "+coment_set.getString("body"));
    				aggregate += coment_set.getString("body");
    			}
    			System.out.println("-------------------------------------------------------------------------");
    		}
    		insert.setString(1, user_id);
    		insert.setString(2,aggregate);
    		insert.execute();
    		System.out.println("=======================================================================");
    	}
    	
    }
    
    public static void main (String[] args)
    {
        try{
            init();
            dropTables();
            createTables();
            insertData();
            displayData();
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
