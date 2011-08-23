import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;




@SuppressWarnings("deprecation")
public class MapredJob{
	/**Holds a tuple for a user as <userid, aggregate>**/
    static class AggregateRecord implements Writable, DBWritable{
    	String user_id;
    	String aggregate;
    	
    	public void readFields(DataInput in) throws IOException {
    		this.user_id = Text.readString(in);
    		this.aggregate = Text.readString(in);
    	}
    	
    	public void write(DataOutput out) throws IOException {
    		Text.writeString(out, user_id);
    		Text.writeString(out, aggregate);
    	}
    	
        public void readFields(ResultSet resultSet) throws SQLException {
        	this.user_id = resultSet.getString("id");
        	this.aggregate = resultSet.getString("aggregate"); 
        }
        
        public void write(PreparedStatement statement) throws SQLException{
        	statement.setString(1, user_id);
            statement.setString(2, aggregate);
        }
    }
    
    /**Holds a final output tuple as <userid,word,count> for each user**/
    static class AggregateWordCount implements Writable, DBWritable{
    	String user_id;
    	String word;
    	int count;
    	public AggregateWordCount(String user_id, String word, int count) {
    	      this.user_id = user_id;
    	      this.word = word;
    	      this.count = count;
    	 }
    	    
    	public void readFields(DataInput in) throws IOException {
    	      this.user_id = Text.readString(in);
    	      this.word = Text.readString(in);
    	      this.count = in.readInt();
    	}
    	
    	public void write(DataOutput out) throws IOException {
    	      Text.writeString(out, user_id);
    	      Text.writeString(out, word);
    	      out.writeInt(count);
    	}
    	public void readFields(ResultSet resultSet) throws SQLException {
    	      this.user_id = resultSet.getString("id");
    	      this.word = resultSet.getString("word");
    	      this.count = resultSet.getInt("count");    	    
    	}
    	
    	public void write(PreparedStatement statement) throws SQLException {
    	      statement.setString(1, user_id);
    	      statement.setString(2, word);
    	      statement.setInt(3, count);
    	}
    	public String toString() {
    	      return user_id+ " " + word+" "+count;
    	}
    }
    
    //Trying to use the old api as i found lot of examples using the same, so for confirmation that the code is working
	
	public static class AggregateMapper extends MapReduceBase implements Mapper<LongWritable, AggregateRecord, Text, LongWritable> {
    	LongWritable ONE = new LongWritable(1);
        private Text word = new Text();
        
        public void map(LongWritable key, AggregateRecord value, OutputCollector< Text, LongWritable> output, Reporter reporter)
            throws IOException {
        	StringTokenizer itr = new StringTokenizer(value.aggregate);
        	while(itr.hasMoreElements()){
        		word.set(itr.nextToken().toLowerCase());
        		output.collect(new Text(value.user_id+"#"+word),ONE);
        	}
        }
    }
    /**This should count the number of words for each user **/
    
	public static class AggregateReducer extends MapReduceBase implements Reducer<Text, LongWritable, AggregateWordCount, NullWritable>{
    	
    	NullWritable n = NullWritable.get();
    	
    	public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<AggregateWordCount, NullWritable> output, Reporter reporter) throws IOException{
    		int sum = 0;
    	    while(values.hasNext()) {
    	    	sum += values.next().get();
    	    }
    	    String[] temp = key.toString().split("#");
    	    output.collect(new AggregateWordCount(temp[0],temp[1],sum),n);
    	}
    }
    
	public static void main(String[] args) throws Exception{
    	String[] fields = {"id","word","count"};
    	String[] agg_fields = {"id","aggregate"};
    	JobConf jobconf = new JobConf(new Configuration(),Hadmyfab.class);
    	jobconf.setJobName("Hadmyfab Mapred Job");
    	jobconf.setMapperClass(AggregateMapper.class);
    	jobconf.setReducerClass(AggregateReducer.class);
    	DBConfiguration.configureDB(jobconf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost/junkdb","haduser","pass");
    	DBInputFormat.setInput(jobconf,AggregateRecord.class,"AGGREGATE",null,"id",agg_fields);
    	DBOutputFormat.setOutput(jobconf, "RESULT",fields);
    	
    	jobconf.setMapOutputKeyClass(Text.class);
    	jobconf.setMapOutputValueClass(LongWritable.class);
    	jobconf.setOutputKeyClass(AggregateWordCount.class);
        jobconf.setOutputValueClass(NullWritable.class);
        
        try{
        	JobClient.runJob(jobconf);
        	
        }
        catch(Exception e){
        	System.err.println (e);
        }
    	
    }
}
