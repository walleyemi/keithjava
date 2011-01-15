package identified;

import java.sql.*;
import java.util.*;
import it.unimi.dsi.fastutil.ints.*;
import java.io.File;
import identified.utils.Time;
import identified.lucene.*;
import identified.graph.*;

class IndexPostgres {
    public IndexPostgres(){}

    public static void main(String[] args) 
        throws java.lang.ClassNotFoundException, java.sql.SQLException, java.io.IOException, org.apache.lucene.queryParser.ParseException, Exception {

        //TextIndexer indexer = new TextIndexer();
        //indexer.index();        
        //indexer.search();    
        IndexPostgres indexer = new IndexPostgres();
        indexer.index();        

    }

    public boolean index() 
        throws java.lang.ClassNotFoundException, java.sql.SQLException, Exception
    {
        Graph first = new Graph(new File("first.graph"));
        Graph second = new Graph(new File("second.graph"));        

        // Load drive
        Class.forName("org.postgresql.Driver");
        
        Connection conn = DriverManager.getConnection("jdbc:postgresql:spark_development?user=spark&password=spark");

        Statement statement = conn.createStatement();
        first.importSQL("Select candidate_id, friend_candidate_id from identified_buddies", statement);        
     
        second.importSQL("Select candidate_id, facebook_user_id from facebook_buddies", statement);

        ArrayList<Double> times = new ArrayList<Double>();

        for(int target = 70; target < 100; target++){

            double start = Time.now();
            IntArrayList first_conns = first.connections(target);
            if(first_conns == null) continue;
            
            IntArrayList second_conns = second.connections(first_conns);                    

            System.out.format("Calculated %d connections of %d in %f\n", second_conns.size(), target, Time.now() - start);
        
            start = Time.now();
            Int2ObjectOpenHashMap<IntArrayList> connectionsWithRoutes = second.connectionsWithRoutes(first_conns);
            System.out.format("Calculated %d connections with routes of %d in %f\n", connectionsWithRoutes.size(), target, Time.now() - start);

            
            start = Time.now();
            Int2IntOpenHashMap uniq = new Int2IntOpenHashMap();
            for(Integer c: second_conns){
                Integer curr = uniq.get(c);
                if(curr == null) curr = Integer.valueOf(0);
                uniq.put(c, Integer.valueOf(curr.intValue() + 1));
            }
            System.out.format("Calculated %d uniq connections of %d in %f\n", uniq.size(), target, Time.now() - start);
            /*
            start = Time.now();
            IntArrayList uniq2 = new IntArrayList();
            Collections.sort(second_conns);
            int last = -1;
            for(Integer c: second_conns){
                if(c != last){
                    uniq2.add(c);
                }
                last = c;
            }
            System.out.format("(Method 2)Calculated %d uniq connections of %d in %f\n", uniq2.size(), target, Time.now() - start);
            */

        }

        return true;
    }

}
