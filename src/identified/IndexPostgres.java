package identified;

import java.sql.*;
import java.util.*;

class IndexPostgres {
    public IndexPostgres(){}

    public static void main(String[] args) 
        throws java.lang.ClassNotFoundException, java.sql.SQLException, java.io.IOException, org.apache.lucene.queryParser.ParseException {

        //TextIndexer indexer = new TextIndexer();
        //indexer.index();        
        //indexer.search();    
        //IndexPostgres indexer = new IndexPostgres();
        //indexer.index();        
        GraphIndexer indexer = new GraphIndexer();
        indexer.index();
    }

    public boolean index() 
        throws java.lang.ClassNotFoundException, java.sql.SQLException 
    {
        Graph first = new Graph();
        Graph second = new Graph();        

        // Load drive
        Class.forName("org.postgresql.Driver");
        
        Connection conn = DriverManager.getConnection("jdbc:postgresql:spark_development?user=spark&password=spark");

        Statement statement = conn.createStatement();
        this.indexSql("Select candidate_id, friend_candidate_id from identified_buddies", first, statement);        
     
        this.indexSql("Select candidate_id, facebook_user_id from facebook_buddies", second, statement);

        ArrayList<Double> times = new ArrayList<Double>();

        for(int target = 70; target < 10000; target++){

            double start = now();
            ArrayList<Integer> first_conns = first.connections(target);
            if(first_conns == null) continue;

            ArrayList<Integer> second_conns = second.connections(first_conns);                    

            System.out.format("Calculated %d connections of %d in %f\n", second_conns.size(), target, now() - start);
        
            start = now();
            HashMap<Integer, ArrayList<Integer>> connectionsWithRoutes = second.connectionsWithRoutes(first_conns);
            System.out.format("Calculated %d connections with routes of %d in %f\n", connectionsWithRoutes.size(), target, now() - start);

            
            start = now();
            HashMap<Integer, Integer> uniq = new HashMap<Integer, Integer>();
            for(Integer c: second_conns){
                Integer curr = uniq.get(c);
                if(curr == null) curr = Integer.valueOf(0);
                uniq.put(c, Integer.valueOf(curr.intValue() + 1));
            }
            System.out.format("Calculated %d uniq connections of %d in %f\n", uniq.size(), target, now() - start);
            /*
            start = now();
            ArrayList<Integer> uniq2 = new ArrayList<Integer>();
            Collections.sort(second_conns);
            int last = -1;
            for(Integer c: second_conns){
                if(c != last){
                    uniq2.add(c);
                }
                last = c;
            }
            System.out.format("(Method 2)Calculated %d uniq connections of %d in %f\n", uniq2.size(), target, now() - start);
            */

        }

        return true;
    }

    private double now(){
        double time = System.currentTimeMillis();
        return time/1000.0;
    }

    private boolean indexSql(String sql, identified.Graph graph, Statement statement)
        throws java.sql.SQLException
    {

        long start = System.currentTimeMillis();

        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        while(rs.next()){
            count++;
            graph.connect(rs.getInt(1), rs.getInt(2));
        }

        System.out.format("Completed load of %d rows from %s in %d\n", count, sql, (System.currentTimeMillis() - start));
        
        return true;
    }
}
