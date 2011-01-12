package identified;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.graphdb.*;
import java.sql.*;
import java.util.*;

class GraphIndexer {
    
    private GraphDatabaseService _graph;
    private HashMap<Integer, org.neo4j.graphdb.Node> _uids;

    public GraphIndexer(){
        System.out.println("Creating graph");
        _graph = new EmbeddedGraphDatabase("graphdb");                
        _uids = new HashMap<Integer, org.neo4j.graphdb.Node>();
    }

    private org.neo4j.graphdb.Node getOrCreate(int uid){
        org.neo4j.graphdb.Node node = _uids.get(Integer.valueOf(uid));
        if(node == null){
            node = _graph.createNode();
            _uids.put(Integer.valueOf(uid), node);
        }
        return node;
    }
    

    public boolean index() 
        throws java.sql.SQLException, java.lang.ClassNotFoundException
    {

        // Load drive
        Class.forName("org.postgresql.Driver");
        
        Connection conn = DriverManager.getConnection("jdbc:postgresql:spark_development?user=spark&password=spark");
        
        Statement statement = conn.createStatement();        

        double start = Time.now();
        String sql = "select candidate_id, friend_candidate_id from identified_buddies";
        ResultSet rs = statement.executeQuery(sql);
            
        System.out.format("Time to fetch results: %f\n", Time.now() - start);
        start = Time.now();
        Transaction tx = _graph.beginTx();
        try {
            while(rs.next()){
                getOrCreate(rs.getInt(1)).createRelationshipTo(getOrCreate(rs.getInt(2)), Relationships.KNOWS);          
            }
        } 
        finally {
        }

        System.out.format("Time to create graph: %f\n", Time.now() - start);


        start = Time.now();
        sql = "select candidate_id, (facebook_user_id + 100000) as facebook_user_id from facebook_buddies";
        rs = statement.executeQuery(sql);
            
        System.out.format("Time to fetch results: %f\n", Time.now() - start);

        start = Time.now();
        tx = _graph.beginTx();
        try {
            while(rs.next()){
                getOrCreate(rs.getInt(1)).createRelationshipTo(getOrCreate(rs.getInt(2)), Relationships.KNOWS);          
            }
        } 
        finally {
            tx.finish();
        }

        System.out.format("Time to create graph: %f\n", Time.now() - start);


        for(int i = 0; i < 10; i++){
            
        }

   
        return true;
    }

}
