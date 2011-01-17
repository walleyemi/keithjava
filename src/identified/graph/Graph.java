package identified.graph;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.sql.*;
import krati.sos.SerializableObjectStore;
import krati.store.DynamicDataStore;
import krati.core.segment.*;
import java.io.File;
import identified.utils.IntSerializer;
import java.util.*;
import identified.utils.Time;
import identified.graph.*;

public class Graph {

    private SerializableObjectStore<Integer, Node> _nodes;


    public Graph(File persistenceDir)        
        throws java.lang.Exception
    {
        DynamicDataStore store = new DynamicDataStore(persistenceDir,
                                                      0, 
                                                      new ChannelSegmentFactory());
        
        _nodes = 
            new SerializableObjectStore<Integer, Node>(store, new IntSerializer(), new Node.Serializer());        
    }

    public boolean clear() throws java.io.IOException {
        _nodes.clear();
        return true;
    }

    public IntArrayList connections(int of){
        Node node = _nodes.get(Integer.valueOf(of));
        if(node != null){
            return node.edges();
        }
        else return null;
    }

    public IntArrayList connections(IntArrayList of){        
        IntArrayList allConnections = new IntArrayList();
        for(int i: of){
            IntArrayList connections = this.connections(i);
            if(connections != null){
                allConnections.addAll(connections);
            }
        }
        return allConnections;
    }

    public Int2ObjectOpenHashMap<IntArrayList> connectionsWithRoutes(IntArrayList of){
        Int2ObjectOpenHashMap<IntArrayList> allConnections = new Int2ObjectOpenHashMap<IntArrayList>();

        for(int source: of){
            IntArrayList connections = this.connections(source);
            if(connections != null){
                for(int connection: connections){
                    IntArrayList routes = allConnections.get(connection);
                    if(routes == null) {
                        routes = new IntArrayList();
                        allConnections.put(connection, routes);
                    }
                    routes.add(source);
                }
            }
        }

        return allConnections;
    }


    /* Loads a graph from a sql string.  Sql should
       contain exactly two fields, a from uid and to uid.
       Ex: Select id, friend_id from friends
     */
    
    public boolean importSQL(String sql, Statement statement)
        throws java.sql.SQLException, GraphException
    {
        
        double start = Time.now();
        
        ResultSet rs = statement.executeQuery(sql);
        int count = 0;
        Int2ObjectOpenHashMap<Node> nodes = new Int2ObjectOpenHashMap<Node>();
        
        while(rs.next()){
            int from = rs.getInt(1);
            int to = rs.getInt(2);
            Node node = nodes.get(from);
            if(node == null){
                node = new Node();
                nodes.put(from, node);
            }
            node.addEdge(to);        
            count++;
        }                       

        System.out.format("Completed mapping of %d rows from %s in %f\n", count, sql, (Time.now() - start));
        
        System.out.println("Writing cache");
        start = Time.now();
        try {
            ObjectSet<Map.Entry<Integer, Node>> pairs = nodes.entrySet();        
            for(Map.Entry<Integer, Node> pair: pairs){
                _nodes.put(pair.getKey(), pair.getValue());
            }
            _nodes.persist();
            System.out.format("Wrote %d nodes in %f\n", pairs.size(), Time.now() - start);
        }
        catch (Exception e){
            throw new GraphException(String.format("Exception persisting graph nodes (%s)", e.getMessage()), e);
        }
        return true;   
    }
    
    public void sync() throws java.io.IOException {_nodes.sync();}
}
