package identified;

import java.util.HashMap;
import java.util.ArrayList;

class Graph {

    private HashMap<Integer, identified.Node> _nodes;

    public Graph(){
        _nodes = new HashMap<Integer, identified.Node>();
    }

    public void connect(int from, int to){
        Node node = _nodes.get(from);
        if(node == null){
            node = new Node();
            _nodes.put(from, node);
        }
        node.addEdge(to);        
    }

    public ArrayList<Integer> connections(int of){
        Node node = _nodes.get(of);
        if(node != null){
            return node.edges();
        }
        else return null;
    }

    public ArrayList<Integer> connections(ArrayList<Integer> of){
        ArrayList<Integer> allConnections = new ArrayList<Integer>();
        for(Integer i: of){
            ArrayList<Integer> connections = this.connections(i.intValue());
            if(connections != null){
                allConnections.addAll(connections);
            }
        }
        return allConnections;
    }

    public HashMap<Integer, ArrayList<Integer>> connectionsWithRoutes(ArrayList<Integer> of){
        HashMap<Integer, ArrayList<Integer>> allConnections = new HashMap<Integer, ArrayList<Integer>>();

        for(Integer source: of){
            ArrayList<Integer> connections = this.connections(source.intValue());
            if(connections != null){
                for(Integer connection: connections){
                    ArrayList<Integer> routes = allConnections.get(connection);
                    if(routes == null) {
                        routes = new ArrayList<Integer>();
                        allConnections.put(connection, routes);
                    }
                    routes.add(source);
                }
            }
        }

        return allConnections;
    }
}
