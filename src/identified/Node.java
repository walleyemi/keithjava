package identified;

import java.util.ArrayList;

class Node {

    private ArrayList<Integer> _edges;

    public Node(){
        _edges = new ArrayList<Integer>();
    }

    public void addEdge(int to){
        _edges.add(Integer.valueOf(to));
    }

    public ArrayList<Integer> edges() {
        return _edges;
    }
}
