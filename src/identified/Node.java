package identified;

import it.unimi.dsi.fastutil.ints.*;
import krati.sos.*;
import java.nio.*;

class Node {

    private IntArrayList _edges;

    public Node(){
        _edges = new IntArrayList();
    }

    public Node(int[] edges){
        _edges = new IntArrayList(edges);
    }

    public void addEdge(int to){
        _edges.add(to);
    }

    public IntArrayList edges() {
        return _edges;
    }

    static public class Serializer implements krati.sos.ObjectSerializer<Node> {
        
        public byte[] serialize(Node n){
            if(n == null) return null;

            IntArrayList edges = n.edges();
            if(edges == null) return null;

            ByteBuffer bytes = ByteBuffer.allocate(edges.size() * 4);
            IntBuffer ints = bytes.asIntBuffer();
            ints.put(edges.elements(), 0, edges.size());            
            return bytes.array();            
        }
        
        public Node construct(byte[] in)
            throws ObjectConstructionException
        {
            if(in == null) return null;

            ByteBuffer bytes = ByteBuffer.wrap(in);
            if(bytes.array().length % 4 != 0) {
                throw new ObjectConstructionException("Byte array cannot be converted to ints because length is not divisible by 4");
            }
            
            IntBuffer ints = bytes.asIntBuffer();
            Node node = null;
            if(ints.remaining() > 0){
                int[] rawInts = new int[ints.remaining()];
                ints.get(rawInts);
                node = new Node(rawInts);
            }
            else node = new Node();
            return node;
        }
        
    }
}
