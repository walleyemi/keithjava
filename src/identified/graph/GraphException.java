package identified.graph;

public class GraphException extends Exception {
    private static final long serialVersionUID = 1L;

    GraphException(String reason, Throwable cause){super(reason, cause);}
}
