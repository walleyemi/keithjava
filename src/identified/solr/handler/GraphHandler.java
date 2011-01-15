package identified.solr;

import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.NamedList;
import org.apache.solr.util.plugin.SolrCoreAware;

public class GraphHandler extends RequestHandlerBase implements SolrCoreAware {
    
    private Graph _graph;
    private Connections _dbConn;

    public GraphHandler(){
        _graph = null;
        _dbConn = null;
    }

    public void init(NamedList args){
        _graph = new Graph(new File("graph"));
    }

    void inform(SolrCore core) {

    }

    private boolean clean(){
        return false;
    }

    private boolean fullImport(){
        _graph.clear();
        _graph.importSQL();
        
    }

    private boolean deltaImport(){
        return false;
    }

    private boolean update() {
        return false;
    }

    private boolean delete(){
        return false;
    }

    private boolean query(){
        return false;
    }

    public String getVersion(){return "0.1";}
    public String getSource(){return "";}
    public String getSourceId(){return "";}
    public String getDescription(){return "Imports a social graph and allows queries against that graph";}

    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp){
        
        // What kind of command are we dealing with
        SolrParams params = req.getParams();
        String cmd = params.getString("cmd");
        if(!cmd){
            return;
        }

        if(cmd == "import"){
            fullImport();
        }
        else if(cmd == "delta"){
            deltaImport();
        }
        else if(cmd == "update"){
            update();
        }
        else if(cmd == "delete"){
            delete();
        }        
        
    }
}
