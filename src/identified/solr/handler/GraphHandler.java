package identified.solr;

import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParserPlugin;

import org.apache.lucene.search.Query;
import org.apache.lucene.queryParser.ParseException;

import java.sql.*;
import java.io.File;
import java.util.logging.Logger;
import java.io.IOException;
import identified.graph.Graph;

public class GraphHandler extends RequestHandlerBase implements SolrCoreAware {
    
    private static Logger log = Logger.getLogger("identified.graph.graphhandler");
    private Graph _graph;
    private Connection _dbConn;

    public GraphHandler(){
        _graph = null;
        _dbConn = null;       
    }


    /* Initialize state dependent on solr core */
    public void inform(SolrCore core) {

        try{
            Class.forName("org.postgresql.Driver");
            _dbConn = DriverManager.getConnection("jdbc:postgresql:spark_development?user=spark&password=spark");
        } 
        catch(java.lang.ClassNotFoundException e){
            log.severe("Unable to load postgres driver");            
        }
        catch(java.sql.SQLException e){
            log.severe(String.format("Unable to connect to database: %s\n", e.getMessage()));
        }

        try{
            _graph = new Graph(new File(core.getDataDir(), "graph"));
        }
        catch(Exception e){
            log.severe(String.format("Unable to initialize graph: %s\n", e.getMessage()));
        }
    }

    private boolean clean(){
        return false;
    }

    private boolean fullImport(SolrQueryRequest req, SolrQueryResponse rsp){       
        log.info("Starting import");
        try {
            _graph.clear();
        } 
        catch(java.io.IOException e){
            log.severe(String.format("Error importing graph - Unable to clear graph directory due to an IO Exception: %s\n", e.getMessage()));
            return false;
        }

        try{
            Statement statement = _dbConn.createStatement();
            _graph.importSQL("select candidate_id, friend_candidate_id from identified_buddies", statement);
            rsp.add("status", "successful import");
        }
        catch(java.sql.SQLException e){
            log.severe(String.format("Error importing graph - Database error: %s\n", e.getMessage()));
            return false;
        }
        catch(identified.graph.GraphException e){
            log.severe(String.format("Error importing graph - Graph error: %s\n", e.getMessage()));
            return false;
        }
        log.info("Finished import");

        return true;        
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
    
    /* Handle text query + network filter */
    private boolean query(SolrQueryRequest req, SolrQueryResponse rsp){
        
        SolrParams params = req.getParams();
        String queryStr = params.get("query");
        String network = params.get("network");
        
        if(queryStr == null) {
            return false;
        }
        
        try {
            rsp.add("query", queryStr);

            QParser parser = QParser.getParser(queryStr, QParserPlugin.DEFAULT_QTYPE, req);
            Query query = parser.getQuery();                 
            
            SolrIndexSearcher.QueryCommand command = new SolrIndexSearcher.QueryCommand();
            command.setQuery(query);
                
            SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
            SolrIndexSearcher searcher = req.getSearcher();
            searcher.search(result, command);

            rsp.add("matches", result.getDocList());
            for(int docid: result.getDocList()){
                log.info(String.format("Doc id: %d", docid));
            }
        }
        catch(ParseException e){
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
        }
        catch(IOException e){
            log.severe(String.format("IO Error while handling query: %s", e.getMessage()));
            return false;
        }
        

        return true;
    }

    public String getVersion(){return "0.1";}
    public String getSource(){return "";}
    public String getSourceId(){return "";}
    public String getDescription(){return "Imports a social graph and allows queries against that graph";}

    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp){                

        rsp.setHttpCaching(false);

        // What kind of command are we dealing with
        SolrParams params = req.getParams();
        String cmd = params.get("cmd");

        if(cmd != null){
            rsp.add("cmd", cmd);
        }

        if(cmd == null){
            return;
        }

        if(cmd.equals("import")){
            fullImport(req, rsp);            
        }
        else if(cmd.equals("delta")){
            deltaImport();
        }
        else if(cmd.equals("update")){
            update();
        }
        else if(cmd.equals("delete")){
            delete();
        }        
        else if(cmd.equals("query")){
            query(req, rsp);
        }
        
    }
}
