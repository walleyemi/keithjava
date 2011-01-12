package identified;

import java.sql.*;
import java.io.File;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.QueryParser;


class TextIndexer {
    public TextIndexer(){

    }

    public boolean index()
        throws java.lang.ClassNotFoundException, java.sql.SQLException, java.io.IOException
    {
        
        // Load drive
        Class.forName("org.postgresql.Driver");
        
        Connection conn = DriverManager.getConnection("jdbc:postgresql:spark_development?user=spark&password=spark");
        
        Statement statement = conn.createStatement();
        
        String sql = "select facebook_users.id, " +
            "array_to_string(coalesce(array_agg(facebook_educations.school), '{}'), ' ') as schools, " +
            "array_to_string(coalesce(array_agg(facebook_educations.major_name), '{}'), ' ') as majors, " +
            "array_to_string(coalesce(array_agg(facebook_jobs.company), '{}'), ' ') as companies, " +
            "array_to_string(coalesce(array_agg(facebook_jobs.position), '{}'), ' ') as job_titles " +
            "from facebook_users left join facebook_educations on facebook_users.id = facebook_educations.facebook_user_id " +
            "left join facebook_jobs on facebook_users.id = facebook_jobs.facebook_user_id group by facebook_users.id";

        double start = Time.now();            
        ResultSet rs = statement.executeQuery(sql);

        System.out.format("Loaded in %f\n", Time.now() - start);

        start = Time.now();
        IndexWriter index = new IndexWriter(new NIOFSDirectory(new File("index")),
                                            new StandardAnalyzer(Version.LUCENE_30),
                                            IndexWriter.MaxFieldLength.UNLIMITED
                                            );
        

        while(rs.next()){
            Document doc = new Document();
            doc.add(new Field("id", rs.getString(1), Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.add(new Field("schools", rs.getString(2), Field.Store.YES, Field.Index.ANALYZED));
            doc.add(new Field("majors", rs.getString(3), Field.Store.YES, Field.Index.ANALYZED));
            doc.add(new Field("companies", rs.getString(4), Field.Store.YES, Field.Index.ANALYZED));
            doc.add(new Field("job_titles", rs.getString(5), Field.Store.YES, Field.Index.ANALYZED));
            index.addDocument(doc);
        }

        index.commit();
        
        System.out.format("Indexed in %f\n", Time.now() - start);

        return true;
    }

    public void search()
        throws java.io.IOException, org.apache.lucene.queryParser.ParseException
    {
        IndexSearcher searcher = new IndexSearcher(new NIOFSDirectory(new File("index")));

        for(int i = 0; i < 10; i++){
        
            String[] searches = {"schools:((Stanford AND University) OR (Berkeley) OR (Carnegie AND mellon)) AND majors:(computer AND science) AND job_titles:(software AND engineer)", "Berkeley", "Carnegie Mellon", "+University of +California, +Los +Angeles", "+Yale +University", "Princeton University"};
            for(String q: searches){
                
                double start = Time.now();
                QueryParser parser = new QueryParser(Version.LUCENE_30,
                                                     "schools",
                                                     new StandardAnalyzer(Version.LUCENE_30));            
                Query query = parser.parse(q);
                TopDocs results = searcher.search(query, 10);
                
                System.out.format("Search for %s returned %d hits in %f\n", q, results.totalHits, Time.now() - start);
            }
        }
    }
}
