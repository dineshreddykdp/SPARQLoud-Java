package controllers;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.query.ResultSetRewindable;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.resultset.*;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.delay.core.QueryExecutionFactoryDelay;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.pagination.core.QueryExecutionFactoryPaginated;
import play.mvc.Controller;
import play.mvc.Result;
import utils.ResultSetCompareAndDiff;

/**
 * Created by magnus on 17.02.15.
 */
public class SPARQLoud extends Controller {

    public static Result index() {
        return ok();
    }

    public static Result sparql() {

        Long startTime = System.currentTimeMillis();

        QueryExecutionFactory qef = new QueryExecutionFactoryHttp("http://dbpedia.org/sparql", "http://dbpedia.org");
        //qef = new QueryExecutionFactoryDelay(qef, 2000);
        qef = new QueryExecutionFactoryPaginated(qef, 5000);

        QueryExecution qe1 = qef.createQueryExecution("SELECT ?s { ?s a <http://dbpedia.org/ontology/City> } LIMIT 5");
        ResultSet rs1 = qe1.execSelect();

        System.out.println("Query 1: " + (System.currentTimeMillis() - startTime));

//        System.out.println(ResultSetFormatter.asText(rs1));

        QueryExecution qe2 = qef.createQueryExecution("SELECT ?s { ?s a <http://dbpedia.org/ontology/Person> } ORDER BY ?s LIMIT 50000");
        ResultSet rs2 = qe2.execSelect();

        System.out.println("Query 2: " + (System.currentTimeMillis() - startTime));

//        System.out.println(ResultSetFormatter.asText(rs2));

        ResultSetRewindable rs1a = ResultSetFactory.makeRewindable(rs1) ;
        System.out.println("Size Result Set 1: " + rs1a.size());
        System.out.println("Make rewindable: " + (System.currentTimeMillis() - startTime));

        ResultSetRewindable rs2a = ResultSetFactory.makeRewindable(rs2) ;
        System.out.println("Size Result Set 2: " + rs2a.size());
        System.out.println("Make rewindable: " + (System.currentTimeMillis() - startTime));

        Model rs1model = ModelFactory.createDefaultModel();
        // TODO ResultSetFormatter.asRDF does not include index, so ordered results can not be deserialized in ordered form
        ResultSetFormatter.asRDF(rs1model, rs1a);
        //rs1model.write(System.out, "TURTLE");
        System.out.println("Serialize Result Set 1: " + (System.currentTimeMillis() - startTime));

        Model rs2model = ModelFactory.createDefaultModel();
        ResultSetFormatter.asRDF(rs2model, rs2a);
        //rs2model.write(System.out, "TURTLE");
        System.out.println("Serialize Result Set 2: " + (System.currentTimeMillis() - startTime));

        rs1a.reset();
        rs2a.reset();

        ResultSetRewindable rs1b = ResultSetFactory.makeRewindable(new RDFInput(rs1model));
        System.out.println("Deserialize Result Set 1: " + (System.currentTimeMillis() - startTime));

        ResultSetRewindable rs2b = ResultSetFactory.makeRewindable(new RDFInput(rs2model));
        System.out.println("Deserialize Result Set 2: " + (System.currentTimeMillis() - startTime));

        System.out.println(ResultSetCompareAndDiff.equalsByTerm(rs2a, rs2b) + " " + (System.currentTimeMillis() - startTime));
        rs2a.reset();
        rs2b.reset();
        System.out.println(ResultSetCompare.equalsByTermAndOrder(rs2a, rs2b) + " " + (System.currentTimeMillis() - startTime));
//        rs1a.reset();
//        rs2a.reset();
//        System.out.println(ResultSetCompareAndDiff.equalsByValue(rs1a, rs2a) + " " + (System.currentTimeMillis() - startTime));
//        rs1a.reset();
//        rs2a.reset();
//        System.out.println(ResultSetCompareAndDiff.equalsByValueAndOrder(rs1a, rs2a) + " " + (System.currentTimeMillis() - startTime));

        return play.mvc.Results.TODO;
    }
}
