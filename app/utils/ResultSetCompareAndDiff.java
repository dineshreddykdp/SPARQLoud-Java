package utils;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingUtils;
import com.hp.hpl.jena.sparql.resultset.ResultSetRewindable;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;
import com.hp.hpl.jena.sparql.util.NodeUtils;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.Transform;

import java.util.*;

/**
 * Created by magnus on 18.02.15.
 * <p/>
 * This is a reimplementation of ARQ's ResultSetCompare class
 */
public class ResultSetCompareAndDiff {
    /* This is from the DAWG test suite.
     * Result set 1:
     *   ---------------
     *   | x    | y    |
     *   ===============
     *   | _:b0 | _:b1 |
     *   | _:b2 | _:b3 |
     *   | _:b1 | _:b0 |
     *   ---------------
     * Result set 2:
     *   ---------------
     *   | x    | y    |
     *   ===============
     *   | _:b1 | _:b0 |
     *   | _:b3 | _:b2 |
     *   | _:b2 | _:b3 |
     *   ---------------
     */

//    private static String[] rs1$ = {
//        "(resultset (?x ?y)",
//        "   (row (?x _:b0) (?y _:b1))",
//        "   (row (?x _:b2) (?y _:b3))",
//        "   (row (?x _:b1) (?y _:b0))",
//        ")"} ;
//    private static String[] rs2$ = {
//        "(resultset (?x ?y)",
//        "   (row (?x _:c1) (?y _:c0))",
//        "   (row (?x _:c3) (?y _:c2))",
//        "   (row (?x _:c2) (?y _:c3))",
//        ")"} ;
//
    // Nasty result set to test.
    // These are the same but the first row of rs2$ throws in a wrong mapping of b0/c1

    // Right mapping is:
    // b0->c3, b1->c2, b2->c1, b3->c0
    // Currently we get, working simply top to bottom, no backtracking:
    // b0->c1, b1->c0, b2->c3, b3->c2, then last row fails as _:b1 is mapped to c0, b0 to c1 not (c2, c3)

    // ----

    // Limitations:
    // This code does not do compare/isomorphism combined with value testing.
    // It drops to graph isomorphism, which is term based.

    /**
     * Compare two result sets for equivalence.  Equivalance means:
     * A row rs1 has one matching row in rs2, and vice versa.
     * A row is only matched once.
     * Rows match if they have the same variables with the same values.
     * bNodes must map to a consistent other bNodes.  Value comparisons of nodes.
     * <p/>
     * Destructive - rs1 and rs2 are both read, possibly to exhaustion.
     *
     * @param rs1a
     * @param rs2a
     * @return true if they are equivalent
     */
    public static boolean equalsByValue(ResultSetRewindable rs1a, ResultSetRewindable rs2a) {
        if (!compareHeader(rs1a, rs2a)) return false;

        //return equivalent(convert(rs1), convert(rs2), new BNodeIso(NodeUtils.sameValue)) ;

        // Add the isomprohism test
        // Imperfect - need by-value and isomorphism - but this covers test suite needs.

        //ResultSetRewindable rs1a = ResultSetFactory.makeRewindable(rs1) ;
        //ResultSetRewindable rs2a = ResultSetFactory.makeRewindable(rs2) ;

        if (equivalent(convert(rs1a), convert(rs2a), new BNodeIso(NodeUtils.sameValue)))
            return true;
        rs1a.reset();
        rs2a.reset();
        return isomorphic(rs1a, rs2a);
    }

    public static boolean equalsByTest(Collection<Binding> b1, Collection<Binding> b2, NodeUtils.EqualityTest match) {
        List<Binding> rows1 = new ArrayList<Binding>(b1);
        List<Binding> rows2 = new ArrayList<Binding>(b2);
        return equivalent(rows1, rows2, match);
    }

    /**
     * compare two result sets for equivalence.  Equivalance means:
     * A row rs1 has one matching row in rs2, and vice versa.
     * A row is only matched once.
     * Rows match if they have the same variables with the same values,
     * bNodes must map to a consistent other bNodes.
     * Term comparisons of nodes.
     * <p/>
     * Destructive - rs1 and rs2 are both read, possibly to exhaustion.
     *
     * @param rs1a
     * @param rs2a
     * @return true if they are equivalent
     */
    public static boolean equalsByTerm(ResultSetRewindable rs1a, ResultSetRewindable rs2a) {
        if (!compareHeader(rs1a, rs2a)) return false;

        //return equivalent(convert(rs1), convert(rs2), new BNodeIso(NodeUtils.sameTerm)) ;
        //ResultSetRewindable rs1a = ResultSetFactory.makeRewindable(rs1) ;
        //ResultSetRewindable rs2a = ResultSetFactory.makeRewindable(rs2) ;

        if (equivalent(convert(rs1a), convert(rs2a), new BNodeIso(NodeUtils.sameTerm)))
            return true;
        rs1a.reset();
        rs2a.reset();
        return isomorphic(rs1a, rs2a);
    }

    /**
     * compare two result sets for equivalence.  Equivalance means:
     * Each row in rs1 matchs the same index row in rs2.
     * Rows match if they have the same variables with the same values,
     * bNodes must map to a consistent other bNodes.
     * Value comparisons of nodes.
     * <p/>
     * Destructive - rs1 and rs2 are both read, possibly to exhaustion.
     *
     * @param rs1
     * @param rs2
     * @return true if they are equivalent
     */
    public static boolean equalsByValueAndOrder(ResultSetRewindable rs1, ResultSetRewindable rs2) {
        if (!compareHeader(rs1, rs2)) return false;

        return equivalentByOrder(convert(rs1), convert(rs2), new BNodeIso(NodeUtils.sameValue));
    }

    /**
     * compare two result sets for equivalence.  Equivalance means:
     * Each row in rs1 matchs the same index row in rs2.
     * Rows match if they have the same variables with the same values,
     * bNodes must map to a consistent other bNodes.
     * RDF term comparisons of nodes.
     * <p/>
     * Destructive - rs1 and rs2 are both read, possibly to exhaustion.
     *
     * @param rs1
     * @param rs2
     * @return true if they are equivalent
     */
    public static boolean equalsByTermAndOrder(ResultSetRewindable rs1, ResultSetRewindable rs2) {
        if (!compareHeader(rs1, rs2)) return false;

        return equivalentByOrder(convert(rs1), convert(rs2), new BNodeIso(NodeUtils.sameTerm));
    }

    /**
     * Compare two result sets for bNode isomorphism equivalence.
     * Only does RDF term comparison.
     */
    public static boolean isomorphic(ResultSet rs1, ResultSet rs2) {
        Model m1 = ResultSetFormatter.toModel(rs1);
        Model m2 = ResultSetFormatter.toModel(rs2);
        return m1.isIsomorphicWith(m2);
    }

    /**
     * Compare two bindings, use the node equality test provided
     */
    static public boolean equal(Binding bind1, Binding bind2, NodeUtils.EqualityTest test) {
        if (bind1 == bind2) return true;

        if (bind1.size() != bind2.size())
            return false;
        // They are the same size so containment is enough.
        if (!containedIn(bind1, bind2, test)) return false;
        return true;
    }

    static private boolean compareHeader(ResultSet rs1, ResultSet rs2) {
        if (rs1 == null && rs2 == null) return true;
        if (rs1 == null) return false;
        if (rs2 == null) return false;
        Set<String> names1 = new HashSet<String>(rs1.getResultVars());
        Set<String> names2 = new HashSet<String>(rs2.getResultVars());
        return names1.equals(names2);
    }

    static private List<Binding> convert(ResultSet rs) {
        return Iter.iter(rs).map(qs2b).toList();
    }


//    static boolean equivalentByTerm(ResultSet rs1, ResultSet rs2)
//    {
//        return equivalent(convert(rs1), convert(rs2), sameTerm) ;
//    }

    static private boolean equivalent(Collection<Binding> rows1, Collection<Binding> rows2, NodeUtils.EqualityTest match) {
        if (rows1.size() != rows2.size())
            return false;
        for (Binding row1 : rows1) {
            // find in rows2.
            Binding matched = null;
            for (Binding row2 : rows2) {
                // NEED BACKTRACKING

                if (equal(row1, row2, match)) {
                    matched = row2;
                    break;
                }
            }

            if (matched == null) return false;
            // Remove matching.
            rows2.remove(matched);
        }
        return true;
    }

    static private boolean equivalentByOrder(List<Binding> rows1, List<Binding> rows2, NodeUtils.EqualityTest match) {
        if (rows1.size() != rows2.size())
            return false;

        Iterator<Binding> iter1 = rows1.iterator();
        Iterator<Binding> iter2 = rows2.iterator();

        while (iter1.hasNext()) {
            // Does not need backtracking because rows must
            // align and so must variables in a row.
            Binding row1 = iter1.next();
            Binding row2 = iter2.next();
            if (!equal(row1, row2, match))
                return false;
        }
        return true;
    }

    // Is bind1 contained in bind22?  For every (var,value) in bind1, is it in bind2?
    // Maybe more in bind2.
    private static boolean containedIn(Binding bind1, Binding bind2, NodeUtils.EqualityTest test) {
        // There are about 100 ways to do this!
        Iterator<Var> iter1 = bind1.vars();

        for (Var v : Iter.iter(iter1)) {
            Node n1 = bind1.get(v);
            Node n2 = bind2.get(v);
            if (n2 == null)
                // v bound in bind1 and not in bind2.
                return false;
            if (!test.equal(n1, n2))
                return false;
        }
        return true;
    }

    private static Transform<QuerySolution, Binding> qs2b = new Transform<QuerySolution, Binding>() {

        @Override
        public Binding convert(QuerySolution item) {
            return BindingUtils.asBinding(item);
        }
    };

    public static class BNodeIso implements NodeUtils.EqualityTest {
        private NodeIsomorphismMap mapping;
        private NodeUtils.EqualityTest literalTest;

        public BNodeIso(NodeUtils.EqualityTest literalTest) {
            this.mapping = new NodeIsomorphismMap();
            this.literalTest = literalTest;
        }

        @Override
        public boolean equal(Node n1, Node n2) {
            if (n1 == null && n2 == null) return true;
            if (n1 == null) return false;
            if (n2 == null) return false;

            if (n1.isURI() && n2.isURI())
                return n1.equals(n2);

            if (n1.isLiteral() && n2.isLiteral())
                return literalTest.equal(n1, n2);

            if (n1.isBlank() && n2.isBlank())
                return mapping.makeIsomorphic(n1, n2);

            return false;
        }
    }

}