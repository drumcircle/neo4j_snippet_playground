package org.neo4j.ps.churn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

public class ProcMain {
   @Context
   public Transaction tx;
   @Context
   public Log log;
   @Context
   public GraphDatabaseService db;
   @Context
   public TerminationGuard terminationGuard;

   @Procedure(
      name = "org.neo4j.ps.churn.findOpportunityJourney",
      mode = Mode.READ
   )
   @Description("org.neo4j.ps.churn.findOpportunityJourney(Node startNode")
   public Stream findOpportunityJourneys(@Name("n") Node n) {
      return this.getAllPaths(n).stream().map((path) -> {
         return new ProcResult(path.toPath());
      });
   }

   public List getAllPaths(Node n) {
      List completedPaths = new ArrayList();
      List currPaths = new ArrayList();
      OpportunityJourney journey = new OpportunityJourney(n);
      currPaths.add(journey);

      while(true) {
         List nextPaths = new ArrayList();
         Iterator var6 = currPaths.iterator();

         while(var6.hasNext()) {
            OpportunityJourney p = (OpportunityJourney)var6.next();
            List relationships = p.getNextRelationships();
            if (relationships.size() == 0) {
               completedPaths.add(p);
            } else {
               List extendedPaths = p.extendPath(relationships);
               nextPaths.addAll(extendedPaths);
            }
         }

         if (nextPaths.size() == 0) {
            return completedPaths;
         }

         currPaths = nextPaths;
      }
   }
}
