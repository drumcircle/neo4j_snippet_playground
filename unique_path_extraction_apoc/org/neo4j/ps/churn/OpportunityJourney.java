package org.neo4j.ps.churn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class OpportunityJourney {
   private static final String NEXT_RELATIONSHIP = "NEXT";
   private static final String HAS_CHURN_RELATIONSHIP = "HAS_CHURN";
   private final Node startNode;
   private final List journey;

   public OpportunityJourney(Node startNode) {
      this(startNode, new ArrayList());
   }

   public OpportunityJourney(Node startNode, List path) {
      this.startNode = startNode;
      this.journey = new ArrayList(path);
   }

   public void add(Relationship rel) {
      this.journey.add(rel);
   }

   public OpportunityJourney copyPath() {
      return new OpportunityJourney(this.startNode, this.journey);
   }

   public List getJourney() {
      return this.journey;
   }

   public Node getLastNode() {
      if (this.journey.size() == 0) {
         return this.startNode;
      } else {
         Relationship lastRel = (Relationship)this.journey.get(this.journey.size() - 1);
         return lastRel.getEndNode();
      }
   }

   public Path toPath() {
      PathImpl.Builder pathBuilder = new PathImpl.Builder(this.startNode);
      List relationships = this.getJourney();

      Relationship relationship;
      for(Iterator var3 = relationships.iterator(); var3.hasNext(); pathBuilder = pathBuilder.push(relationship)) {
         relationship = (Relationship)var3.next();
      }

      return pathBuilder.build();
   }

   public List extendPath(List relationships) {
      List output = new ArrayList();
      Iterator var3 = relationships.iterator();

      while(var3.hasNext()) {
         Relationship relationship = (Relationship)var3.next();
         OpportunityJourney newPath = this.copyPath();
         newPath.add(relationship);
         output.add(newPath);
      }

      return output;
   }

   public List getNextRelationships() {
      List relationships = new ArrayList();
      Iterable relationshipsIter = this.getLastNode().getRelationships(Direction.OUTGOING, new RelationshipType[]{RelationshipType.withName("NEXT"), RelationshipType.withName("HAS_CHURN")});
      Iterator var3 = relationshipsIter.iterator();

      while(true) {
         Relationship relationship;
         do {
            if (!var3.hasNext()) {
               return relationships;
            }

            relationship = (Relationship)var3.next();
         } while(relationship.getType().toString().equals("HAS_CHURN") && relationship.getEndNode().getProperty("reason").toString().toLowerCase().contains("duplicate"));

         relationships.add(relationship);
      }
   }
}
