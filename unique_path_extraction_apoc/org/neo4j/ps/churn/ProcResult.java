package org.neo4j.ps.churn;

import org.neo4j.graphdb.Path;

public class ProcResult {
   public Path path;

   public ProcResult(Path path) {
      this.path = path;
   }
}
