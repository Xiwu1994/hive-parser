package com.products;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.fs.FileUtils;
import java.io.File;
import java.io.IOException;

public class EmbeddedNeo4j {

    public static enum RelTypes implements RelationshipType {
        dep, beDep, has, belong
    }

    private static final String DB_PATH = "/Users/liebaomac/IdeaProjects/neo4j/packaging/standalone/target/neo4j-community-3.3.0-SNAPSHOT/data/databases/lineage_analysis.db";
    GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( new File(DB_PATH) );

    static {
        try {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
