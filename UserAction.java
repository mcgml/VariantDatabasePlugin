package nhs.genetics.cardiff;

import org.neo4j.graphdb.*;
import java.util.Date;

/**
 * Created by ml on 29/12/2015.
 */
public class UserAction {

    //schema
    /*
    * (subjectNode)-->(actionNode)-->(addedByUser|removedByUser|addedAuthUser|removedAuthUser)
    *
    */

    public enum UserActionStatus {
        PENDING_APPROVAL, ACTIVE, PENDING_RETIRE, RETIRE, UNKNOWN
    }

    public static void addUserAction(GraphDatabaseService graphDb, Node subjectNode, RelationshipType actionRelationshipType, Node actionNode, Node userNode, String evidence){

        try (Transaction tx = graphDb.beginTx()) {

            //todo check action doesn't already exist

            //link action to user
            Relationship addedByRelationship = actionNode.createRelationshipTo(userNode, VariantDatabase.getAddedByRelationship());
            addedByRelationship.setProperty("date", new Date().getTime());
            if (evidence != null) addedByRelationship.setProperty("evidence", evidence);

            //link to subject node
            subjectNode.createRelationshipTo(actionNode, actionRelationshipType);

            tx.success();
        }

    }
    public static void removeUserAction(GraphDatabaseService graphDb, Node actionNode, Node userNode, String evidence){

        try (Transaction tx = graphDb.beginTx()) {

            //todo check action doesn't already exist

            //link action to user
            Relationship removedByRelationship = actionNode.createRelationshipTo(userNode, VariantDatabase.getRemovedByRelationship());
            removedByRelationship.setProperty("date", new Date().getTime());
            if (evidence != null) removedByRelationship.setProperty("evidence", evidence);

            tx.success();
        }

    }
    public static void authUserAction(GraphDatabaseService graphDb, Node actionNode, RelationshipType authRelationshipType, Node userNode){
        //todo check action doesn't already exist

        try (Transaction tx = graphDb.beginTx()) {
            Relationship relationship = actionNode.createRelationshipTo(userNode, authRelationshipType);
            relationship.setProperty("date", new Date().getTime());
            tx.success();
        }

    }
    public static UserActionStatus getUserActionStatus(GraphDatabaseService graphDb, Node actionNode) {

        Relationship addedByRelationship, addAuthorisedByRelationship, removedByRelationship, removeAuthorisedByRelationship;

        try (Transaction tx = graphDb.beginTx()) {
            addedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);
            addAuthorisedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getAddAuthorisedByRelationship(), Direction.OUTGOING);
            removedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getRemovedByRelationship(), Direction.OUTGOING);
            removeAuthorisedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);
        }

        if (addedByRelationship != null && addAuthorisedByRelationship == null && removedByRelationship == null && removeAuthorisedByRelationship == null){
            return UserActionStatus.PENDING_APPROVAL;
        } else if (addedByRelationship != null && addAuthorisedByRelationship != null && removedByRelationship == null && removeAuthorisedByRelationship == null){
            return UserActionStatus.ACTIVE;
        } else if (addedByRelationship != null && addAuthorisedByRelationship != null && removedByRelationship != null && removeAuthorisedByRelationship == null){
            return UserActionStatus.PENDING_RETIRE;
        } else if (addedByRelationship != null && addAuthorisedByRelationship != null && removedByRelationship != null && removeAuthorisedByRelationship != null){
            return UserActionStatus.RETIRE;
        }

        return UserActionStatus.UNKNOWN;
    }

}
