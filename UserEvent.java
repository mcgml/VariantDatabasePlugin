package nhs.genetics.cardiff;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.impl.store.PropertyType;

import java.util.*;

/**
 * Created by ml on 02/02/2016.
 */
public class UserEvent {

    public enum UserEventStatus {
        PENDING_AUTH, ACTIVE, REJECTED
    }

    public static void addUserEvent(GraphDatabaseService graphDb, Node subjectNode, Label eventNodeLabel, Node userNode, HashMap<String, Object> properties) throws IllegalArgumentException, NullPointerException {

        try (Transaction tx = graphDb.beginTx()) {
            long lastEventNodeId = subjectNode.getId();

            TraversalDescription traversalDescription = graphDb.traversalDescription()
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .relationships(VariantDatabase.getHasUserEventRelationship(), Direction.OUTGOING);

            for (Path path : traversalDescription.traverse(subjectNode)) {
                for (Node eventNode : path.nodes()) {

                    //skip start node
                    if (eventNode.getId() == subjectNode.getId()) continue;

                    UserEventStatus status = getUserEventStatus(graphDb, eventNode);

                    if (status == null){
                        throw new NullPointerException("Event has null status");
                    } else if (status == UserEventStatus.PENDING_AUTH){
                        throw new IllegalArgumentException("Event is pending authorisation. Cannot add new event");
                    }

                    lastEventNodeId = eventNode.getId();
                }
            }

            Node newEventNode = graphDb.createNode(eventNodeLabel);

            for (Map.Entry<String, Object> iter : properties.entrySet()){
                newEventNode.setProperty(iter.getKey(), iter.getValue());
            }

            graphDb.getNodeById(lastEventNodeId).createRelationshipTo(newEventNode, VariantDatabase.getHasUserEventRelationship());

            Relationship addedByRelationship = newEventNode.createRelationshipTo(userNode, VariantDatabase.getAddedByRelationship());
            addedByRelationship.setProperty("date", new Date().getTime());

            tx.success();
        }

    }

    public static void authUserEvent(GraphDatabaseService graphDb, Node eventNode, Node userNode){
        try (Transaction tx = graphDb.beginTx()) {
            Relationship authByRelationship = eventNode.createRelationshipTo(userNode, VariantDatabase.getAuthorisedByRelationship());
            authByRelationship.setProperty("date", new Date().getTime());
            tx.success();
        }
    }

    public static void rejectUserEvent(GraphDatabaseService graphDb, Node eventNode, Node userNode){
        try (Transaction tx = graphDb.beginTx()) {
            Relationship rejectedByRelationship = eventNode.createRelationshipTo(userNode, VariantDatabase.getRejectedByRelationship());
            rejectedByRelationship.setProperty("date", new Date().getTime());
            tx.success();
        }
    }

    public static Node getSubjectNode(GraphDatabaseService graphDb, Node eventNode){
        Node subjectNode = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription().relationships(VariantDatabase.getHasUserEventRelationship(), Direction.INCOMING).traverse(eventNode)) {
                for (Node previousNode : path.nodes()) {
                    subjectNode = previousNode;
                }
            }
        }

        return subjectNode;
    }

    public static UserEventStatus getUserEventStatus(GraphDatabaseService graphDb, Node eventNode) {
        Relationship authorisedByRelationship, rejectedByRelationship;

        try (Transaction tx = graphDb.beginTx()) {
            authorisedByRelationship = eventNode.getSingleRelationship(VariantDatabase.getAuthorisedByRelationship(), Direction.OUTGOING);
            rejectedByRelationship = eventNode.getSingleRelationship(VariantDatabase.getRejectedByRelationship(), Direction.OUTGOING);
        }

        if (authorisedByRelationship == null && rejectedByRelationship == null){
            return UserEventStatus.PENDING_AUTH;
        }

        if (authorisedByRelationship != null && rejectedByRelationship == null){
            return UserEventStatus.ACTIVE;
        }

        if (authorisedByRelationship == null && rejectedByRelationship != null){
            return UserEventStatus.REJECTED;
        }

        return null;
    }

}
