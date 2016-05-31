package nhs.genetics.cardiff;

import java.io.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import htsjdk.tribble.readers.LineIteratorImpl;
import htsjdk.tribble.readers.LineReaderUtil;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;

import javax.security.auth.login.CredentialException;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/**
 * A class for querying a Neo4j DB with variant data
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2015-06-23
 */
@Path("/variantdatabase")
public class VariantDatabasePlugin
{
    private enum UserEventStatus {
        PENDING_AUTH, ACTIVE, REJECTED
    }

    public enum ClinVarCode {
        UncertainSignificance(0),
        NotProvided(1),
        Benign(2),
        LikelyBenign(3),
        LikelyPathogenic(4),
        Pathogenic(5),
        DrugResponse(6),
        Histocompatibility(7),
        Other(255);

        private int code;

        ClinVarCode(int code) {
            this.code = code;
        }

        public int getCode() { return code; }

        public static ClinVarCode get(int code) {
            switch(code) {
                case  0: return UncertainSignificance;
                case  1: return NotProvided;
                case  2: return Benign;
                case  3: return LikelyBenign;
                case  4: return LikelyPathogenic;
                case  5: return Pathogenic;
                case  6: return DrugResponse;
                case  7: return Histocompatibility;
                case  255: return Other;
            }
            return null;
        }

    }

    private Log logger;
    private GraphDatabaseService graphDb;
    private final ObjectMapper objectMapper;
    private Method[] methods;

    public VariantDatabasePlugin(@Context GraphDatabaseService graphDb, @Context Log logger)
    {
        this.logger = logger;
        this.graphDb = graphDb;
        this.objectMapper = new ObjectMapper();
        this.methods = this.getClass().getMethods();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Workflow {
        String name();
        String description();
    }

    @POST
    @Path("/diagnostic/return")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response diagnosticReturn(final String json) {
        logger.debug(json);
        return Response.status( Response.Status.OK ).entity(
                (json).getBytes( Charset.forName("UTF-8") ) ).build();
    }

    @GET
    @Path("/diagnostic/nodes/multiplerelationships")
    @Produces(MediaType.APPLICATION_JSON)
    public Response diagnosticNodesMultipleRelationships() {
        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    HashSet<Long> ids = new HashSet<>();
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    try (Transaction tx = graphDb.beginTx()) {
                        for (Node node : graphDb.getAllNodes()){

                            for (Relationship relationship : node.getRelationships(Direction.OUTGOING)){
                                if (ids.contains(relationship.getEndNode().getId())) {
                                    logger.debug("node " + node.getId() + " " + node.getLabels().toString() + " is connected to node " + relationship.getEndNode().getId() + " " + relationship.getEndNode().getLabels().toString() + " more than once");
                                } else {
                                    ids.add(relationship.getEndNode().getId());
                                }
                            }

                            ids.clear();
                        }
                    }

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @GET
    @Path("/diagnostic/warmup")
    @Produces(MediaType.APPLICATION_JSON)
    public Response diagnosticWarmup() {

        try {

            try (Transaction tx = graphDb.beginTx()) {
                Node start;

                for ( Node n : graphDb.getAllNodes() ) {
                    n.getPropertyKeys();
                    for ( Relationship relationship : n.getRelationships() ) {
                        start = relationship.getStartNode();
                    }
                }

                for ( Relationship r : graphDb.getAllRelationships() ) {
                    r.getPropertyKeys();
                    start = r.getStartNode();
                }
            }

            logger.info("Warmed up!");
            return Response.ok().build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/workflows/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response workflowsInfo() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    //print method names with get or post annotations
                    for (Method method : methods){
                        if (method.isAnnotationPresent(Workflow.class) && !method.isAnnotationPresent(Deprecated.class)){
                            jg.writeStartObject();

                            jg.writeStringField("name", method.getAnnotation(Workflow.class).name());
                            jg.writeStringField("description", method.getAnnotation(Workflow.class).description());

                            jg.writeEndObject();
                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/analyses/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response analysesInfo() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> sampleNodes = graphDb.findNodes(VariantDatabase.getSampleLabel())) {

                            while (sampleNodes.hasNext()) {
                                Node sampleNode = sampleNodes.next();

                                for (Relationship relationship : sampleNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasAnalysisRelationship())) {
                                    Node runInfoNode = relationship.getEndNode();

                                    //check run has passed QC
                                    Node lastEventNode = getLastUserEventNode(runInfoNode);

                                    if (lastEventNode.getId() != runInfoNode.getId()){
                                        UserEventStatus status = getUserEventStatus(lastEventNode);

                                        if (status == UserEventStatus.ACTIVE && (boolean) lastEventNode.getProperty("passOrFail")){
                                            jg.writeStartObject();

                                            writeSampleInformation(sampleNode, jg);
                                            writeRunInformation(runInfoNode, jg);

                                            jg.writeEndObject();
                                        }

                                    }

                                }
                            }

                            sampleNodes.close();
                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/panels/list")
    @Produces(MediaType.APPLICATION_JSON)
    public Response panelsInfoAll() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> virtualPanelNodes = graphDb.findNodes(VariantDatabase.getVirtualPanelLabel())) {

                            while (virtualPanelNodes.hasNext()) {
                                Node virtualPanelNode = virtualPanelNodes.next();

                                jg.writeStartObject();
                                writeVirtualPanelInformation(virtualPanelNode, jg);
                                jg.writeEndObject();

                            }

                            virtualPanelNodes.close();
                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/panels/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response panelsInfoSingle(final String json) {

        try {
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node panelNode = graphDb.getNodeById(parameters.panelNodeId);

                        writeVirtualPanelInformation(panelNode, jg);

                        jg.writeArrayFieldStart("symbols");

                        for (Relationship containsSymbol : panelNode.getRelationships(Direction.OUTGOING, VariantDatabase.getContainsSymbolRelationship())){
                            Node symbolNode = containsSymbol.getEndNode();

                            jg.writeStartObject();
                            writeSymbolInformation(symbolNode, jg);
                            jg.writeEndObject();

                        }

                        jg.writeEndArray();
                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/panels/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response panelsAdd(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.getNodeById(parameters.userNodeId);
                if (!userNode.hasLabel(VariantDatabase.getUserLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getUserLabel().name() + " got " + userNode.getLabels().toString());

                //make panel node
                Node virtualPanelNode = graphDb.createNode(VariantDatabase.getVirtualPanelLabel());
                virtualPanelNode.setProperty("virtualPanelName", parameters.virtualPanelName);

                //link to user
                Relationship designedByRelationship = virtualPanelNode.createRelationshipTo(userNode, VariantDatabase.getDesignedByRelationship());
                designedByRelationship.setProperty("date", new Date().getTime());

                //link to genes
                for (String gene : parameters.virtualPanelList) {
                    Node geneNode = Neo4j.matchOrCreateUniqueNode(graphDb, VariantDatabase.getSymbolLabel(), "symbolId", gene); //match or create gene
                    virtualPanelNode.createRelationshipTo(geneNode, VariantDatabase.getContainsSymbolRelationship()); //link to virtual panel
                }

                tx.success();
            }

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/variant/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response variantInfo(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    Node variantNode = null;

                    jg.writeStartObject();

                    //find variant
                    try (Transaction tx = graphDb.beginTx()) {

                        //find variant
                        try (ResourceIterator<Node> variants = graphDb.findNodes(VariantDatabase.getVariantLabel(), "variantId", parameters.variantId)) {
                            while (variants.hasNext()) {
                                variantNode = variants.next();
                            }
                            variants.close();
                        }

                    }

                    //print variant info
                    if (variantNode != null){
                        writeVariantInformation(variantNode, jg);
                        writeEventHistory(variantNode,jg);
                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    //?todo use global count funtion
    @POST
    @Path("/variant/counts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response variantCounts(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node variantNode = graphDb.getNodeById(parameters.variantNodeId);

                        for (Relationship inheritanceRel : variantNode.getRelationships(Direction.INCOMING)) {
                            Node runInfoNode = inheritanceRel.getStartNode();

                            //check if run has passed QC
                            Node qcNode = getLastActiveUserEventNode(runInfoNode);
                            if (qcNode == null || !(boolean) qcNode.getProperty("passOrFail")){
                                continue;
                            }

                            if (runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())){
                                Node sampleNode = runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode();

                                if (sampleNode.hasLabel(VariantDatabase.getSampleLabel())){
                                    jg.writeStartObject();

                                    writeSampleInformation(sampleNode, jg);
                                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                                    writeRunInformation(runInfoNode, jg);

                                    jg.writeEndObject();
                                }

                            }

                        }

                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/variant/addpathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response variantAddPathogenicity(final String json) {

        try {

            Node variantNode, userNode;
            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            //check classification is in range
            if (parameters.classification < 1 || parameters.classification > 5){
                throw new IllegalArgumentException("Unknown classification");
            }

            //get nodes
            try (Transaction tx = graphDb.beginTx()) {
                variantNode = graphDb.getNodeById(parameters.variantNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
            }

            //check variant does not already have outstanding auths
            Node lastEventNode = getLastUserEventNode(variantNode);

            if (lastEventNode.getId() != variantNode.getId()){
                UserEventStatus status = getUserEventStatus(lastEventNode);

                if (status == UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add pathogenicity. Auth pending.");
                }
            }

            //add properties
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("classification", parameters.classification);

            if (parameters.evidence != null) {
                if (!parameters.evidence.equals("")) properties.put("evidence", parameters.evidence);
            }

            //add event
            addUserEvent(lastEventNode, VariantDatabase.getVariantPathogenicityLabel(), properties, userNode);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/variant/filter")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response variantFilter(final String json) {

        try{
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException {

                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    HashSet<Long> excludeRunInfoNodes = new HashSet<>(Arrays.asList(parameters.excludeRunInfoNodes));
                    HashSet<Long> includePanelNodes = new HashSet<>(Arrays.asList(parameters.includePanelNodes));
                    Node runInfoNode;

                    try (Transaction tx = graphDb.beginTx()) {
                        runInfoNode = graphDb.getNodeById(parameters.runInfoNodeId);
                    }

                    //exec workflow
                    switch (parameters.workflowName) {
                        case "Rare Variant Workflow v1":
                            runRareVariantWorkflowv1(jg, excludeRunInfoNodes, includePanelNodes, runInfoNode);
                            break;
                        case "Autosomal Dominant Workflow v1":
                            runAutosomalDominantWorkflowv1(jg, excludeRunInfoNodes, includePanelNodes, runInfoNode);
                            break;
                        case "Rare Homozygous Workflow v1":
                            runRareHomozygousWorkflowv1(jg, excludeRunInfoNodes, includePanelNodes, runInfoNode);
                            break;
                        case "Autosomal Recessive Workflow v1":
                            runAutosomalRecessiveWorkflowv1(jg, excludeRunInfoNodes, includePanelNodes, runInfoNode);
                            break;
                        case "X Linked Workflow v1":
                            runXLinkedWorkflowv1(jg, excludeRunInfoNodes, includePanelNodes, runInfoNode);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown workflow");
                    }

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @GET
    @Path("/variant/pendingauth")
    @Produces(MediaType.APPLICATION_JSON)
    public Response variantPendingAuth() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> variantPathogenicityIterator = graphDb.findNodes(VariantDatabase.getVariantPathogenicityLabel())){

                            while (variantPathogenicityIterator.hasNext()) {
                                Node variantPathogenicity = variantPathogenicityIterator.next();

                                if (getUserEventStatus(variantPathogenicity) == UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = variantPathogenicity.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);

                                    jg.writeStartObject();

                                    jg.writeNumberField("eventNodeId", variantPathogenicity.getId());
                                    jg.writeStringField("event", "Variant classification");
                                    jg.writeNumberField("value", (int) variantPathogenicity.getProperty("classification"));
                                    if (variantPathogenicity.hasProperty("evidence")) jg.writeStringField("evidence", variantPathogenicity.getProperty("evidence").toString());

                                    jg.writeObjectFieldStart("add");
                                    writeLiteUserInformation(addedByRelationship.getEndNode(), jg);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    writeVariantInformation(getSubjectNodeFromEventNode(variantPathogenicity), jg);
                                    jg.writeEndObject();

                                }

                            }

                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/feature/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response featureInfo(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    Node featureNode = null;

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {

                        try (ResourceIterator<Node> features = graphDb.findNodes(VariantDatabase.getFeatureLabel(), "featureId", parameters.featureId)) {
                            while (features.hasNext()) {
                                featureNode = features.next();
                            }
                            features.close();
                        }

                        if (featureNode!=null){
                            writeFeatureInformation(featureNode, jg);
                            writeEventHistory(featureNode,jg);
                        }

                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/feature/addpreference")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response featureAddPreference(final String json) {

        try {

            Node featureNode, userNode;
            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            //get nodes
            try (Transaction tx = graphDb.beginTx()) {
                featureNode = graphDb.getNodeById(parameters.featureNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
            }

            //check variant does not already have outstanding auths
            Node lastEventNode = getLastUserEventNode(featureNode);

            if (lastEventNode.getId() != featureNode.getId()){
                UserEventStatus status = getUserEventStatus(lastEventNode);

                if (status == UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add preference. Auth pending.");
                }
            }

            //add properties
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("preference", parameters.featurePreference);

            if (parameters.evidence != null) {
                if (!parameters.evidence.equals("")) properties.put("evidence", parameters.evidence);
            }

            //add event
            addUserEvent(lastEventNode, VariantDatabase.getFeaturePreferenceLabel(), properties, userNode);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/feature/pendingauth")
    @Produces(MediaType.APPLICATION_JSON)
    public Response featurePendingAuth() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(VariantDatabase.getFeaturePreferenceLabel())){

                            while (iter.hasNext()) {
                                Node featurePreferenceNode = iter.next();

                                if (getUserEventStatus(featurePreferenceNode) == UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = featurePreferenceNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);

                                    jg.writeStartObject();

                                    jg.writeNumberField("eventNodeId", featurePreferenceNode.getId());
                                    jg.writeStringField("event", "Feature preference");
                                    jg.writeBooleanField("value", (boolean) featurePreferenceNode.getProperty("preference"));
                                    if (featurePreferenceNode.hasProperty("evidence")) jg.writeStringField("evidence", featurePreferenceNode.getProperty("evidence").toString());

                                    jg.writeObjectFieldStart("add");
                                    writeLiteUserInformation(addedByRelationship.getEndNode(), jg);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    writeFeatureInformation(getSubjectNodeFromEventNode(featurePreferenceNode), jg);
                                    jg.writeEndObject();

                                }

                            }

                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/symbol/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response symbolInfo(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    Node symbolNode = null;

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {

                        try (ResourceIterator<Node> symbols = graphDb.findNodes(VariantDatabase.getSymbolLabel(), "symbolId", parameters.symbolId)) {
                            while (symbols.hasNext()) {
                                symbolNode = symbols.next();
                            }
                            symbols.close();
                        }

                        if (symbolNode!=null){
                            writeSymbolInformation(symbolNode, jg);

                            jg.writeArrayFieldStart("features");
                            for (Relationship hasProteinCodingBiotypeRelationship : symbolNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasProteinCodingBiotypeRelationship())){
                                Node featureNode = hasProteinCodingBiotypeRelationship.getEndNode();

                                jg.writeStartObject();
                                writeFeatureInformation(featureNode, jg);
                                jg.writeEndObject();

                            }
                            jg.writeEndArray();

                        }

                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/annotation/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response annotationInfo(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        Node variantNode = graphDb.getNodeById(parameters.variantNodeId);

                        for (Relationship consequenceRel : variantNode.getRelationships(Direction.OUTGOING)) {
                            Node annotationNode = consequenceRel.getEndNode();

                            if (annotationNode.hasLabel(VariantDatabase.getAnnotationLabel())){

                                for (Relationship inFeatureRel : annotationNode.getRelationships(Direction.OUTGOING, VariantDatabase.getInFeatureRelationship())) {
                                    Node featureNode = inFeatureRel.getEndNode();

                                    if (featureNode.hasLabel(VariantDatabase.getFeatureLabel())){

                                        for (Relationship biotypeRel : featureNode.getRelationships(Direction.INCOMING)) {
                                            Node symbolNode = biotypeRel.getStartNode();

                                            if (symbolNode.hasLabel(VariantDatabase.getSymbolLabel())){

                                                jg.writeStartObject();

                                                writeSymbolInformation(symbolNode, jg);
                                                writeFeatureInformation(featureNode, jg);
                                                writeFunctionalAnnotation(annotationNode, consequenceRel, biotypeRel, jg);

                                                jg.writeEndObject();

                                            }

                                        }

                                    }

                                }

                            }

                        }

                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/sample/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response sampleInfo(final String json) {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    Node sampleNode = null;

                    jg.writeStartObject();

                    try (Transaction tx = graphDb.beginTx()) {

                        try (ResourceIterator<Node> samples = graphDb.findNodes(VariantDatabase.getSampleLabel(), "sampleId", parameters.sampleId)) {
                            while (samples.hasNext()) {
                                sampleNode = samples.next();
                            }
                            samples.close();
                        }

                        if (sampleNode != null) {
                            jg.writeArrayFieldStart("analyses");
                            for (Relationship hasAnalysisRelationship : sampleNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasAnalysisRelationship())){
                                jg.writeStartObject();
                                writeRunInformation(hasAnalysisRelationship.getEndNode(), jg);
                                jg.writeEndObject();
                            }
                            jg.writeEndArray();

                            writeSampleInformation(sampleNode, jg);
                        }

                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/analyses/pendingqc")
    @Produces(MediaType.APPLICATION_JSON)
    public Response analysesPendingQc() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(VariantDatabase.getRunInfoLabel())){

                            while (iter.hasNext()) {
                                Node runInfoNode = iter.next();

                                //check run has passed QC
                                Node lastEventNode = getLastUserEventNode(runInfoNode);

                                if (lastEventNode.getId() != runInfoNode.getId()){
                                    UserEventStatus status = getUserEventStatus(lastEventNode);

                                    //skip pending QC
                                    if (status == UserEventStatus.REJECTED){
                                        jg.writeStartObject();
                                        writeSampleInformation(runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode(), jg);
                                        writeRunInformation(runInfoNode, jg);
                                        jg.writeEndObject();
                                    }

                                } else {
                                    jg.writeStartObject();
                                    writeSampleInformation(runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode(), jg);
                                    writeRunInformation(runInfoNode, jg);
                                    jg.writeEndObject();
                                }

                            }

                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/analyses/addqc")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response analysesAddQc(final String json) {

        try {

            Node runInfoNode, userNode;
            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            //get nodes
            try (Transaction tx = graphDb.beginTx()) {
                runInfoNode = graphDb.getNodeById(parameters.runInfoNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
            }

            //check variant does not already have outstanding auths
            Node lastEventNode = getLastUserEventNode(runInfoNode);

            if (lastEventNode.getId() != runInfoNode.getId()){
                UserEventStatus status = getUserEventStatus(lastEventNode);

                if (status == UserEventStatus.PENDING_AUTH){
                    throw new IllegalArgumentException("Cannot add QC result. Auth pending.");
                }

            }

            //add properties
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("passOrFail", parameters.passOrFail);

            if (parameters.evidence != null) {
                if (!parameters.evidence.equals("")) properties.put("evidence", parameters.evidence);
            }

            //add event
            addUserEvent(lastEventNode, VariantDatabase.getQualityControlLabel(), properties, userNode);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (IllegalArgumentException e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/analyses/pendingauth")
    @Produces(MediaType.APPLICATION_JSON)
    public Response analysesPendingAuth() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> iter = graphDb.findNodes(VariantDatabase.getQualityControlLabel())){

                            while (iter.hasNext()) {
                                Node qualityControlNode = iter.next();

                                if (getUserEventStatus(qualityControlNode) == UserEventStatus.PENDING_AUTH){

                                    Relationship addedByRelationship = qualityControlNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);

                                    jg.writeStartObject();

                                    jg.writeNumberField("eventNodeId", qualityControlNode.getId());
                                    jg.writeStringField("event", "Quality Control");
                                    jg.writeBooleanField("value", (boolean) qualityControlNode.getProperty("passOrFail"));
                                    if (qualityControlNode.hasProperty("evidence")) jg.writeStringField("evidence", qualityControlNode.getProperty("evidence").toString());

                                    jg.writeObjectFieldStart("add");
                                    writeLiteUserInformation(addedByRelationship.getEndNode(), jg);
                                    jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                                    jg.writeEndObject();

                                    Node runInfoNode = getSubjectNodeFromEventNode(qualityControlNode);

                                    writeRunInformation(runInfoNode, jg);
                                    writeSampleInformation(runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode(), jg);

                                    jg.writeEndObject();

                                }

                            }

                        }
                    }

                    jg.writeEndArray();

                    jg.flush();
                    jg.close();

                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/user/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response userInfo(final String json) {

        try{

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    Node userNode = null;

                    try (Transaction tx = graphDb.beginTx()) {

                        //find variant
                        try (ResourceIterator<Node> users = graphDb.findNodes(VariantDatabase.getUserLabel(), "userId", parameters.userId)) {
                            while (users.hasNext()) {
                                userNode = users.next();
                            }
                            users.close();
                        }

                    }

                    jg.writeStartObject();
                    if (userNode != null) writeFullUserInformation(userNode, jg);
                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/user/updatepassword")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response userUpdatePassword(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.getNodeById(parameters.userNodeId);
                userNode.setProperty("password", parameters.password);
                tx.success();
            }

            return Response.status(Response.Status.OK).build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/user/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response userAdd(final String json) {

        try {
            User user = objectMapper.readValue(json, User.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node newUserNode = graphDb.createNode(VariantDatabase.getUserLabel());

                newUserNode.setProperty("fullName", user.fullName);
                newUserNode.setProperty("password", user.password);
                newUserNode.setProperty("jobTitle", user.jobTitle);
                newUserNode.setProperty("userId", user.userId);
                newUserNode.setProperty("contactNumber", user.contactNumber);
                newUserNode.setProperty("admin", user.admin);

                tx.success();
            }

            return Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/admin/authevent")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response adminAuthEvent(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);
            Node eventNode, userNode;

            try (Transaction tx = graphDb.beginTx()) {
                eventNode = graphDb.getNodeById(parameters.eventNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);

                if (!(boolean) userNode.getProperty("admin")) {
                    throw new CredentialException("Admin rights required for this operation.");
                }

            }

            if (getUserEventStatus(eventNode) != UserEventStatus.PENDING_AUTH) {
                throw new IllegalArgumentException("Event has no pending authorisation");
            }

            authUserEvent(eventNode, userNode, parameters.addOrRemove);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (CredentialException e){
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/report")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response report(final String json) {

        try {
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    PrintWriter pw = new PrintWriter(os);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    float maxAf;
                    DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");

                    try (Transaction tx = graphDb.beginTx()) {

                        Node runInfoNode = graphDb.getNodeById(parameters.runInfoNodeId);
                        Node sampleNode = runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode();

                        //headers
                        pw.print("#Variant Report v1\n");
                        pw.print("#Created " + graphDb.getNodeById(parameters.userNodeId).getProperty("fullName") + " " + dateFormat.format(new Date()) + "\n");
                        pw.print("#" + parameters.workflowName + "\n");
                        pw.print("#SampleId\tWorklistId\tVariant\tGenotype\tQuality\tOccurrence\tdbSNP\tGERP++\tPhyloP\tPhastCons\t");

                        //print pop freq header
                        for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {
                            pw.print("1KG_" + population.toString() + "\t");
                        }
                        pw.print("Max_1KG\t");
                        for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                            pw.print("ExAC_" + population.toString() + "\t");
                        }
                        pw.print("Max_ExAC\t");

                        pw.print("Gene\tTranscript\tTranscriptType\tTranscriptBiotype\tCanonicalTranscript\tPreferredTranscript\tConsequence\tSevere\tOMIM\tInternalClass\tClinVar\tHGVSc\tHGVSp\tLocation\tSIFT\tPolyPhen\tCodons\n");

                        //loop over variants node ids
                        for (long variantNodeId : parameters.variantNodeIds){
                            Node variantNode = graphDb.getNodeById(variantNodeId);
                            Node lastActiveEventNode = getLastActiveUserEventNode(variantNode);

                            //loop over selected variants
                            for (Relationship relationship : variantNode.getRelationships(Direction.INCOMING)){
                                if (relationship.getStartNode().getId() == parameters.runInfoNodeId){

                                    for (Relationship consequenceRel : variantNode.getRelationships(Direction.OUTGOING)) {
                                        Node annotationNode = consequenceRel.getEndNode();

                                        if (annotationNode.hasLabel(VariantDatabase.getAnnotationLabel())){

                                            for (Relationship inFeatureRel : annotationNode.getRelationships(Direction.OUTGOING, VariantDatabase.getInFeatureRelationship())) {
                                                Node featureNode = inFeatureRel.getEndNode();

                                                if (featureNode.hasLabel(VariantDatabase.getFeatureLabel())){

                                                    for (Relationship biotypeRel : featureNode.getRelationships(Direction.INCOMING)) {
                                                        Node symbolNode = biotypeRel.getStartNode();

                                                        if (symbolNode.hasLabel(VariantDatabase.getSymbolLabel())){

                                                            //sample
                                                            if (sampleNode.hasProperty("sampleId")) pw.print(sampleNode.getProperty("sampleId").toString() + "\t"); else pw.print("\t");
                                                            if (runInfoNode.hasProperty("worklistId")) pw.print(runInfoNode.getProperty("worklistId").toString() + "\t"); else pw.print("\t");

                                                            //variant
                                                            if (variantNode.hasProperty("variantId")) pw.print(variantNode.getProperty("variantId").toString() + "\t"); else pw.print("\t");
                                                            pw.print(getVariantInheritance(relationship.getType().name()) + "\t");
                                                            if (relationship.hasProperty("quality")) pw.print(relationship.getProperty("quality").toString() + "\t"); else pw.print("\t");
                                                            pw.print(getGlobalVariantOccurrenceQcPass(variantNode) + "\t");
                                                            if (variantNode.hasProperty("dbSnpId")) pw.print(variantNode.getProperty("dbSnpId").toString() + "\t"); else pw.print("\t");
                                                            if (variantNode.hasProperty("gerp")) pw.print(variantNode.getProperty("gerp").toString() + "\t"); else pw.print("\t");
                                                            if (variantNode.hasProperty("phyloP")) pw.print(variantNode.getProperty("phyloP").toString() + "\t"); else pw.print("\t");
                                                            if (variantNode.hasProperty("phastCons")) pw.print(variantNode.getProperty("phastCons").toString() + "\t"); else pw.print("\t");

                                                            //1kg
                                                            maxAf = -1f;
                                                            for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {
                                                                if (variantNode.hasProperty("kGPhase3" + population.toString() + "Af")){
                                                                    pw.print(variantNode.getProperty("kGPhase3" + population.toString() + "Af").toString() + "\t");

                                                                    float tmp = (float) variantNode.getProperty("kGPhase3" + population.toString() + "Af");
                                                                    if (tmp > maxAf) maxAf = tmp;

                                                                } else {
                                                                    pw.print("\t");
                                                                }
                                                            }
                                                            if (maxAf != -1f){
                                                                pw.print(Float.toString(maxAf) + "\t");
                                                            } else {
                                                                pw.print("0\t");
                                                            }

                                                            //ExAC
                                                            maxAf = -1f;
                                                            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                                                                if (variantNode.hasProperty("exac" + population.toString() + "Af")){
                                                                    pw.print(variantNode.getProperty("exac" + population.toString() + "Af").toString() + "\t");

                                                                    float tmp = (float) variantNode.getProperty("exac" + population.toString() + "Af");
                                                                    if (tmp > maxAf) maxAf = tmp;

                                                                } else {
                                                                    pw.print("\t");
                                                                }
                                                            }
                                                            if (maxAf != -1f){
                                                                pw.print(Float.toString(maxAf) + "\t");
                                                            } else {
                                                                pw.print("0\t");
                                                            }

                                                            //gene & transcript
                                                            if (symbolNode.hasProperty("symbolId")) pw.print(symbolNode.getProperty("symbolId").toString() + "\t"); else pw.print("\t");
                                                            if (featureNode.hasProperty("featureId")) pw.print(featureNode.getProperty("featureId").toString() + "\t"); else pw.print("\t");
                                                            if (featureNode.hasProperty("featureType")) pw.print(featureNode.getProperty("featureType").toString() + "\t"); else pw.print("\t");
                                                            pw.print(getTranscriptBiotype(biotypeRel.getType().name()) + "\t");

                                                            //transcript choice
                                                            if (featureNode.hasLabel(VariantDatabase.getCanonicalLabel())) {
                                                                pw.print("TRUE\t");
                                                            } else {
                                                                pw.print("FALSE\t");
                                                            }

                                                            //internal choice
                                                            Node lastActiveEventFeaturePrefNode =  getLastActiveUserEventNode(featureNode);
                                                            if (lastActiveEventFeaturePrefNode != null){
                                                                pw.print(lastActiveEventFeaturePrefNode.getProperty("preference").toString() + "\t");
                                                            } else {
                                                                pw.print("\t");
                                                            }

                                                            //functional annotations
                                                            String consequence = getFunctionalConsequence(consequenceRel.getType().name());
                                                            pw.print(consequence + "\t");
                                                            pw.print(isConsequenceSevere(consequence) + "\t");

                                                            //omim
                                                            for (Relationship hasAssociatedSymbol : symbolNode.getRelationships(Direction.INCOMING, VariantDatabase.getHasAssociatedSymbol())){
                                                                Node disorderNode = hasAssociatedSymbol.getStartNode();
                                                                pw.print(disorderNode.getProperty("disorder").toString() + ";");
                                                            }
                                                            pw.print("\t");

                                                            if (lastActiveEventNode != null){
                                                                pw.print(lastActiveEventNode.getProperty("classification").toString());
                                                            }
                                                            pw.print("\t");

                                                            if (variantNode.hasProperty("clinvar")){
                                                                int[] clinvarCodes = (int[]) variantNode.getProperty("clinvar");
                                                                for (int i = 0; i < clinvarCodes.length; ++i){
                                                                    pw.print(ClinVarCode.get(clinvarCodes[i]).name());
                                                                    if (i != clinvarCodes.length - 1) pw.print(";");
                                                                }
                                                            }
                                                            pw.print("\t");

                                                            if (annotationNode.hasProperty("hgvsc")) pw.print(annotationNode.getProperty("hgvsc").toString() + "\t"); else pw.print("\t");
                                                            if (annotationNode.hasProperty("hgvsp")) pw.print(annotationNode.getProperty("hgvsp").toString() + "\t"); else pw.print("\t");

                                                            if (annotationNode.hasProperty("exon")) {
                                                                pw.print(annotationNode.getProperty("exon").toString() + "\t");
                                                            } else if (annotationNode.hasProperty("intron")) {
                                                                pw.print(annotationNode.getProperty("intron").toString() + "\t");
                                                            } else {
                                                                pw.print("\t");
                                                            }

                                                            if (annotationNode.hasProperty("sift")) pw.print(annotationNode.getProperty("sift").toString() + "\t"); else pw.print("\t");
                                                            if (annotationNode.hasProperty("polyphen")) pw.print(annotationNode.getProperty("polyphen").toString() + "\t"); else pw.print("\t");
                                                            if (annotationNode.hasProperty("codons")) pw.print(annotationNode.getProperty("codons").toString() + "\n"); else pw.print("\n");

                                                        }

                                                    }

                                                }

                                            }

                                        }

                                    }
                                }
                            }

                        }

                    }

                    pw.flush();
                    pw.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @GET
    @Path("/omim/add")
    @Produces(MediaType.APPLICATION_JSON)
    public Response omimAdd() {

        try {

            String inputLine;
            Node symbolNode, disorderNode;
            URL omimFtp = new URL("http://data.omim.org/downloads/NFUI_mdqQbaQADxKesNmgg/morbidmap.txt");
            BufferedReader in = new BufferedReader(new InputStreamReader(omimFtp.openStream()));

            //add to database
            try (Transaction tx = graphDb.beginTx()) {
                while ((inputLine = in.readLine()) != null){

                    if (Pattern.matches("^#.*", inputLine)) continue;
                    String[] fields = inputLine.split("\t");

                    //match or create disorder
                    ArrayList<Node> disorders = Neo4j.getNodes(graphDb, VariantDatabase.getDisorderLabel(), "disorder", fields[0].trim());

                    if (disorders.size() == 0){
                        disorderNode = graphDb.createNode(VariantDatabase.getDisorderLabel());
                        disorderNode.setProperty("disorder", fields[0].trim());
                    } else {
                        disorderNode = disorders.get(0);
                    }

                    //match or create symbol
                    for (String field : fields[1].split(",")){
                        String symbolId = field.trim();

                        ArrayList<Node> symbols = Neo4j.getNodes(graphDb, VariantDatabase.getSymbolLabel(), "symbolId", symbolId);

                        if (symbols.size() == 0){
                            symbolNode = graphDb.createNode(VariantDatabase.getSymbolLabel());
                            symbolNode.setProperty("symbolId", symbolId);
                        } else {
                            symbolNode = symbols.get(0);
                        }

                        //check relationship does not already exist & link symbol and disorder
                        boolean connected = false;
                        for (Relationship hasAssociatedSymbolRelationship : disorderNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasAssociatedSymbol())){
                            if (hasAssociatedSymbolRelationship.getEndNode().getProperty("symbolId").toString().equals(symbolNode.getProperty("symbolId").toString())){
                                connected = true;
                                break;
                            }
                        }

                        if (!connected){
                            disorderNode.createRelationshipTo(symbolNode, VariantDatabase.getHasAssociatedSymbol());
                        }

                    }

                }
                tx.success();
            }

            in.close();

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/omim/remove")
    @Produces(MediaType.APPLICATION_JSON)
    public Response omimRemove() {

        try {

            try (Transaction tx = graphDb.beginTx()) {
                try (ResourceIterator<Node> iter = graphDb.findNodes(VariantDatabase.getDisorderLabel())) {

                    while (iter.hasNext()) {
                        Node disorderNode = iter.next();

                        //delete relationships
                        for (Relationship hasAssociatedSymbolRelationship : disorderNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasAssociatedSymbol())){
                            hasAssociatedSymbolRelationship.delete();
                        }

                        //delete node
                        disorderNode.delete();

                    }
                }

                tx.success();
            }

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/clinvar/add")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clinvarAdd() {
        VCFCodec codec = new VCFCodec();

        //read VCF from clinvar, decompress and import on the fly
        try (LineIteratorImpl lineIteratorImpl = new LineIteratorImpl(LineReaderUtil.fromBufferedStream(new GZIPInputStream(new URL("ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz").openStream())))) {
            codec.readActualHeader(lineIteratorImpl);

            while(lineIteratorImpl.hasNext())
            {
                VariantContext variantContext = codec.decode(lineIteratorImpl.next());

                if (variantContext.getAttribute("CLNALLE") instanceof String) {
                    if (variantContext.getAttribute("CLNSIG") == null) continue; //not always present

                    int clinAllele = Integer.parseInt((String) variantContext.getAttribute("CLNALLE"));
                    if (clinAllele == -1) continue; //A value of -1 indicates that no allele was found to match a corresponding HGVS allele name
                    String clinSig = (String) variantContext.getAttribute("CLNSIG");

                    GenomeVariant genomeVariant = new GenomeVariant(variantContext.getContig(), variantContext.getStart(), variantContext.getReference().getBaseString(), variantContext.getAlleles().get(clinAllele).getBaseString());
                    genomeVariant.convertToMinimalRepresentation();

                    ArrayList<Node> nodes = Neo4j.getNodes(graphDb, VariantDatabase.getVariantLabel(), "variantId", genomeVariant.toString());

                    if (nodes.size() == 1){

                        logger.info("Adding clinvar to " + genomeVariant.toString());

                        String[] clinSigsString = clinSig.split("\\|");
                        int[] clinSigsInt = new int[clinSigsString.length];

                        for (int i = 0; i < clinSigsString.length; ++i){
                            clinSigsInt[i] = Integer.parseInt(clinSigsString[i]);
                        }

                        try (Transaction tx = graphDb.beginTx()) {
                            nodes.get(0).setProperty("clinvar", clinSigsInt);
                            tx.success();
                        }

                    }

                } else if (variantContext.getAttribute("CLNALLE") instanceof ArrayList) {
                    if (variantContext.getAttribute("CLNSIG") == null) continue; //not always present

                    ArrayList<String> clinSigs = (ArrayList<String>) variantContext.getAttribute("CLNSIG");
                    ArrayList<String> clinAlleles = (ArrayList<String>) variantContext.getAttribute("CLNALLE");

                    for (int n = 0; n < clinAlleles.size(); ++n){

                        int clinAllele = Integer.parseInt(clinAlleles.get(n));
                        if (clinAllele == -1) continue; //A value of -1 indicates that no allele was found to match a corresponding HGVS allele name

                        GenomeVariant genomeVariant = new GenomeVariant(variantContext.getContig(), variantContext.getStart(), variantContext.getReference().getBaseString(), variantContext.getAlleles().get(clinAllele).getBaseString());
                        genomeVariant.convertToMinimalRepresentation();

                        ArrayList<Node> nodes = Neo4j.getNodes(graphDb, VariantDatabase.getVariantLabel(), "variantId", genomeVariant.toString());

                        if (nodes.size() == 1){

                            logger.info("Adding clinvar to " + genomeVariant.toString());

                            String[] clinSigsString = clinSigs.get(n).split("\\|");
                            int[] clinSigsInt = new int[clinSigsString.length];

                            for (int i = 0; i < clinSigsString.length; ++i){
                                clinSigsInt[i] = Integer.parseInt(clinSigsString[i]);
                            }

                            try (Transaction tx = graphDb.beginTx()) {
                                nodes.get(0).setProperty("clinvar", clinSigsInt);
                                tx.success();
                            }

                        }

                    }

                }

            }

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/clinvar/remove")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clinvarRemove() {
        try {

            try (Transaction tx = graphDb.beginTx()) {
                try (ResourceIterator<Node> iter = graphDb.findNodes(VariantDatabase.getVariantLabel())) {

                    while (iter.hasNext()) {
                        Node variantNode = iter.next();

                        if (variantNode.hasProperty("clinvar")){
                            variantNode.removeProperty("clinvar");
                        }

                    }
                }

                tx.success();
            }

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            logger.error(e.getMessage());
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    /*workflows*/
    @Workflow(name = "Rare Variant Workflow v1", description = "A workflow to prioritise rare calls")
    public void runRareVariantWorkflowv1(JsonGenerator jg, HashSet<Long> excludeRunInfoNodes, HashSet<Long> includePanelNodes, Node runInfoNode) throws IOException {

        boolean includeCallsFromPanel = false, excludeCallsFromSample = false;
        int class1Calls = 0, not1KGRareVariants = 0, notExACRareVariants = 0, passVariants = 0, total = 0;

        if (includePanelNodes.size() > 0) includeCallsFromPanel = true;
        if (excludeRunInfoNodes.size() > 0) excludeCallsFromSample = true;

        jg.writeStartObject();

        jg.writeFieldName("variants");
        jg.writeStartArray();

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {

                if (!inheritanceRel.getType().equals(VariantDatabase.getHasHomVariantRelationship()) &&
                        !inheritanceRel.getType().equals(VariantDatabase.getHasHetVariantRelationship())){
                    continue;
                }

                Node variantNode = inheritanceRel.getEndNode();

                //check if variant belongs to supplied panel
                if (includeCallsFromPanel && !variantBelongsToVirtualPanel(variantNode, includePanelNodes)){
                    continue;
                }

                //check if variant is not present in exclusion samples
                if (excludeCallsFromSample && variantPresentInExclusionSamples(variantNode, excludeRunInfoNodes)){
                    continue;
                }

                jg.writeStartObject();

                writeVariantInformation(variantNode, jg);
                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));

                //stratify variants
                Node lastActiveEventNode = getLastActiveUserEventNode(variantNode);
                if (lastActiveEventNode != null && lastActiveEventNode.hasProperty("classification")) {

                    if ((int) lastActiveEventNode.getProperty("classification") == 1){
                        jg.writeNumberField("filter", 0);
                        class1Calls++;
                    } else {
                        jg.writeNumberField("filter", 3);
                        passVariants++;
                    }

                } else if (!isExACRareVariant(variantNode, 0.01)) {
                    jg.writeNumberField("filter", 1);
                    notExACRareVariants++;
                } else if (!is1KGRareVariant(variantNode, 0.01)) {
                    jg.writeNumberField("filter", 2);
                    not1KGRareVariants++;
                } else {
                    jg.writeNumberField("filter", 3);
                    passVariants++;
                }

                total++;

                jg.writeEndObject();

            }

        }

        jg.writeEndArray();

        //write filters
        jg.writeFieldName("filters");
        jg.writeStartArray();

        jg.writeStartObject();
        jg.writeStringField("key", "Class 1");
        jg.writeNumberField("y", class1Calls);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "ExAC >1% Frequency");
        jg.writeNumberField("y", notExACRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "1KG >1% Frequency");
        jg.writeNumberField("y", not1KGRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Pass");
        jg.writeNumberField("y", passVariants);
        jg.writeEndObject();

        jg.writeEndArray();

        jg.writeNumberField("total", total);

        jg.writeEndObject();

    }

    @Deprecated
    @Workflow(name = "Autosomal Dominant Workflow v1", description = "A workflow to prioritise rare autosomal heterozygous calls")
    public void runAutosomalDominantWorkflowv1(JsonGenerator jg, HashSet<Long> excludeRunInfoNodes, HashSet<Long> includePanelNodes, Node runInfoNode) throws IOException {

        boolean includeCallsFromPanel = false, excludeCallsFromSample = false;
        int notHeterozygousVariants = 0, notAutosomalVariants = 0, not1KGRareVariants = 0, notExACRareVariants = 0, passVariants = 0, total = 0;

        if (includePanelNodes.size() > 0) includeCallsFromPanel = true;
        if (excludeRunInfoNodes.size() > 0) excludeCallsFromSample = true;

        jg.writeStartObject();

        jg.writeFieldName("variants");
        jg.writeStartArray();

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                Node variantNode = inheritanceRel.getEndNode();

                //check if variant belongs to supplied panel
                if (includeCallsFromPanel && !variantBelongsToVirtualPanel(variantNode, includePanelNodes)){
                    continue;
                }

                //check if variant is not present in exclusion samples
                if (excludeCallsFromSample && variantPresentInExclusionSamples(variantNode, excludeRunInfoNodes)){
                    continue;
                }

                jg.writeStartObject();

                writeVariantInformation(variantNode, jg);
                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));

                //stratify variants
                if (inheritanceRel.isType(VariantDatabase.getHasHomVariantRelationship())) {
                    jg.writeNumberField("filter", 0);
                    notHeterozygousVariants++;
                } else if (!variantNode.hasLabel(VariantDatabase.getAutosomeLabel())) {
                    jg.writeNumberField("filter", 1);
                    notAutosomalVariants++;
                } else if (!isExACRareVariant(variantNode, 0.01)) {
                    jg.writeNumberField("filter", 2);
                    notExACRareVariants++;
                } else if (!is1KGRareVariant(variantNode, 0.01)) {
                    jg.writeNumberField("filter", 3);
                    not1KGRareVariants++;
                } else {
                    jg.writeNumberField("filter", 4);
                    passVariants++;
                }

                total++;

                jg.writeEndObject();

            }

        }

        jg.writeEndArray();

        //write filters
        jg.writeFieldName("filters");
        jg.writeStartArray();

        jg.writeStartObject();
        jg.writeStringField("key", "Homozygous");
        jg.writeNumberField("y", notHeterozygousVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Non Autosomal");
        jg.writeNumberField("y", notAutosomalVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "ExAC >1% Frequency");
        jg.writeNumberField("y", notExACRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "1KG >1% Frequency");
        jg.writeNumberField("y", not1KGRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Pass");
        jg.writeNumberField("y", passVariants);
        jg.writeEndObject();

        jg.writeEndArray();

        jg.writeNumberField("total", total);

        jg.writeEndObject();

    }

    @Deprecated
    @Workflow(name = "Rare Homozygous Workflow v1", description = "A workflow to prioritise rare homozygous calls")
    public void runRareHomozygousWorkflowv1(JsonGenerator jg, HashSet<Long> excludeRunInfoNodes, HashSet<Long> includePanelNodes, Node runInfoNode) throws IOException {

        boolean includeCallsFromPanel = false, excludeCallsFromSample = false;
        int notHomozygousVariants = 0, not1KGRareVariants = 0, notExACRareVariants = 0, passVariants = 0, total = 0;

        if (includePanelNodes.size() > 0) includeCallsFromPanel = true;
        if (excludeRunInfoNodes.size() > 0) excludeCallsFromSample = true;

        jg.writeStartObject();

        jg.writeFieldName("variants");
        jg.writeStartArray();

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                Node variantNode = inheritanceRel.getEndNode();

                //check if variant belongs to supplied panel
                if (includeCallsFromPanel && !variantBelongsToVirtualPanel(variantNode, includePanelNodes)){
                    continue;
                }

                //check if variant is not present in exclusion samples
                if (excludeCallsFromSample && variantPresentInExclusionSamples(variantNode, excludeRunInfoNodes)){
                    continue;
                }

                jg.writeStartObject();

                writeVariantInformation(variantNode, jg);
                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));

                //stratify variants
                if (inheritanceRel.isType(VariantDatabase.getHasHetVariantRelationship())) {
                    jg.writeNumberField("filter", 0);
                    notHomozygousVariants++;
                } else if (!isExACRareVariant(variantNode, 0.05)) {
                    jg.writeNumberField("filter", 1);
                    notExACRareVariants++;
                } else if (!is1KGRareVariant(variantNode, 0.05)) {
                    jg.writeNumberField("filter", 2);
                    not1KGRareVariants++;
                } else {
                    jg.writeNumberField("filter", 3);
                    passVariants++;
                }

                total++;

                jg.writeEndObject();

            }

        }

        jg.writeEndArray();

        //write filters
        jg.writeFieldName("filters");
        jg.writeStartArray();

        jg.writeStartObject();
        jg.writeStringField("key", "Heterozygous");
        jg.writeNumberField("y", notHomozygousVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "ExAC >5% Frequency");
        jg.writeNumberField("y", notExACRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "1KG >5% Frequency");
        jg.writeNumberField("y", not1KGRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Pass");
        jg.writeNumberField("y", passVariants);
        jg.writeEndObject();

        jg.writeEndArray();

        jg.writeNumberField("total", total);

        jg.writeEndObject();

    }

    @Deprecated
    @Workflow(name = "Autosomal Recessive Workflow v1", description = "A workflow to prioritise rare autosomal compound calls")
    public void runAutosomalRecessiveWorkflowv1(JsonGenerator jg, HashSet<Long> excludeRunInfoNodes, HashSet<Long> includePanelNodes, Node runInfoNode) throws IOException {

        boolean includeCallsFromPanel = false, excludeCallsFromSample = false, hasAssociatedSymbol;
        int notAutosomalVariants = 0, not1KGRareVariants = 0, notExACRareVariants = 0, noAnnotation = 0, singleGeneChange = 0, passVariants = 0, total = 0;
        HashMap<String, HashSet<Node>> callsByGene = new HashMap<>();
        HashSet<Long> uniqueIds = new HashSet<>();

        if (includePanelNodes.size() > 0) includeCallsFromPanel = true;
        if (excludeRunInfoNodes.size() > 0) excludeCallsFromSample = true;

        jg.writeStartObject();

        jg.writeFieldName("variants");
        jg.writeStartArray();

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                Node variantNode = inheritanceRel.getEndNode();

                //check if variant belongs to supplied panel
                if (includeCallsFromPanel && !variantBelongsToVirtualPanel(variantNode, includePanelNodes)){
                    continue;
                }

                //check if variant is not present in exclusion samples
                if (excludeCallsFromSample && variantPresentInExclusionSamples(variantNode, excludeRunInfoNodes)){
                    continue;
                }

                //stratify variants
                if (!variantNode.hasLabel(VariantDatabase.getAutosomeLabel())) {
                    jg.writeStartObject();

                    writeVariantInformation(variantNode, jg);
                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                    jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                    jg.writeNumberField("filter", 0);

                    notAutosomalVariants++;
                    total++;

                    jg.writeEndObject();
                } else if (!isExACRareVariant(variantNode, 0.05)) {
                    jg.writeStartObject();

                    writeVariantInformation(variantNode, jg);
                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                    jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                    jg.writeNumberField("filter", 1);

                    notExACRareVariants++;
                    total++;

                    jg.writeEndObject();
                } else if (!is1KGRareVariant(variantNode, 0.05)) {
                    jg.writeStartObject();

                    writeVariantInformation(variantNode, jg);
                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                    jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                    jg.writeNumberField("filter", 2);

                    not1KGRareVariants++;
                    total++;

                    jg.writeEndObject();
                } else {
                    hasAssociatedSymbol = false;
                    for (Relationship inSymbolRelationship : variantNode.getRelationships(Direction.OUTGOING, VariantDatabase.getInSymbolRelationship())){

                        hasAssociatedSymbol = true;
                        Node symbolNode = inSymbolRelationship.getEndNode();
                        String symbolId = symbolNode.getProperty("symbolId").toString();

                        if (!callsByGene.containsKey(symbolId)){
                            callsByGene.put(symbolId, new HashSet<Node>());
                        }

                        callsByGene.get(symbolId).add(variantNode);
                    }

                    //catch calls without symbol
                    if (!hasAssociatedSymbol){
                        jg.writeStartObject();

                        writeVariantInformation(variantNode, jg);
                        jg.writeStringField("Inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                        jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                        jg.writeNumberField("filter", 3);

                        noAnnotation++;
                        total++;

                        jg.writeEndObject();
                    }
                }

            } //done looping over variants

            //collect remaining calls in genes with multiple calls
            for (Map.Entry<String, HashSet<Node>> iter : callsByGene.entrySet()){

                if (iter.getValue().size() == 1){
                    for (Node variantNode : iter.getValue()){

                        if (uniqueIds.contains(variantNode.getId())) continue;
                        uniqueIds.add(variantNode.getId());

                        jg.writeStartObject();

                        writeVariantInformation(variantNode, jg);

                        //find inheritance
                        for (Relationship inheritanceRel : variantNode.getRelationships(Direction.INCOMING)){
                            if (inheritanceRel.getStartNode().equals(runInfoNode)){
                                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                            }
                        }

                        jg.writeNumberField("filter", 4);

                        singleGeneChange++;
                        total++;

                        jg.writeEndObject();

                    }
                } else {
                    for (Node variantNode : iter.getValue()){

                        if (uniqueIds.contains(variantNode.getId())) continue;
                        uniqueIds.add(variantNode.getId());

                        jg.writeStartObject();

                        writeVariantInformation(variantNode, jg);

                        //find inheritance
                        for (Relationship inheritanceRel : variantNode.getRelationships(Direction.INCOMING)){
                            if (inheritanceRel.getStartNode().equals(runInfoNode)){
                                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));
                            }
                        }

                        jg.writeNumberField("filter", 5);

                        passVariants++;
                        total++;

                        jg.writeEndObject();

                    }
                }

            }

        }

        jg.writeEndArray();

        //write filters
        jg.writeFieldName("filters");
        jg.writeStartArray();

        jg.writeStartObject();
        jg.writeStringField("key", "Non Autosomal");
        jg.writeNumberField("y", notAutosomalVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "ExAC >5% Frequency");
        jg.writeNumberField("y", notExACRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "1KG >5% Frequency");
        jg.writeNumberField("y", not1KGRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "No Annotation");
        jg.writeNumberField("y", noAnnotation);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Single Gene Change");
        jg.writeNumberField("y", singleGeneChange);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Pass");
        jg.writeNumberField("y", passVariants);
        jg.writeEndObject();

        jg.writeEndArray();

        jg.writeNumberField("total", total);

        jg.writeEndObject();

    }

    @Deprecated
    @Workflow(name = "X Linked Workflow v1", description = "A workflow to prioritise X-linked calls")
    public void runXLinkedWorkflowv1(JsonGenerator jg, HashSet<Long> excludeRunInfoNodes, HashSet<Long> includePanelNodes, Node runInfoNode) throws IOException {

        boolean includeCallsFromPanel = false, excludeCallsFromSample = false;
        int notXChromosomeVariants = 0, notExACRareVariants = 0, not1KGRareVariants = 0, passVariants = 0, total = 0;

        if (includePanelNodes.size() > 0) includeCallsFromPanel = true;
        if (excludeRunInfoNodes.size() > 0) excludeCallsFromSample = true;

        jg.writeStartObject();

        jg.writeFieldName("variants");
        jg.writeStartArray();

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                Node variantNode = inheritanceRel.getEndNode();

                //check if variant belongs to supplied panel
                if (includeCallsFromPanel && !variantBelongsToVirtualPanel(variantNode, includePanelNodes)){
                    continue;
                }

                //check if variant is not present in exclusion samples
                if (excludeCallsFromSample && variantPresentInExclusionSamples(variantNode, excludeRunInfoNodes)){
                    continue;
                }

                jg.writeStartObject();

                writeVariantInformation(variantNode, jg);
                jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                jg.writeNumberField("quality", (short) inheritanceRel.getProperty("quality"));

                //stratify variants
                if (!variantNode.hasLabel(VariantDatabase.getxChromLabel())) {
                    jg.writeNumberField("filter", 0);
                    notXChromosomeVariants++;
                } else if (!isExACRareVariant(variantNode, 0.05)) {
                    jg.writeNumberField("filter", 1);
                    notExACRareVariants++;
                } else if (!is1KGRareVariant(variantNode, 0.05)) {
                    jg.writeNumberField("filter", 2);
                    not1KGRareVariants++;
                } else {
                    jg.writeNumberField("filter", 3);
                    passVariants++;
                }

                total++;

                jg.writeEndObject();

            }

        }

        jg.writeEndArray();

        //write filters
        jg.writeFieldName("filters");
        jg.writeStartArray();

        jg.writeStartObject();
        jg.writeStringField("key", "NotXLinked");
        jg.writeNumberField("y", notXChromosomeVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "ExAC >5% Frequency");
        jg.writeNumberField("y", notExACRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "1KG >5% Frequency");
        jg.writeNumberField("y", not1KGRareVariants);
        jg.writeEndObject();

        jg.writeStartObject();
        jg.writeStringField("key", "Pass");
        jg.writeNumberField("y", passVariants);
        jg.writeEndObject();

        jg.writeEndArray();

        jg.writeNumberField("total", total);

        jg.writeEndObject();

    }

    /*helper functions*/
    private Boolean variantHasSevereConsequence(Node variantNode){
        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship consequenceRel : variantNode.getRelationships(Direction.OUTGOING)){
                if (consequenceRel.getEndNode().hasLabel(VariantDatabase.getAnnotationLabel())){
                    Boolean severity = isConsequenceSevere(getFunctionalConsequence(consequenceRel.getType().name()));

                    if (severity != null && severity){
                        return true;
                    }

                }
            }
        }
        return false;
    }
    private Boolean isConsequenceSevere(String consequence){
        switch (consequence) {
            case "3_PRIME_UTR_VARIANT":
                return false;
            case "5_PRIME_UTR_VARIANT":
                return false;
            case "DOWNSTREAM_GENE_VARIANT":
                return false;
            case "UPSTREAM_GENE_VARIANT":
                return false;
            case "INTRON_VARIANT":
                return false;
            case "SYNONYMOUS_VARIANT":
                return false;
            case "SPLICE_REGION_VARIANT":
                return false;
            case "MISSENSE_VARIANT":
                return true;
            case "STOP_LOST":
                return true;
            case "INFRAME_DELETION":
                return true;
            case "INFRAME_INSERTION":
                return true;
            case "FRAMESHIFT_VARIANT":
                return true;
            case "SPLICE_ACCEPTOR_VARIANT":
                return true;
            case "SPLICE_DONOR_VARIANT":
                return true;
            case "STOP_GAINED":
                return true;
            case "START_LOST":
                return true;
            case "CODING_SEQUENCE_VARIANT":
                return true;
            case "STOP_RETAINED_VARIANT":
                return true;
            case "PROTEIN_ALTERING_VARIANT":
                return true;
            case "INCOMPLETE_TERMINAL_CODON_VARIANT":
                return true;
            default:
                return null;
        }
    }
    private int getGlobalVariantOccurrenceQcPass(Node variantNode){
        int occurrence = 0;

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship relationship : variantNode.getRelationships(Direction.INCOMING)) {
                Node runInfoNode = relationship.getStartNode();

                if (runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())) {

                    //check if run has passed QC
                    Node qcNode = getLastActiveUserEventNode(runInfoNode);
                    if (qcNode == null || !(boolean) qcNode.getProperty("passOrFail")){
                        continue;
                    }

                    if (relationship.isType(VariantDatabase.getHasHetVariantRelationship()) && relationship.getStartNode().hasLabel(VariantDatabase.getRunInfoLabel())) {
                        occurrence += 1;
                    } else if (relationship.isType(VariantDatabase.getHasHomVariantRelationship()) && relationship.getStartNode().hasLabel(VariantDatabase.getRunInfoLabel())) {
                        occurrence += 2;
                    }

                }

            }
        }

        return occurrence;
    }
    private boolean is1KGRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {

                if (variantNode.hasProperty("kGPhase3" + population.toString() + "Af")){
                    if ((float) variantNode.getProperty("kGPhase3" + population.toString() + "Af") > maxAlleleFrequency){
                        return false;
                    }
                }

            }

        }

        return true;
    }
    private boolean isExACRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {

                if (variantNode.hasProperty("exac" + population.toString() + "Af")){
                    if ((float) variantNode.getProperty("exac" + population.toString() + "Af") > maxAlleleFrequency){
                        return false;
                    }
                }

            }

        }

        return true;
    }
    private String getVariantInheritance(String inheritanceRelationshipTypeName){
        if (inheritanceRelationshipTypeName.length() > 12) {
            return inheritanceRelationshipTypeName.substring(4, inheritanceRelationshipTypeName.length() - 8);
        } else {
            return null;
        }
    }
    private double getVariantInternalFrequency(int panelOccurrence, int variantOccurrence){
        return (double) Math.round((((double) variantOccurrence / (panelOccurrence * 2)) * 100) * 100d) / 100d;
    }
    private boolean variantBelongsToVirtualPanel(Node variantNode, HashSet<Long> panelNodeIds){

        try (Transaction tx = graphDb.beginTx()) {

            //check variant belongs to virtual panel
            if (variantNode.hasLabel(VariantDatabase.getVariantLabel())) {

                for (Relationship inSymbolRel : variantNode.getRelationships(Direction.OUTGOING, VariantDatabase.getInSymbolRelationship())) {
                    Node symbolNode = inSymbolRel.getEndNode();

                    if (symbolNode.hasLabel(VariantDatabase.getSymbolLabel())) {

                        for (Relationship containsSymbol : symbolNode.getRelationships(Direction.INCOMING, VariantDatabase.getContainsSymbolRelationship())) {
                            Node virtualPanelNode = containsSymbol.getStartNode();

                            //check if variant belongs to the virtual panel
                            if (panelNodeIds.contains(virtualPanelNode.getId())) {
                                return true;
                            }

                        }

                    }
                }

            }

        }

        return false;
    }
    private boolean variantPresentInExclusionSamples(Node variantNode, HashSet<Long> excludedRunInfoNodeIds){

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship inheritanceRelationship : variantNode.getRelationships(Direction.INCOMING)) {
                Node foreignRunInfoNode = inheritanceRelationship.getStartNode();

                if (excludedRunInfoNodeIds.contains(foreignRunInfoNode.getId())){
                    return true;
                }

            }
        }

        return false;
    }
    private String getTranscriptBiotype(String biotypeRelName){
        if (biotypeRelName.length() > 12) {
            return biotypeRelName.substring(4, biotypeRelName.length() - 8);
        } else {
            return biotypeRelName;
        }
    }
    private String getFunctionalConsequence(String consequenceRelName){
        if (consequenceRelName.length() > 12) {
            return consequenceRelName.substring(4, consequenceRelName.length() - 12);
        } else {
            return consequenceRelName;
        }
    }
    private Node getSubjectNodeFromEventNode(Node eventNode){

        Node subjectNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(VariantDatabase.getHasUserEventRelationship(), Direction.INCOMING)
                    .traverse(eventNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                subjectNode = node;
            }

        }

        return subjectNode;
    }
    private Node getLastUserEventNode(Node subjectNode){

        Node lastEventNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(VariantDatabase.getHasUserEventRelationship(), Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                lastEventNode = node;
            }

        }

        return lastEventNode;
    }
    private Node getLastActiveUserEventNode(Node subjectNode){

        Node lastEventNode = null;
        org.neo4j.graphdb.Path longestPath = null;

        try (Transaction tx = graphDb.beginTx()) {
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(VariantDatabase.getHasUserEventRelationship(), Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node node : longestPath.nodes()) {
                if (getUserEventStatus(node) == UserEventStatus.ACTIVE){
                    lastEventNode = node;
                }
            }

        }

        return lastEventNode;
    }
    private void addUserEvent(Node lastUserEventNode, Label newEventNodeLabel, HashMap<String, Object> properties, Node userNode) {

        try (Transaction tx = graphDb.beginTx()) {
            Node newEventNode = graphDb.createNode(newEventNodeLabel);

            for (Map.Entry<String, Object> iter : properties.entrySet()){
                newEventNode.setProperty(iter.getKey(), iter.getValue());
            }

            Relationship addedByRelationship = newEventNode.createRelationshipTo(userNode, VariantDatabase.getAddedByRelationship());
            addedByRelationship.setProperty("date", new Date().getTime());

            lastUserEventNode.createRelationshipTo(newEventNode, VariantDatabase.getHasUserEventRelationship());

            tx.success();
        }

    }
    private void authUserEvent(Node eventNode, Node userNode, boolean acceptOrReject){
        try (Transaction tx = graphDb.beginTx()) {
            Relationship authByRelationship = eventNode.createRelationshipTo(userNode, acceptOrReject ? VariantDatabase.getAuthorisedByRelationship() : VariantDatabase.getRejectedByRelationship());
            authByRelationship.setProperty("date", new Date().getTime());
            tx.success();
        }
    }
    private UserEventStatus getUserEventStatus(Node eventNode) {
        Relationship addedbyRelationship, authorisedByRelationship, rejectedByRelationship;

        try (Transaction tx = graphDb.beginTx()) {
            addedbyRelationship = eventNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);
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

    /*write functions*/
    private void writeFullUserInformation(Node userNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!userNode.hasLabel(VariantDatabase.getUserLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getUserLabel().name() + " got " + userNode.getLabels().toString());

            jg.writeNumberField("userNodeId", userNode.getId());

            if (userNode.hasProperty("userId")) jg.writeStringField("userId", userNode.getProperty("userId").toString());
            if (userNode.hasProperty("password")) jg.writeStringField("password", userNode.getProperty("password").toString());
            if (userNode.hasProperty("admin")) jg.writeBooleanField("admin", (boolean) userNode.getProperty("admin"));
            if (userNode.hasProperty("fullName")) jg.writeStringField("fullName", userNode.getProperty("fullName").toString());
            if (userNode.hasProperty("jobTitle")) jg.writeStringField("jobTitle", userNode.getProperty("jobTitle").toString());
            if (userNode.hasProperty("emailAddress")) jg.writeStringField("emailAddress", userNode.getProperty("emailAddress").toString());
            if (userNode.hasProperty("contactNumber")) jg.writeStringField("contactNumber", userNode.getProperty("contactNumber").toString());

        }
    }
    private void writeLiteUserInformation(Node userNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!userNode.hasLabel(VariantDatabase.getUserLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getUserLabel().name() + " got " + userNode.getLabels().toString());

            jg.writeNumberField("userNodeId", userNode.getId());
            if (userNode.hasProperty("admin")) jg.writeBooleanField("admin", (boolean) userNode.getProperty("admin"));
            if (userNode.hasProperty("fullName")) jg.writeStringField("fullName", userNode.getProperty("fullName").toString());

        }
    }
    private void writeSampleInformation(Node sampleNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!sampleNode.hasLabel(VariantDatabase.getSampleLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getSampleLabel().name() + " got " + sampleNode.getLabels().toString());

            jg.writeNumberField("sampleNodeId", sampleNode.getId());
            jg.writeStringField("sampleId", sampleNode.getProperty("sampleId").toString());
            jg.writeStringField("tissue", sampleNode.getProperty("tissue").toString());
        }
    }
    private void writeRunInformation(Node runInfoNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getRunInfoLabel().name() + " got " + runInfoNode.getLabels().toString());

            jg.writeNumberField("runInfoNodeId", runInfoNode.getId());

            if (runInfoNode.hasProperty("worklistId"))
                jg.writeStringField("worklistId", runInfoNode.getProperty("worklistId").toString());
            if (runInfoNode.hasProperty("seqId"))
                jg.writeStringField("seqId", runInfoNode.getProperty("seqId").toString());
            if (runInfoNode.hasProperty("assay"))
                jg.writeStringField("assay", runInfoNode.getProperty("assay").toString());
            if (runInfoNode.hasProperty("pipelineName"))
                jg.writeStringField("pipelineName", runInfoNode.getProperty("pipelineName").toString());
            if (runInfoNode.hasProperty("pipelineVersion"))
                jg.writeNumberField("pipelineVersion", (int) runInfoNode.getProperty("pipelineVersion"));
            if (runInfoNode.hasProperty("remoteVcfFilePath"))
                jg.writeStringField("remoteVcfFilePath", runInfoNode.getProperty("remoteVcfFilePath").toString());
            if (runInfoNode.hasProperty("remoteBamFilePath"))
                jg.writeStringField("remoteBamFilePath", runInfoNode.getProperty("remoteBamFilePath").toString());

            //QC
            if (runInfoNode.hasProperty("GenotypicGender"))
                jg.writeStringField("GenotypicGender", runInfoNode.getProperty("GenotypicGender").toString());
            if (runInfoNode.hasProperty("EstimatedContamination"))
                jg.writeNumberField("EstimatedContamination", (float) runInfoNode.getProperty("EstimatedContamination"));
            if (runInfoNode.hasProperty("PercentageGt30"))
                jg.writeNumberField("PercentageGt30", (float) runInfoNode.getProperty("PercentageGt30"));
            if (runInfoNode.hasProperty("DuplicationRate"))
                jg.writeNumberField("DuplicationRate", (float) runInfoNode.getProperty("DuplicationRate"));

        }
    }
    private void writeVirtualPanelInformation(Node virtualPanelNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!virtualPanelNode.hasLabel(VariantDatabase.getVirtualPanelLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getVirtualPanelLabel().name() + " got " + virtualPanelNode.getLabels().toString());

            Relationship designedByRelationship = virtualPanelNode.getSingleRelationship(VariantDatabase.getDesignedByRelationship(), Direction.OUTGOING);
            Node userNode = designedByRelationship.getEndNode();

            jg.writeNumberField("panelNodeId", virtualPanelNode.getId());

            if (virtualPanelNode.hasProperty("virtualPanelName"))
                jg.writeStringField("virtualPanelName", virtualPanelNode.getProperty("virtualPanelName").toString());
            if (designedByRelationship.hasProperty("date"))
                jg.writeNumberField("date", (long) designedByRelationship.getProperty("date"));
            if (userNode.hasLabel(VariantDatabase.getUserLabel()) && userNode.hasProperty("fullName"))
                jg.writeStringField("fullName", userNode.getProperty("fullName").toString());

        }

    }
    private void writeSymbolInformation(Node symbolNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!symbolNode.hasLabel(VariantDatabase.getSymbolLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getSymbolLabel().name() + " got " + symbolNode.getLabels().toString());

            jg.writeNumberField("symbolNodeId", symbolNode.getId());

            if (symbolNode.hasProperty("symbolId")) {
                jg.writeStringField("symbolId", symbolNode.getProperty("symbolId").toString());
            }

            jg.writeArrayFieldStart("disorders");

            for (Relationship hasAssociatedSymbolRelationship : symbolNode.getRelationships(Direction.INCOMING, VariantDatabase.getHasAssociatedSymbol())){
                Node disorderNode = hasAssociatedSymbolRelationship.getStartNode();

                jg.writeStartObject();
                jg.writeStringField("disorder", disorderNode.getProperty("disorder").toString());
                jg.writeEndObject();

            }

            jg.writeEndArray();

        }
    }
    private void writeFeatureInformation(Node featureNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!featureNode.hasLabel(VariantDatabase.getFeatureLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getFeatureLabel().name() + " got " + featureNode.getLabels().toString());

            Node lastActiveEventNode = getLastActiveUserEventNode(featureNode);

            jg.writeNumberField("featureNodeId", featureNode.getId());

            if (lastActiveEventNode != null){
                jg.writeBooleanField("preference", (boolean) lastActiveEventNode.getProperty("preference"));
            }

            if (featureNode.hasProperty("featureId"))
                jg.writeStringField("featureId", featureNode.getProperty("featureId").toString());
            if (featureNode.hasProperty("strand")) {
                if ((boolean) featureNode.getProperty("strand")) {
                    jg.writeStringField("strand", "+");
                } else {
                    jg.writeStringField("strand", "-");
                }
            }
            if (featureNode.hasProperty("ccdsId"))
                jg.writeStringField("ccdsId", featureNode.getProperty("ccdsId").toString());
            if (featureNode.hasProperty("featureType"))
                jg.writeStringField("featureType", featureNode.getProperty("featureType").toString());
            if (featureNode.hasProperty("totalExons"))
                jg.writeNumberField("totalExons", (short) featureNode.getProperty("totalExons"));
            if (featureNode.hasLabel(VariantDatabase.getCanonicalLabel())) {
                jg.writeBooleanField("canonical", true);
            } else {
                jg.writeBooleanField("canonical", false);
            }

        }
    }
    private void writeVariantInformation(Node variantNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            if (!variantNode.hasLabel(VariantDatabase.getVariantLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getVariantLabel().name() + " got " + variantNode.getLabels().toString());

            Node lastActiveEventNode = getLastActiveUserEventNode(variantNode);

            jg.writeNumberField("variantNodeId", variantNode.getId());
            jg.writeNumberField("occurrence", getGlobalVariantOccurrenceQcPass(variantNode));

            //variant class
            if (lastActiveEventNode != null){
                jg.writeNumberField("classification", (int) lastActiveEventNode.getProperty("classification"));
            }

            if (variantNode.hasLabel(VariantDatabase.getSnpLabel())) {
                jg.writeStringField("type", "Snp");
            } else if (variantNode.hasLabel(VariantDatabase.getIndelLabel())){
                jg.writeStringField("type", "Indel");
            }

            if (variantNode.hasProperty("variantId")) {
                jg.writeStringField("variantId", variantNode.getProperty("variantId").toString());
            }
            if (variantNode.hasProperty("dbSnpId")){
                jg.writeStringField("dbSnpId", variantNode.getProperty("dbSnpId").toString());
            }
            if (variantNode.hasProperty("gerp")) {
                jg.writeNumberField("gerp", (float) variantNode.getProperty("gerp"));
            }
            if (variantNode.hasProperty("phyloP")) {
                jg.writeNumberField("phyloP", (float) variantNode.getProperty("phyloP"));
            }
            if (variantNode.hasProperty("phastCons")) {
                jg.writeNumberField("phastCons", (float) variantNode.getProperty("phastCons"));
            }
            for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {
                if (variantNode.hasProperty("kGPhase3" + population.toString() + "Af")){
                    jg.writeNumberField("kGPhase3" + population.toString() + "Af", (double) Math.round( ((float) variantNode.getProperty("kGPhase3" + population.toString() + "Af") * 100) * 100d) / 100d);
                }
            }
            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                if (variantNode.hasProperty("exac" + population.toString() + "Af")){
                    jg.writeNumberField("exac" + population.toString() + "Af", (double) Math.round( ((float) variantNode.getProperty("exac" + population.toString() + "Af") * 100) * 100d) / 100d);
                }
            }
            jg.writeBooleanField("severe", variantHasSevereConsequence(variantNode));
            if (variantNode.hasProperty("clinvar")) {
                jg.writeArrayFieldStart("clinvar");

                for (int clinSig : (int[]) variantNode.getProperty("clinvar")){
                    jg.writeNumber(clinSig);
                }

                jg.writeEndArray();
            }

        }
    }
    private void writeFunctionalAnnotation(Node annotationNode, Relationship consequenceRel, Relationship biotypeRel, JsonGenerator jg) throws IOException {

        String[] domainSources = {"pfamDomain", "hmmpanther", "prosite", "superfamilyDomains"};

        try (Transaction tx = graphDb.beginTx()) {
            if (!annotationNode.hasLabel(VariantDatabase.getAnnotationLabel())) throw new WrongLabelException("Expected " + VariantDatabase.getAnnotationLabel().name() + " got " + annotationNode.getLabels().toString());

            jg.writeNumberField("annotationNodeId", annotationNode.getId());

            if (annotationNode.hasProperty("hgvsc"))
                jg.writeStringField("hgvsc", annotationNode.getProperty("hgvsc").toString());
            if (annotationNode.hasProperty("hgvsp"))
                jg.writeStringField("hgvsp", annotationNode.getProperty("hgvsp").toString());
            if (annotationNode.hasProperty("exon")) {
                jg.writeStringField("location", annotationNode.getProperty("exon").toString());
            } else if (annotationNode.hasProperty("intron")) {
                jg.writeStringField("location", annotationNode.getProperty("intron").toString());
            }
            if (annotationNode.hasProperty("sift"))
                jg.writeStringField("sift", annotationNode.getProperty("sift").toString());
            if (annotationNode.hasProperty("polyphen"))
                jg.writeStringField("polyphen", annotationNode.getProperty("polyphen").toString());
            if (annotationNode.hasProperty("codons"))
                jg.writeStringField("codons", annotationNode.getProperty("codons").toString());

            //domains
            for (String source : domainSources){

                if (annotationNode.hasProperty(source)) {
                    String[] domains = (String[]) annotationNode.getProperty(source);

                    jg.writeArrayFieldStart(source);

                    for (String domain : domains) {
                        jg.writeString(domain);
                    }

                    jg.writeEndArray();
                }

            }

            //consequence
            jg.writeStringField("consequence", getFunctionalConsequence(consequenceRel.getType().name()));

            //biotype
            jg.writeStringField("biotype", getTranscriptBiotype(biotypeRel.getType().name()));

        }

    }
    private void writeEventHistory(Node subjectNode, JsonGenerator jg) throws IOException {
        org.neo4j.graphdb.Path longestPath = null;

        jg.writeArrayFieldStart("history");

        try (Transaction tx = graphDb.beginTx()) {

            //get longest path
            for (org.neo4j.graphdb.Path path : graphDb.traversalDescription()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .uniqueness(Uniqueness.NODE_GLOBAL)
                    .relationships(VariantDatabase.getHasUserEventRelationship(), Direction.OUTGOING)
                    .traverse(subjectNode)) {
                longestPath = path;
            }

            //loop over nodes in this path
            for (Node eventNode : longestPath.nodes()) {
                if (eventNode.getId() == subjectNode.getId()) continue;

                jg.writeStartObject();

                Relationship addedByRelationship = eventNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);
                Relationship authorisedByRelationship = eventNode.getSingleRelationship(VariantDatabase.getAuthorisedByRelationship(), Direction.OUTGOING);
                Relationship rejectedByRelationship = eventNode.getSingleRelationship(VariantDatabase.getRejectedByRelationship(), Direction.OUTGOING);

                //event info
                if (eventNode.hasLabel(VariantDatabase.getVariantPathogenicityLabel())){
                    jg.writeStringField("event", "Variant classification");
                    jg.writeNumberField("value", (int) eventNode.getProperty("classification"));
                } else if (eventNode.hasLabel(VariantDatabase.getFeaturePreferenceLabel())){
                    jg.writeStringField("event", "Preferred Transcript");
                    jg.writeBooleanField("value", (boolean) eventNode.getProperty("preference"));
                }

                if (eventNode.hasProperty("evidence")) jg.writeStringField("evidence", eventNode.getProperty("evidence").toString());

                jg.writeObjectFieldStart("add");
                writeLiteUserInformation(addedByRelationship.getEndNode(), jg);
                jg.writeNumberField("date",(long) addedByRelationship.getProperty("date"));
                jg.writeEndObject();

                if (authorisedByRelationship == null && rejectedByRelationship == null){
                    jg.writeStringField("status", UserEventStatus.PENDING_AUTH.toString());
                }

                if (authorisedByRelationship != null && rejectedByRelationship == null){
                    jg.writeStringField("status", UserEventStatus.ACTIVE.toString());

                    jg.writeObjectFieldStart("auth");
                    writeLiteUserInformation(authorisedByRelationship.getEndNode(), jg);
                    jg.writeNumberField("date",(long) authorisedByRelationship.getProperty("date"));
                    jg.writeEndObject();

                }

                if (authorisedByRelationship == null && rejectedByRelationship != null){
                    jg.writeStringField("status", UserEventStatus.REJECTED.toString());

                    jg.writeObjectFieldStart("auth");
                    writeLiteUserInformation(rejectedByRelationship.getEndNode(), jg);
                    jg.writeNumberField("date",(long) rejectedByRelationship.getProperty("date"));
                    jg.writeEndObject();

                }

                jg.writeEndObject();

            }

        }

        jg.writeEndArray();
    }
}