package nhs.genetics.cardiff;

import java.io.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.security.auth.login.CredentialException;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.*;

import htsjdk.tribble.readers.LineIteratorImpl;
import htsjdk.tribble.readers.LineReader;
import htsjdk.tribble.readers.LineReaderUtil;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFFileReader;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import org.neo4j.graphdb.traversal.*;
import org.parboiled.support.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/variantdatabase")
public class VariantDatabasePlugin
{
    private static final Logger logger = LoggerFactory.getLogger(VariantDatabasePlugin.class);

    private enum UserEventStatus {
        PENDING_AUTH, ACTIVE, REJECTED
    }
    private GraphDatabaseService graphDb;
    private final ObjectMapper objectMapper;
    private Method[] methods;

    public VariantDatabasePlugin(@Context GraphDatabaseService graphDb)
    {
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

                            jg.writeFieldName("name");
                            jg.writeString(method.getAnnotation(Workflow.class).name());

                            jg.writeFieldName("description");
                            jg.writeString(method.getAnnotation(Workflow.class).description());

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

                                    if (runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())){

                                        jg.writeStartObject();

                                        writeSampleInformation(sampleNode, jg);
                                        writeRunInformation(runInfoNode, jg);

                                        jg.writeEndObject();

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


    //todo here!
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

                            if (symbolNode.hasLabel(VariantDatabase.getSymbolLabel())){

                                jg.writeStartObject();
                                writeSymbolInformation(symbolNode, jg);
                                jg.writeEndObject();

                            }

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

            HashMap<String, Object> properties = new HashMap<>();
            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.getNodeById(parameters.userNodeId);

                if (userNode.hasLabel(VariantDatabase.getUserLabel())) {

                    //make panel node
                    properties.put("virtualPanelName", parameters.virtualPanelName);
                    Node virtualPanelNode = Neo4j.addNode(graphDb, VariantDatabase.getVirtualPanelLabel(), properties);

                    //link to user
                    Relationship designedByRelationship = virtualPanelNode.createRelationshipTo(userNode, VariantDatabase.getDesignedByRelationship());
                    designedByRelationship.setProperty("date", new Date().getTime());

                    //link to genes
                    for (String gene : parameters.virtualPanelList) {

                        //match or create gene
                        Node geneNode = Neo4j.matchOrCreateUniqueNode(graphDb, VariantDatabase.getSymbolLabel(), "symbolId", gene);

                        //link to virtual panel
                        virtualPanelNode.createRelationshipTo(geneNode, VariantDatabase.getContainsSymbolRelationship());
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

                                if (getLastActiveUserEventNode(runInfoNode) == null){

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
                    throw new IllegalArgumentException("Cannot add qc result. Auth pending.");
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
    @Path("/analyses/pendingqcauth")
    @Produces(MediaType.APPLICATION_JSON)
    public Response analysesPendingQcAuth() {

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

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                    Parameters parameters = objectMapper.readValue(json, Parameters.class);
                    float maxAf;

                    try (Transaction tx = graphDb.beginTx()) {

                        Node runInfoNode = graphDb.getNodeById(parameters.runInfoNodeId);
                        Node sampleNode = runInfoNode.getSingleRelationship(VariantDatabase.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode();

                        //headers
                        //jg.writeRaw("#Variant Report v" + version + "\n");
                        //jg.writeRaw("#Created " + graphDb.getNodeById(parameters.userNodeId).getProperty("fullName") + " " + dateFormat.format(new Date()) + "\n");
                        jg.writeRaw("SampleId,WorklistId,Variant,Genotype,Quality,Occurrence,dbSNP,GERP++,PhyloP,PhastCons,");

                        //print pop freq header
                        for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {
                            jg.writeRaw("1KG_" + population.toString() + ",");
                        }
                        jg.writeRaw("Max_1KG,");
                        for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                            jg.writeRaw("ExAC_" + population.toString() + ",");
                        }
                        jg.writeRaw("Max_ExAC,");

                        jg.writeRaw("Gene,Transcript,TranscriptType,TranscriptBiotype,CanonicalTranscript,PreferredTranscript,Consequence,Severe,OMIM,InternalClass,HGVSc,HGVSp,Location,SIFT,PolyPhen,Codons\n");

                        //loop over variants node ids
                        for (long variantNodeId : parameters.variantNodeIds){
                            Node variantNode = graphDb.getNodeById(variantNodeId);

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
                                                            if (sampleNode.hasProperty("sampleId")) jg.writeRaw(sampleNode.getProperty("sampleId").toString() + ","); else jg.writeRaw(",");
                                                            if (runInfoNode.hasProperty("worklistId")) jg.writeRaw(runInfoNode.getProperty("worklistId").toString() + ","); else jg.writeRaw(",");

                                                            //variant
                                                            if (variantNode.hasProperty("variantId")) jg.writeRaw(variantNode.getProperty("variantId").toString() + ","); else jg.writeRaw(",");
                                                            jg.writeRaw(getVariantInheritance(relationship.getType().name()) + ",");
                                                            if (relationship.hasProperty("quality")) jg.writeRaw(relationship.getProperty("quality").toString() + ","); else jg.writeRaw(",");
                                                            jg.writeRaw(getGlobalVariantOccurrence(variantNode) + ",");
                                                            if (variantNode.hasProperty("dbSnpId")) jg.writeRaw(variantNode.getProperty("dbSnpId").toString() + ","); else jg.writeRaw(",");
                                                            if (variantNode.hasProperty("gerp")) jg.writeRaw(variantNode.getProperty("gerp").toString() + ","); else jg.writeRaw(",");
                                                            if (variantNode.hasProperty("phyloP")) jg.writeRaw(variantNode.getProperty("phyloP").toString() + ","); else jg.writeRaw(",");
                                                            if (variantNode.hasProperty("phastCons")) jg.writeRaw(variantNode.getProperty("phastCons").toString() + ","); else jg.writeRaw(",");

                                                            //1kg
                                                            maxAf = -1f;
                                                            for (VariantDatabase.kGPhase3Population population : VariantDatabase.kGPhase3Population.values()) {
                                                                if (variantNode.hasProperty("kGPhase3" + population.toString() + "Af")){
                                                                    jg.writeRaw(variantNode.getProperty("kGPhase3" + population.toString() + "Af").toString() + ",");

                                                                    float tmp = (float) variantNode.getProperty("kGPhase3" + population.toString() + "Af");
                                                                    if (tmp > maxAf) maxAf = tmp;

                                                                } else {
                                                                    jg.writeRaw(",");
                                                                }
                                                            }
                                                            if (maxAf != -1f){
                                                                jg.writeRaw(Float.toString(maxAf) + ",");
                                                            } else {
                                                                jg.writeRaw("0,");
                                                            }

                                                            //ExAC
                                                            maxAf = -1f;
                                                            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                                                                if (variantNode.hasProperty("exac" + population.toString() + "Af")){
                                                                    jg.writeRaw(variantNode.getProperty("exac" + population.toString() + "Af").toString() + ",");

                                                                    float tmp = (float) variantNode.getProperty("exac" + population.toString() + "Af");
                                                                    if (tmp > maxAf) maxAf = tmp;

                                                                } else {
                                                                    jg.writeRaw(",");
                                                                }
                                                            }
                                                            if (maxAf != -1f){
                                                                jg.writeRaw(Float.toString(maxAf) + ",");
                                                            } else {
                                                                jg.writeRaw("0,");
                                                            }

                                                            //gene & transcript
                                                            if (symbolNode.hasProperty("symbolId")) jg.writeRaw(symbolNode.getProperty("symbolId").toString() + ","); else jg.writeRaw(",");
                                                            if (featureNode.hasProperty("featureId")) jg.writeRaw(featureNode.getProperty("featureId").toString() + ","); else jg.writeRaw(",");
                                                            if (featureNode.hasProperty("featureType")) jg.writeRaw(featureNode.getProperty("featureType").toString() + ","); else jg.writeRaw(",");
                                                            jg.writeRaw(getTranscriptBiotype(biotypeRel.getType().name()) + ",");

                                                            //transcript choice
                                                            if (featureNode.hasLabel(VariantDatabase.getCanonicalLabel())) {
                                                                jg.writeRaw("TRUE,");
                                                            } else {
                                                                jg.writeRaw("FALSE,");
                                                            }

                                                            //internal choice
                                                            Node lastActiveEventFeaturePrefNode =  getLastActiveUserEventNode(featureNode);
                                                            if (lastActiveEventFeaturePrefNode != null){
                                                                jg.writeRaw(lastActiveEventFeaturePrefNode.getProperty("preference").toString() + ",");
                                                            } else {
                                                                jg.writeRaw(",");
                                                            }

                                                            //functional annotations
                                                            String consequence = getFunctionalConsequence(consequenceRel.getType().name());
                                                            jg.writeRaw(consequence + ",");
                                                            jg.writeRaw(isConsequenceSevere(consequence) + ",");

                                                            //omim
                                                            for (Relationship hasAssociatedSymbol : symbolNode.getRelationships(Direction.INCOMING, VariantDatabase.getHasAssociatedSymbol())){
                                                                Node disorderNode = hasAssociatedSymbol.getStartNode();
                                                                jg.writeRaw(disorderNode.getProperty("disorder").toString() + ";");
                                                            }
                                                            jg.writeRaw(",");

                                                            if (annotationNode.hasProperty("hgvsc")) jg.writeRaw(annotationNode.getProperty("hgvsc").toString() + ","); else jg.writeRaw(",");
                                                            if (annotationNode.hasProperty("hgvsp")) jg.writeRaw(annotationNode.getProperty("hgvsp").toString() + ","); else jg.writeRaw(",");

                                                            if (annotationNode.hasProperty("exon")) {
                                                                jg.writeRaw(annotationNode.getProperty("exon").toString() + ",");
                                                            } else if (annotationNode.hasProperty("intron")) {
                                                                jg.writeRaw(annotationNode.getProperty("intron").toString() + ",");
                                                            } else {
                                                                jg.writeRaw(",");
                                                            }

                                                            if (annotationNode.hasProperty("sift")) jg.writeRaw(annotationNode.getProperty("sift").toString() + ","); else jg.writeRaw(",");
                                                            if (annotationNode.hasProperty("polyphen")) jg.writeRaw(annotationNode.getProperty("polyphen").toString() + ","); else jg.writeRaw(",");
                                                            if (annotationNode.hasProperty("codons")) jg.writeRaw(annotationNode.getProperty("codons").toString() + "\n"); else jg.writeRaw("\n");

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

    //todo
    @GET
    @Path("/omim/add")
    @Produces(MediaType.APPLICATION_JSON)
    public Response omimAdd() {

        Neo4j.createConstraint(graphDb, VariantDatabase.getDisorderLabel(), "disorder");

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

    //todo
    @GET
    @Path("/clinvar/add")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clinvarAdd() {

        try {

            //todo parse VCF and import classifications
            // BufferedReader in = new BufferedReader(new InputStreamReader(gzipInputStream));

            //read VCF from clinvar, decompress and import on the fly
            VCFCodec codec = new VCFCodec();
            URL clinvarFtp = new URL("ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz");
            GZIPInputStream gzipInputStream = new GZIPInputStream(clinvarFtp.openStream());
            LineReader lineReader = LineReaderUtil.fromBufferedStream(gzipInputStream);
            LineIteratorImpl lineIteratorImpl = new LineIteratorImpl(lineReader);
            codec.readActualHeader(lineIteratorImpl);

            while(lineIteratorImpl.hasNext())
            {
                VariantContext variantContext = codec.decode(lineIteratorImpl.next());

                //loop over alternate alleles
                for (Allele allele : variantContext.getAlternateAlleles()){
                    for (String clinSig : variantContext.getAttribute("CLNSIG").toString().split("\\|")){
                        logger.debug(variantContext.getContig() + " " + variantContext.getStart() + " " + variantContext.getReference().getBaseString() + " " + allele.getBaseString() + " " + Integer.parseInt(clinSig));
                    }
                }

            }

            lineReader.close();
            gzipInputStream.close();

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
            default:
                return null;
        }
    }
    private int getGlobalVariantOccurrence(Node variantNode){
        int seen = 0;

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship relationship : variantNode.getRelationships(Direction.INCOMING)) {
                if (relationship.isType(VariantDatabase.getHasHetVariantRelationship()) && relationship.getStartNode().hasLabel(VariantDatabase.getRunInfoLabel())) {
                    seen += 1;
                } else if (relationship.isType(VariantDatabase.getHasHomVariantRelationship()) && relationship.getStartNode().hasLabel(VariantDatabase.getRunInfoLabel())) {
                    seen += 2;
                }
            }
        }

        return seen;
    }
    private int getAssayVariantOccurrence(Node variantNode, String assay){
        int seen = 0;

        if (assay == null)
            return 0;

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship relationship : variantNode.getRelationships(Direction.INCOMING)) {

                if (relationship.isType(VariantDatabase.getHasHetVariantRelationship())) {
                    Node runInfoNode = relationship.getStartNode();

                    if (runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())) {
                        if (runInfoNode.hasProperty("assay")) {
                            if (runInfoNode.getProperty("assay").toString().equals(assay)) {
                                seen += 1;
                            }
                        }
                    }

                } else if (relationship.isType(VariantDatabase.getHasHomVariantRelationship())) {
                    Node runInfoNode = relationship.getStartNode();

                    if (runInfoNode.hasLabel(VariantDatabase.getRunInfoLabel())){
                        if (runInfoNode.hasProperty("assay")){
                            if (runInfoNode.getProperty("assay").toString().equals(assay)){
                                seen += 2;
                            }
                        }
                    }

                }

            }
        }

        return seen;
    }
    private int getPanelOccurrence(String assay){
        int tested = 0;

        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Node> runInfoNodes = graphDb.findNodes(VariantDatabase.getRunInfoLabel())) {

                while (runInfoNodes.hasNext()) {
                    Node runInfoNode = runInfoNodes.next();

                    if (runInfoNode.hasProperty("assay")){
                        if (runInfoNode.getProperty("assay").toString().equals(assay)){
                            tested++;
                        }
                    }

                }

                runInfoNodes.close();
            }
        }

        return tested;
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

            jg.writeNumberField("userNodeId", userNode.getId());
            if (userNode.hasProperty("admin")) jg.writeBooleanField("admin", (boolean) userNode.getProperty("admin"));
            if (userNode.hasProperty("fullName")) jg.writeStringField("fullName", userNode.getProperty("fullName").toString());

        }
    }
    private void writeSampleInformation(Node sampleNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {
            jg.writeNumberField("sampleNodeId", sampleNode.getId());
            jg.writeStringField("sampleId", sampleNode.getProperty("sampleId").toString());
            jg.writeStringField("tissue", sampleNode.getProperty("tissue").toString());
        }
    }
    private void writeRunInformation(Node runInfoNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

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

        }
    }
    private void writeVirtualPanelInformation(Node virtualPanelNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

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
        Node lastActiveEventNode = getLastActiveUserEventNode(featureNode);
        try (Transaction tx = graphDb.beginTx()) {

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
        Node lastActiveEventNode = getLastActiveUserEventNode(variantNode);

        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("variantNodeId", variantNode.getId());
            jg.writeNumberField("occurrence", getGlobalVariantOccurrence(variantNode));

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

        }
    }
    private void writeFunctionalAnnotation(Node annotationNode, Relationship consequenceRel, Relationship biotypeRel, JsonGenerator jg) throws IOException {

        String[] domainSources = {"pfamDomain", "hmmpanther", "prosite", "superfamilyDomains"};

        try (Transaction tx = graphDb.beginTx()) {

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