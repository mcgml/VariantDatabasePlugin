package nhs.genetics.cardiff;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.NotFoundException;

//todo mask interal frequency for poorly sampled regions

@Path("/variantdatabase")
public class VariantDatabasePlugin
{
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

    @GET
    @Path("/workflows")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflows() {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                jg.writeStartArray();

                //print method names with get or post annotations
                for (Method method : methods){
                    if (method.isAnnotationPresent(Workflow.class) && !method.isAnnotationPresent(Deprecated.class)){

                        jg.writeStartObject();

                        jg.writeFieldName("Name");
                        jg.writeString(method.getAnnotation(Workflow.class).name());

                        jg.writeFieldName("Description");
                        jg.writeString(method.getAnnotation(Workflow.class).description());

                        jg.writeFieldName("Path");
                        jg.writeString(method.getAnnotation(Path.class).value());

                        jg.writeEndObject();

                    }
                }

                jg.writeEndArray();

                jg.flush();
                jg.close();
            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/analyses")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAnalyses() {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> sampleNodes = graphDb.findNodes(Neo4j.getSampleLabel())) {

                        while (sampleNodes.hasNext()) {
                            Node sampleNode = sampleNodes.next();

                            for (Relationship relationship : sampleNode.getRelationships(Direction.OUTGOING, Neo4j.getHasAnalysisRelationship())) {
                                Node runInfoNode = relationship.getEndNode();

                                if (runInfoNode.hasLabel(Neo4j.getRunInfoLabel())){

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
    }

    @GET
    @Path("/panels")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVirtualPanels() {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> virtualPanelNodes = graphDb.findNodes(Neo4j.getVirtualPanelLabel())) {

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
    }

    @POST
    @Path("/variantinformation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVariantInformation(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                Parameters parameters = objectMapper.readValue(json, Parameters.class);
                Node variantNode = null;

                jg.writeStartObject();

                try (Transaction tx = graphDb.beginTx()) {

                    //find variant
                    try (ResourceIterator<Node> variants = graphDb.findNodes(Neo4j.getVariantLabel(), "VariantId", parameters.VariantId)) {
                        while (variants.hasNext()) {
                            variantNode = variants.next();
                        }
                        variants.close();
                    }

                    int variantOccurrence = getVariantOccurrence(variantNode);

                    writeVariantInformation(variantNode, jg);
                    writeVariantHistory(variantNode, jg);

                    jg.writeNumberField("Occurrence", variantOccurrence);
                }

                jg.writeEndObject();

                jg.flush();
                jg.close();
            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

    }

    @POST
    @Path("/featureinformation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFeatureInformation(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                Parameters parameters = objectMapper.readValue(json, Parameters.class);
                Node featureNode = null;

                jg.writeStartObject();

                try (Transaction tx = graphDb.beginTx()) {

                    try (ResourceIterator<Node> features = graphDb.findNodes(Neo4j.getFeatureLabel(), "FeatureId", parameters.FeatureId)) {
                        while (features.hasNext()) {
                            featureNode = features.next();
                        }
                        features.close();
                    }

                    writeFeatureInformation(featureNode, jg);
                    writeFeatureHistory(featureNode, jg);

                }

                jg.writeEndObject();

                jg.flush();
                jg.close();
            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

    }

    @POST
    @Path("/variantobservations")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVariantObservations(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    Node variantNode = graphDb.getNodeById(parameters.VariantNodeId);

                    for (Relationship inheritanceRel : variantNode.getRelationships(Direction.INCOMING)) {
                        Node runInfoNode = inheritanceRel.getStartNode();

                        if (runInfoNode.hasLabel(Neo4j.getRunInfoLabel())){
                            Node sampleNode = runInfoNode.getSingleRelationship(Neo4j.getHasAnalysisRelationship(), Direction.INCOMING).getStartNode();

                            if (sampleNode.hasLabel(Neo4j.getSampleLabel())){
                                jg.writeStartObject();

                                writeSampleInformation(sampleNode, jg);
                                jg.writeStringField("Inheritance", getVariantInheritance(inheritanceRel.getType().name()));
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
    }

    @POST
    @Path("/functionalannotation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFunctionalAnnotation(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    Node variantNode = graphDb.getNodeById(parameters.VariantNodeId);

                    for (Relationship consequenceRel : variantNode.getRelationships(Direction.OUTGOING)) {
                        Node annotationNode = consequenceRel.getEndNode();

                        if (annotationNode.hasLabel(Neo4j.getAnnotationLabel())){

                            for (Relationship inFeatureRel : annotationNode.getRelationships(Direction.OUTGOING, Neo4j.getHasInFeatureRelationship())) {
                                Node featureNode = inFeatureRel.getEndNode();

                                if (featureNode.hasLabel(Neo4j.getFeatureLabel())){

                                    for (Relationship biotypeRel : featureNode.getRelationships(Direction.INCOMING)) {
                                        Node symbolNode = biotypeRel.getStartNode();

                                        if (symbolNode.hasLabel(Neo4j.getSymbolLabel())){

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
    }

    @POST
    @Path("/addvariantpathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addVariantPathogenicity(final String json) throws IOException {
        Parameters parameters = objectMapper.readValue(json, Parameters.class);

        try (Transaction tx = graphDb.beginTx()) {

            Node variantNode = graphDb.getNodeById(parameters.VariantNodeId);
            Node userNode = graphDb.getNodeById(parameters.UserNodeId);

            if (variantNode.hasLabel(Neo4j.getVariantLabel()) && userNode.hasLabel(Neo4j.getUserLabel())){

                if (isClassificationPendingApproval(variantNode)){
                    throw new UnsupportedOperationException("Variant has pending classification.");
                }

                Date date = new Date();

                //create pathogenicity
                Node pathogenicityNode = graphDb.createNode(Neo4j.getPathogenicityLabel());
                variantNode.createRelationshipTo(pathogenicityNode, Neo4j.getHasPathogenicityRelationship());

                Relationship addedByRelationship = pathogenicityNode.createRelationshipTo(userNode, Neo4j.getAddedByRelationship());

                //add properties
                addedByRelationship.setProperty("Classification", parameters.Classification);
                addedByRelationship.setProperty("Date", date.getTime());
                if (parameters.Evidence != null) addedByRelationship.setProperty("Evidence", parameters.Evidence);

            }

            tx.success();
        } catch (UnsupportedOperationException e){
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

        return Response
                .status(Response.Status.OK)
                .build();
    }

    @POST
    @Path("/removevariantpathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeVariantPathogenicity(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                try (Transaction tx = graphDb.beginTx()) {

                    Node pathogenicityNode = graphDb.getNodeById(parameters.PathogenicityNodeId);
                    Node userNode = graphDb.getNodeById(parameters.UserNodeId);

                    if (pathogenicityNode.hasLabel(Neo4j.getPathogenicityLabel()) && userNode.hasLabel(Neo4j.getUserLabel())){

                        Date date = new Date();

                        //remove pathogenicity
                        Relationship removedByRelationship = pathogenicityNode.createRelationshipTo(userNode, Neo4j.getRemovedByRelationship());

                        //add properties
                        removedByRelationship.setProperty("Date", date.getTime());
                        if (parameters.Evidence != null) removedByRelationship.setProperty("Evidence", parameters.Evidence);

                    }

                    tx.success();
                }

            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/authorisevariantpathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response authoriseVariantPathogenicity(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                try (Transaction tx = graphDb.beginTx()) {

                    Node pathogenicityNode = graphDb.getNodeById(parameters.PathogenicityNodeId);
                    Node userNode = graphDb.getNodeById(parameters.UserNodeId);

                    if (pathogenicityNode.hasLabel(Neo4j.getPathogenicityLabel()) && userNode.hasLabel(Neo4j.getUserLabel())){

                        Date date = new Date();

                        if (parameters.AddorRemove){
                            Relationship relationship = pathogenicityNode.createRelationshipTo(userNode, Neo4j.getAddAuthorisedByRelationship());
                            relationship.setProperty("Date", date.getTime());
                        } else {
                            Relationship relationship = pathogenicityNode.createRelationshipTo(userNode, Neo4j.getRemoveAuthorisedByRelationship());
                            relationship.setProperty("Date", date.getTime());
                        }

                    }

                    tx.success();
                }

            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/getnewpathogenicitiesforauthorisation")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNewPathogenicitiesForAuthorisation(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> pathogenicityNodeIterator = graphDb.findNodes(Neo4j.getPathogenicityLabel())) {

                        while (pathogenicityNodeIterator.hasNext()) {
                            Node pathogenicityNode = pathogenicityNodeIterator.next();

                            Relationship addedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddedByRelationship(), Direction.OUTGOING);
                            Relationship removedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemovedByRelationship(), Direction.OUTGOING);
                            Relationship addAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddAuthorisedByRelationship(), Direction.OUTGOING);
                            Relationship removeAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);
                            Relationship hasPathogenicityRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getHasPathogenicityRelationship(), Direction.INCOMING);

                            if (addedByRelationship != null && removedByRelationship == null && addAuthorisedByRelationship == null && removeAuthorisedByRelationship == null){
                                Node userNode = addedByRelationship.getEndNode();
                                Node variantNode = hasPathogenicityRelationship.getStartNode();

                                if (userNode.hasLabel(Neo4j.getUserLabel()) && variantNode.hasLabel(Neo4j.getVariantLabel())){

                                    jg.writeStartObject();

                                    if (variantNode.hasProperty("VariantId")) jg.writeStringField("VariantId", variantNode.getProperty("VariantId").toString());
                                    jg.writeNumberField("PathogenicityNodeId", pathogenicityNode.getId());
                                    if (addedByRelationship.hasProperty("Classification")) jg.writeNumberField("Classification", (int) addedByRelationship.getProperty("Classification"));
                                    jg.writeBooleanField("AddAction", true);
                                    if (userNode.hasProperty("UserId")) jg.writeStringField("UserId", userNode.getProperty("UserId").toString());
                                    if (addedByRelationship.hasProperty("Date")) jg.writeNumberField("Date", (long) addedByRelationship.getProperty("Date"));
                                    if (addedByRelationship.hasProperty("Evidence")) jg.writeStringField("Evidence", addedByRelationship.getProperty("Evidence").toString());

                                    jg.writeEndObject();

                                }

                            } else if (addedByRelationship != null && removedByRelationship != null && removeAuthorisedByRelationship == null){
                                Node userNode = removedByRelationship.getEndNode();
                                Node variantNode = hasPathogenicityRelationship.getStartNode();

                                if (userNode.hasLabel(Neo4j.getUserLabel()) && variantNode.hasLabel(Neo4j.getVariantLabel())){

                                    jg.writeStartObject();

                                    if (variantNode.hasProperty("VariantId")) jg.writeStringField("VariantId", variantNode.getProperty("VariantId").toString());
                                    jg.writeNumberField("PathogenicityNodeId", pathogenicityNode.getId());
                                    if (addedByRelationship.hasProperty("Classification")) jg.writeNumberField("Classification", (int) addedByRelationship.getProperty("Classification"));
                                    jg.writeBooleanField("AddAction", false);
                                    if (userNode.hasProperty("UserId")) jg.writeStringField("UserId", userNode.getProperty("UserId").toString());
                                    if (removedByRelationship.hasProperty("Date")) jg.writeNumberField("Date", (long) removedByRelationship.getProperty("Date"));
                                    if (removedByRelationship.hasProperty("Evidence")) jg.writeStringField("Evidence", removedByRelationship.getProperty("Evidence").toString());

                                    jg.writeEndObject();

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
    }

    @POST
    @Path("/addvirtualpanel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addVirtualPanel(final String json) throws IOException {

        Date date = new Date();
        HashMap<String, Object> properties = new HashMap<>();
        Parameters parameters = objectMapper.readValue(json, Parameters.class);

        try (Transaction tx = graphDb.beginTx()) {
            Node userNode = graphDb.getNodeById(parameters.UserNodeId);

            if (userNode.hasLabel(Neo4j.getUserLabel())){

                //make panel node
                properties.put("VirtualPanelName", parameters.VirtualPanelName);
                Node virtualPanelNode = Neo4j.addNode(graphDb, Neo4j.getVirtualPanelLabel(), properties);
                properties.clear();

                //link to user
                Relationship designedByRelationship = virtualPanelNode.createRelationshipTo(userNode, Neo4j.getHasDesignedBy());
                designedByRelationship.setProperty("Date", date.getTime());

                //link to genes
                for (String gene : parameters.VirtualPanelList){

                    //match or create gene
                    Node geneNode = Neo4j.matchOrCreateUniqueNode(graphDb, Neo4j.getSymbolLabel(), "SymbolId", gene);

                    //link to virtual panel
                    virtualPanelNode.createRelationshipTo(geneNode, Neo4j.getHasContainsSymbolRelationship());
                }


            }

            tx.success();
        } catch (UnsupportedOperationException e){
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

        return Response
                .status(Response.Status.OK)
                .build();
    }

    @POST
    @Path("/getvirtualpanelgenes")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVirtualPanelGenes(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    Node panelNode = graphDb.getNodeById(parameters.PanelNodeId);

                    for (Relationship containsSymbol : panelNode.getRelationships(Direction.OUTGOING, Neo4j.getHasContainsSymbol())){
                        Node symbolNode = containsSymbol.getEndNode();

                        if (symbolNode.hasLabel(Neo4j.getSymbolLabel())){
                            jg.writeString(symbolNode.getProperty("SymbolId").toString());
                        }

                    }
                }

                jg.writeEndArray();

                jg.flush();
                jg.close();
            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/autosomaldominantworkflowv1")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Workflow(name = "Autosomal Dominant v1", description = "A workflow to stratify homozygous, sex-linked, and frequent variants (>0.01 MAF).")
    public Response autosomalDominantWorkflowv1(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                //filters
                int benignVariants = 0, likelyBenignVariants = 0, unclassifiedVariants = 0, likelyPathogenicVariants = 0, pathogenicVariants = 0,
                        notHeterozygousVariants = 0, notAutosomalVariants = 0, not1KGRareVariants = 0, notExACRareVariants = 0, otherVariants = 0,
                        classification, panelOccurrence = getPanelOccurrence("");
                HashSet<String> reportedVariants = new HashSet<>();

                jg.writeStartObject();

                jg.writeFieldName("Variants");
                jg.writeStartArray();

                Parameters parameters = objectMapper.readValue(json, Parameters.class);

                try (Transaction tx = graphDb.beginTx()) {
                    Node runInfoNode = graphDb.getNodeById(parameters.RunInfoNodeId);

                    for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                        Node variantNode = inheritanceRel.getEndNode();

                        if (variantNode.hasLabel(Neo4j.getVariantLabel())) {

                            for (Relationship inSymbolRel : variantNode.getRelationships(Direction.OUTGOING, Neo4j.getHasInSymbolRelationship())) {
                                Node symbolNode = inSymbolRel.getEndNode();

                                if (symbolNode.hasLabel(Neo4j.getSymbolLabel())) {

                                    for (Relationship containsSymbol : symbolNode.getRelationships(Direction.INCOMING, Neo4j.getHasContainsSymbol())) {
                                        Node virtualPanelNode = containsSymbol.getStartNode();

                                        //check if variant belongs to the virtual panel
                                        if (virtualPanelNode.getId() == parameters.PanelNodeId) {
                                            String variant = variantNode.getProperty("VariantId").toString();

                                            //remove variants already reported; caused by multiple gene association
                                            if (reportedVariants.contains(variant)) {
                                                continue;
                                            } else {
                                                reportedVariants.add(variant);
                                            }

                                            jg.writeStartObject();

                                            int variantOccurrence = getVariantOccurrence(variantNode);

                                            writeVariantInformation(variantNode, jg);
                                            jg.writeStringField("Inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                                            jg.writeNumberField("Occurrence", variantOccurrence);
                                            jg.writeNumberField("Frequency", getVariantInternalFrequency(panelOccurrence, variantOccurrence));

                                            //stratify variants
                                            classification = getVariantPathogenicityClassification(variantNode);
                                            if (classification != -1) { //has classification

                                                if (classification == 1) {
                                                    jg.writeNumberField("Filter", 0);
                                                    benignVariants++;
                                                } else if (classification == 2) {
                                                    jg.writeNumberField("Filter", 1);
                                                    likelyBenignVariants++;
                                                } else if (classification == 3) {
                                                    jg.writeNumberField("Filter", 2);
                                                    unclassifiedVariants++;
                                                } else if (classification == 4) {
                                                    jg.writeNumberField("Filter", 3);
                                                    likelyPathogenicVariants++;
                                                } else if (classification == 5) {
                                                    jg.writeNumberField("Filter", 4);
                                                    pathogenicVariants++;
                                                }

                                            } else if (inheritanceRel.isType(Neo4j.getHasHomVariantRelationship())) {
                                                jg.writeNumberField("Filter", 5);
                                                notHeterozygousVariants++;
                                            } else if (!variantNode.hasLabel(Neo4j.getAutoChromosomeLabel())) {
                                                jg.writeNumberField("Filter", 6);
                                                notAutosomalVariants++;
                                            } else if (!isExACRareVariant(variantNode, 0.01)) {
                                                jg.writeNumberField("Filter", 7);
                                                notExACRareVariants++;
                                            } else if (!is1KGRareVariant(variantNode, 0.01)) {
                                                jg.writeNumberField("Filter", 8);
                                                not1KGRareVariants++;
                                            } else {
                                                jg.writeNumberField("Filter", 9);
                                                otherVariants++;
                                            }

                                            jg.writeEndObject();
                                        }

                                    }
                                }

                            }
                        }
                    }

                }

                jg.writeEndArray();

                //write filters
                jg.writeFieldName("Filters");
                jg.writeStartArray();

                jg.writeStartObject();
                jg.writeStringField("key", "Benign");
                jg.writeNumberField("y", benignVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "Likely Benign");
                jg.writeNumberField("y", likelyBenignVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "Unclassified");
                jg.writeNumberField("y", unclassifiedVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "Likely Pathogenic");
                jg.writeNumberField("y", likelyPathogenicVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "Pathogenic");
                jg.writeNumberField("y", pathogenicVariants);
                jg.writeEndObject();

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
                jg.writeNumberField("y", otherVariants);
                jg.writeEndObject();

                jg.writeEndArray();

                jg.writeEndObject();

                jg.flush();
                jg.close();

            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    //helper functions
    private int getVariantOccurrence(Node variantNode){

        int seen = 0;

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship relationship : variantNode.getRelationships(Direction.INCOMING)) {
                if (relationship.isType(Neo4j.getHasHetVariantRelationship()) && relationship.getStartNode().hasLabel(Neo4j.getRunInfoLabel())) {
                    seen += 1;
                } else if (relationship.isType(Neo4j.getHasHomVariantRelationship()) && relationship.getStartNode().hasLabel(Neo4j.getRunInfoLabel())) {
                    seen += 2;
                }
            }
        }

        return seen;
    }
    private int getPanelOccurrence(String panelName){ //todo split out by panel name (i.e. TruSight Cancer)
        int tested = 0;

        try (Transaction tx = graphDb.beginTx()) {
            try (ResourceIterator<Node> analyses = graphDb.findNodes(Neo4j.getRunInfoLabel())) {

                while (analyses.hasNext()) {
                    Node analysisNode = analyses.next();

                    //try {

                    //if (analysisNode.getProperties("PanelName").toString().equals(panelName)){
                    tested++;
                    //}

                    //} catch (NotFoundException e){

                    //}

                }

                analyses.close();
            }
        }

        return tested;
    }
    private boolean is1KGRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (Neo4j.onekGPopulation population : Neo4j.onekGPopulation.values()) {

                try {

                    Object frequency = variantNode.getProperty(population.toString());

                    if ((float) frequency > maxAlleleFrequency){
                        return false;
                    }

                } catch (NotFoundException e) {

                }

            }

        }

        return true;
    }
    private boolean isExACRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (Neo4j.exACPopulation population : Neo4j.exACPopulation.values()) {

                try {

                    Object frequency = variantNode.getProperty(population.toString());

                    if ((float) frequency > maxAlleleFrequency){
                        return false;
                    }

                } catch (NotFoundException e) {

                }

            }

        }

        return true;
    }
    private int getVariantPathogenicityClassification(Node variantNode){
        int classification = -1;

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship pathogenicityRelationship : variantNode.getRelationships(Direction.OUTGOING, Neo4j.getHasPathogenicityRelationship())) {
                Node pathogenicityNode = pathogenicityRelationship.getEndNode();

                if (pathogenicityNode.hasLabel(Neo4j.getPathogenicityLabel())) {

                    Relationship addedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddedByRelationship(), Direction.OUTGOING);
                    Relationship addAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddAuthorisedByRelationship(), Direction.OUTGOING);
                    Relationship removeAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);

                    if (addAuthorisedByRelationship != null) {
                        Node userNode = addAuthorisedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            if (addedByRelationship.hasProperty("Classification")){
                                classification = (int) addedByRelationship.getProperty("Classification");
                            }
                        }
                    }

                    if (removeAuthorisedByRelationship != null) {
                        Node userNode = removeAuthorisedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            classification = -1;
                        }

                    } else {
                        return classification;
                    }

                }

            }

        }

        return classification;
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

    //write functions
    private void writeSampleInformation(Node sampleNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("SampleNodeId", sampleNode.getId());

            jg.writeStringField("SampleId", sampleNode.getProperty("SampleId").toString());

        }
    }
    private void writeRunInformation(Node runInfoNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("RunInfoNodeId", runInfoNode.getId());

            if (runInfoNode.hasProperty("WorklistId"))
                jg.writeStringField("WorklistId", runInfoNode.getProperty("WorklistId").toString());
            if (runInfoNode.hasProperty("RunId"))
                jg.writeStringField("RunId", runInfoNode.getProperty("RunId").toString());
            if (runInfoNode.hasProperty("SupplierPanelName"))
                jg.writeStringField("SupplierPanelName", runInfoNode.getProperty("SupplierPanelName").toString());
            if (runInfoNode.hasProperty("PipelineName"))
                jg.writeStringField("PipelineName", runInfoNode.getProperty("PipelineName").toString());
            if (runInfoNode.hasProperty("PipelineVersion"))
                jg.writeNumberField("PipelineVersion", (int) runInfoNode.getProperty("PipelineVersion"));
            if (runInfoNode.hasProperty("RemoteVcfFilePath"))
                jg.writeStringField("RemoteVcfFilePath", runInfoNode.getProperty("RemoteVcfFilePath").toString());
            if (runInfoNode.hasProperty("RemoteBamFilePath"))
                jg.writeStringField("RemoteBamFilePath", runInfoNode.getProperty("RemoteBamFilePath").toString());

        }
    }
    private void writeVirtualPanelInformation(Node virtualPanelNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            Relationship designedByRelationship = virtualPanelNode.getSingleRelationship(Neo4j.getHasDesignedBy(), Direction.OUTGOING);
            Node userNode = designedByRelationship.getEndNode();

            jg.writeNumberField("PanelNodeId", virtualPanelNode.getId());

            if (virtualPanelNode.hasProperty("VirtualPanelName"))
                jg.writeStringField("VirtualPanelName", virtualPanelNode.getProperty("VirtualPanelName").toString());
            if (designedByRelationship.hasProperty("Date"))
                jg.writeNumberField("Date", (long) designedByRelationship.getProperty("Date"));
            if (userNode.hasLabel(Neo4j.getUserLabel()) && userNode.hasProperty("UserId"))
                jg.writeStringField("UserId", userNode.getProperty("UserId").toString());

        }

    }
    private void writeSymbolInformation(Node symbolNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("SymbolNodeId", symbolNode.getId());

            if (symbolNode.hasProperty("SymbolId"))
                jg.writeStringField("SymbolId", symbolNode.getProperty("SymbolId").toString());
            if (symbolNode.hasProperty("GeneId"))
                jg.writeStringField("GeneId", symbolNode.getProperty("GeneId").toString());

        }
    }
    private void writeFeatureInformation(Node featureNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("FeatureNodeId", featureNode.getId());

            if (featureNode.hasProperty("FeatureId"))
                jg.writeStringField("FeatureId", featureNode.getProperty("FeatureId").toString());
            if (featureNode.hasProperty("Strand")) {
                if ((boolean) featureNode.getProperty("Strand")) {
                    jg.writeStringField("Strand", "+");
                } else {
                    jg.writeStringField("Strand", "-");
                }
            }
            if (featureNode.hasProperty("CCDSId"))
                jg.writeStringField("CCDSId", featureNode.getProperty("CCDSId").toString());
            if (featureNode.hasProperty("FeatureType"))
                jg.writeStringField("FeatureType", featureNode.getProperty("FeatureType").toString());
            if (featureNode.hasProperty("TotalExons"))
                jg.writeNumberField("TotalExons", (short) featureNode.getProperty("TotalExons"));
            if (featureNode.hasLabel(Neo4j.getCanonicalLabel())) {
                jg.writeBooleanField("Canonical", true);
            } else {
                jg.writeBooleanField("Canonical", false);
            }

        }
    }
    private void writeVariantInformation(Node variantNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("VariantNodeId", variantNode.getId());

            if (variantNode.hasProperty("VariantId")) {
                jg.writeStringField("VariantId", variantNode.getProperty("VariantId").toString());
            }
            if (variantNode.hasProperty("dbSNPId")){
                jg.writeStringField("dbSNPId", variantNode.getProperty("dbSNPId").toString());
            }
            if (variantNode.hasProperty("GERP")) {
                jg.writeNumberField("GERP", (float) variantNode.getProperty("GERP"));
            }
            if (variantNode.hasProperty("phyloP")) {
                jg.writeNumberField("phyloP", (float) variantNode.getProperty("phyloP"));
            }
            if (variantNode.hasProperty("phastCons")) {
                jg.writeNumberField("phastCons", (float) variantNode.getProperty("phastCons"));
            }
            for (Neo4j.onekGPopulation population : Neo4j.onekGPopulation.values()) {
                if (variantNode.hasProperty(population.toString())){
                    jg.writeNumberField(population.toString(), (double) Math.round( ((float) variantNode.getProperty(population.toString()) * 100) * 100d) / 100d);
                }
            }
            for (Neo4j.exACPopulation population : Neo4j.exACPopulation.values()) {
                if (variantNode.hasProperty(population.toString())){
                    jg.writeNumberField(population.toString(), (double) Math.round( ((float) variantNode.getProperty(population.toString()) * 100) * 100d) / 100d);
                }
            }

        }
    }
    private void writeFunctionalAnnotation(Node annotationNode, Relationship consequenceRel, Relationship biotypeRel, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            if (annotationNode.hasProperty("HGVSc"))
                jg.writeStringField("HGVSc", annotationNode.getProperty("HGVSc").toString());
            if (annotationNode.hasProperty("HGVSp"))
                jg.writeStringField("HGVSp", annotationNode.getProperty("HGVSp").toString());
            if (annotationNode.hasProperty("Exon")) {
                jg.writeNumberField("Location", (short) annotationNode.getProperty("Exon"));
            } else if (annotationNode.hasProperty("Intron")) {
                jg.writeNumberField("Location", (short) annotationNode.getProperty("Intron"));
            }
            if (annotationNode.hasProperty("Sift"))
                jg.writeStringField("Sift", annotationNode.getProperty("Sift").toString());
            if (annotationNode.hasProperty("Polyphen"))
                jg.writeStringField("Polyphen", annotationNode.getProperty("Polyphen").toString());
            if (annotationNode.hasProperty("Codons"))
                jg.writeStringField("Codons", annotationNode.getProperty("Codons").toString());

            //domains
            if (annotationNode.hasProperty("Pfam_domain")) {
                String[] domains = (String[]) annotationNode.getProperty("Pfam_domain");

                jg.writeArrayFieldStart("Pfam_domain");

                for (String domain : domains) {
                    jg.writeString(domain);
                }

                jg.writeEndArray();
            }

            if (annotationNode.hasProperty("hmmpanther")) {
                String[] domains = (String[]) annotationNode.getProperty("hmmpanther");

                jg.writeArrayFieldStart("hmmpanther");

                for (String domain : domains) {
                    jg.writeString(domain);
                }

                jg.writeEndArray();
            }

            if (annotationNode.hasProperty("prosite")) {
                String[] domains = (String[]) annotationNode.getProperty("prosite");

                jg.writeArrayFieldStart("prosite");

                for (String domain : domains) {
                    jg.writeString(domain);
                }

                jg.writeEndArray();
            }

            if (annotationNode.hasProperty("Superfamily_domains")) {
                String[] domains = (String[]) annotationNode.getProperty("Superfamily_domains");

                jg.writeArrayFieldStart("Superfamily_domains");

                for (String domain : domains) {
                    jg.writeString(domain);
                }

                jg.writeEndArray();
            }

            //consequence
            String consequence = consequenceRel.getType().name();
            if (consequence.length() > 16) {
                jg.writeStringField("Consequence", consequence.substring(4, consequence.length() - 12));
            }

            //biotype
            String biotype = biotypeRel.getType().name();
            if (biotype.length() > 12) jg.writeStringField("Biotype", biotype.substring(4, biotype.length() - 8));

        }

    }

    //todo sort
    private boolean isClassificationPendingApproval(Node variantNode){

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship pathogenicityRel : variantNode.getRelationships(Direction.OUTGOING, Neo4j.getHasPathogenicityRelationship())) {
                Node pathogenicityNode = pathogenicityRel.getEndNode();

                if (pathogenicityNode.hasLabel(Neo4j.getPathogenicityLabel())){

                    Relationship addedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddedByRelationship(), Direction.OUTGOING);
                    Relationship addAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddAuthorisedByRelationship(), Direction.OUTGOING);
                    Relationship removedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemovedByRelationship(), Direction.OUTGOING);
                    Relationship removeAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);

                    if (addedByRelationship != null && addAuthorisedByRelationship == null){
                        return true;
                    } else if (removedByRelationship != null && removeAuthorisedByRelationship == null){
                        return true;
                    }

                }

            }

        }

        return false;
    }
    private void writeFeatureHistory(Node featureNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeArrayFieldStart("History");

            //loop over feature preferences
            for (Relationship featurePreferenceRelationship : featureNode.getRelationships(Direction.OUTGOING, Neo4j.getHasFeaturePreferenceRelationship())) {
                Node featurePreferenceNode = featurePreferenceRelationship.getEndNode();

                if (featurePreferenceNode.hasLabel(Neo4j.getFeaturePreferenceLabel())) {

                    jg.writeStartObject();

                    //get user activity
                    Relationship addedByRelationship = featurePreferenceNode.getSingleRelationship(Neo4j.getAddedByRelationship(), Direction.OUTGOING);
                    Relationship removedByRelationship = featurePreferenceNode.getSingleRelationship(Neo4j.getRemovedByRelationship(), Direction.OUTGOING);
                    Relationship addAuthorisedByRelationship = featurePreferenceNode.getSingleRelationship(Neo4j.getAddAuthorisedByRelationship(), Direction.OUTGOING);
                    Relationship removeAuthorisedByRelationship = featurePreferenceNode.getSingleRelationship(Neo4j.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);

                    if (addedByRelationship != null) {
                        Node userNode = addedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            if (addedByRelationship.hasProperty("Evidence"))
                                jg.writeStringField("AddEvidence", addedByRelationship.getProperty("Evidence").toString());
                            if (addedByRelationship.hasProperty("Date"))
                                jg.writeNumberField("AddedDate", (long) addedByRelationship.getProperty("Date"));
                            if (userNode.hasProperty("UserId"))
                                jg.writeStringField("AddedByUserId", userNode.getProperty("UserId").toString());
                        }

                    }

                    if (addAuthorisedByRelationship != null) {
                        Node userNode = addAuthorisedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            if (userNode.hasProperty("UserId"))
                                jg.writeStringField("AddAuthorisedByUserId", userNode.getProperty("UserId").toString());
                            if (addAuthorisedByRelationship.hasProperty("Date"))
                                jg.writeNumberField("AddAuthorisedDate", (long) addAuthorisedByRelationship.getProperty("Date"));
                        }

                    }

                    if (removedByRelationship != null) {
                        Node userNode = removedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            if (removedByRelationship.hasProperty("Evidence"))
                                jg.writeStringField("RemovedEvidence", removedByRelationship.getProperty("Evidence").toString());
                            if (removedByRelationship.hasProperty("Date"))
                                jg.writeNumberField("RemovedDate", (long) removedByRelationship.getProperty("Date"));
                            if (userNode.hasProperty("UserId"))
                                jg.writeStringField("RemovedByUserId", userNode.getProperty("UserId").toString());
                        }

                    }

                    if (removeAuthorisedByRelationship != null) {
                        Node userNode = removeAuthorisedByRelationship.getEndNode();

                        if (userNode.hasLabel(Neo4j.getUserLabel())) {
                            if (userNode.hasProperty("UserId"))
                                jg.writeStringField("RemoveAuthorisedByUserId", userNode.getProperty("UserId").toString());
                            if (removeAuthorisedByRelationship.hasProperty("Date"))
                                jg.writeNumberField("RemoveAuthorisedDate", (long) removeAuthorisedByRelationship.getProperty("Date"));
                        }

                    }

                    jg.writeEndObject();
                }

            }

            jg.writeEndArray();

        }
    }
    private void writeVariantHistory(Node variantNode, JsonGenerator jg) throws IOException {
        jg.writeArrayFieldStart("History");

        for (Relationship pathogenicityRelationship : variantNode.getRelationships(Direction.OUTGOING, Neo4j.getHasPathogenicityRelationship())) {
            Node pathogenicityNode = pathogenicityRelationship.getEndNode();

            if (pathogenicityNode.hasLabel(Neo4j.getPathogenicityLabel())) {
                jg.writeStartObject();

                jg.writeNumberField("PathogenicityNodeId", pathogenicityNode.getId());

                Relationship addedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddedByRelationship(), Direction.OUTGOING);
                Relationship removedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemovedByRelationship(), Direction.OUTGOING);
                Relationship addAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getAddAuthorisedByRelationship(), Direction.OUTGOING);
                Relationship removeAuthorisedByRelationship = pathogenicityNode.getSingleRelationship(Neo4j.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);

                if (addedByRelationship != null){
                    Node userNode = addedByRelationship.getEndNode();

                    if (userNode.hasLabel(Neo4j.getUserLabel())){
                        if (userNode.hasProperty("UserId")) jg.writeStringField("AddedByUserId", userNode.getProperty("UserId").toString());
                        if (addedByRelationship.hasProperty("Date")) jg.writeNumberField("AddedDate", (long) addedByRelationship.getProperty("Date"));

                        if (addedByRelationship.hasProperty("Evidence")) jg.writeStringField("AddEvidence", addedByRelationship.getProperty("Evidence").toString());
                        if (addedByRelationship.hasProperty("Classification")) jg.writeNumberField("Classification", (int) addedByRelationship.getProperty("Classification"));
                    }

                }

                if (addAuthorisedByRelationship != null){
                    Node userNode = addAuthorisedByRelationship.getEndNode();

                    if (userNode.hasLabel(Neo4j.getUserLabel())){
                        if (userNode.hasProperty("UserId")) jg.writeStringField("AddAuthorisedByUserId", userNode.getProperty("UserId").toString());
                        if (addAuthorisedByRelationship.hasProperty("Date")) jg.writeNumberField("AddAuthorisedDate", (long) addAuthorisedByRelationship.getProperty("Date"));
                    }

                }

                if (removedByRelationship != null){
                    Node userNode = removedByRelationship.getEndNode();

                    if (userNode.hasLabel(Neo4j.getUserLabel())){
                        if (userNode.hasProperty("UserId")) jg.writeStringField("RemovedByUserId", userNode.getProperty("UserId").toString());
                        if (removedByRelationship.hasProperty("Date")) jg.writeNumberField("RemovedDate", (long) removedByRelationship.getProperty("Date"));

                        if (removedByRelationship.hasProperty("Evidence")) jg.writeStringField("RemovedEvidence", removedByRelationship.getProperty("Evidence").toString());
                    }

                }

                if (removeAuthorisedByRelationship != null){
                    Node userNode = removeAuthorisedByRelationship.getEndNode();

                    if (userNode.hasLabel(Neo4j.getUserLabel())){
                        if (userNode.hasProperty("UserId")) jg.writeStringField("RemoveAuthorisedByUserId", userNode.getProperty("UserId").toString());
                        if (removeAuthorisedByRelationship.hasProperty("Date")) jg.writeNumberField("RemoveAuthorisedDate", (long) removeAuthorisedByRelationship.getProperty("Date"));
                    }

                }

                jg.writeEndObject();
            }

        }

        jg.writeEndArray();
    }

}