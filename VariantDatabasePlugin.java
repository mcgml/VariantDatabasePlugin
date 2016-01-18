package nhs.genetics.cardiff;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.*;

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

    /*diagnostic*/
    @POST
    @Path("/returninputjson")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response returnInputJson(final String json) {
        return Response.status( Response.Status.OK ).entity(
                (json).getBytes( Charset.forName("UTF-8") ) ).build();
    }

    /*info*/
    @GET
    @Path("/workflows")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflows() {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/analyses")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAnalyses() {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/panels")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVirtualPanels() {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/variantinformation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVariantInformation(final String json) {

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

                        //write user action history
                        try (Transaction tx = graphDb.beginTx()) {
                            jg.writeArrayFieldStart("history");

                            for (Relationship relationship : variantNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasPathogenicityRelationship())){
                                writeActionHistory(relationship.getEndNode(), jg);
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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/featureinformation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFeatureInformation(final String json) {

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

                        writeFeatureInformation(featureNode, jg);

                    }

                    jg.writeEndObject();

                    jg.flush();
                    jg.close();
                }

            };

            return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/variantobservations")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVariantObservations(final String json) {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/functionalannotation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFunctionalAnnotation(final String json) {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/getvirtualpanel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVirtualPanel(final String json) {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/getuserinformation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUserInformation(final String json) {

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    /*workflows*/
    @POST
    @Path("/getfilteredvariants")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFilteredVariants(final String json) {
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
                    default:
                        throw new IllegalArgumentException("Unknown workflow");
                }

                jg.flush();
                jg.close();

            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

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
        jg.writeFieldName("filter");
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
                    jg.writeNumberField("filter", 0);

                    notAutosomalVariants++;
                    total++;

                    jg.writeEndObject();
                } else if (!isExACRareVariant(variantNode, 0.05)) {
                    jg.writeStartObject();

                    writeVariantInformation(variantNode, jg);
                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
                    jg.writeNumberField("filter", 1);

                    notExACRareVariants++;
                    total++;

                    jg.writeEndObject();
                } else if (!is1KGRareVariant(variantNode, 0.05)) {
                    jg.writeStartObject();

                    writeVariantInformation(variantNode, jg);
                    jg.writeStringField("inheritance", getVariantInheritance(inheritanceRel.getType().name()));
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

    /*user action*/
    @POST
    @Path("/addvirtualpanel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addVirtualPanel(final String json) {

        try {

            Date date = new Date();
            HashMap<String, Object> properties = new HashMap<>();
            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.getNodeById(parameters.userNodeId);

                if (userNode.hasLabel(VariantDatabase.getUserLabel())) {

                    //make panel node
                    properties.put("virtualPanelName", parameters.virtualPanelName);
                    Node virtualPanelNode = Neo4j.addNode(graphDb, VariantDatabase.getVirtualPanelLabel(), properties);
                    properties.clear();

                    //link to user
                    Relationship designedByRelationship = virtualPanelNode.createRelationshipTo(userNode, VariantDatabase.getDesignedByRelationship());
                    designedByRelationship.setProperty("date", date.getTime());

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/updateuserpassword")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateUserPassword(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);

            try (Transaction tx = graphDb.beginTx()) {
                Node userNode = graphDb.getNodeById(parameters.userNodeId);
                userNode.setProperty("password", parameters.password);
                tx.success();
            }

            return Response.status(Response.Status.OK).build();

        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }
    }

    @POST
    @Path("/createnewuser")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createNewUser(final String json) {

        try {

            User user = objectMapper.readValue(json, User.class);
            HashMap<String, Object> properties = new HashMap<>();

            properties.put("fullName", user.fullName);
            properties.put("password", user.password);
            properties.put("jobTitle", user.jobTitle);
            properties.put("userId", user.userId);
            properties.put("contactNumber", user.contactNumber);
            properties.put("admin", user.admin);

            try (Transaction tx = graphDb.beginTx()) {
                Neo4j.addNode(graphDb, VariantDatabase.getUserLabel(), properties);
                tx.success();
            }

            return Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/addvariantpathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addVariantPathogenicity(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);
            Node variantNode, userNode;

            try (Transaction tx = graphDb.beginTx()) {
                variantNode = graphDb.getNodeById(parameters.variantNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
            }

            //check no pending approvals
            if (getVariantPathogenicityPendingApprovalStatus(variantNode) != null){
                throw new IllegalArgumentException("Cannot modify classification while pending approval.");
            }

            //add user action
            if (parameters.classification < 1 || parameters.classification > 5){
                throw new IllegalArgumentException("Unknown classification");
            }

            HashMap<String, Object> properties = new HashMap<>();
            properties.put("classification", parameters.classification);

            UserAction.addUserAction(graphDb, variantNode, VariantDatabase.getHasPathogenicityRelationship(), Neo4j.addNode(graphDb, VariantDatabase.getVariantPathogenicityLabel(), properties), userNode, parameters.evidence);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (IllegalArgumentException e) {
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/removevariantathogenicity")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeVariantPathogenicity(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);
            Node variantNode, userNode, pathogenicityNode;

            try (Transaction tx = graphDb.beginTx()) {
                variantNode = graphDb.getNodeById(parameters.variantNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
                pathogenicityNode = graphDb.getNodeById(parameters.pathogenicityNodeId);
            }

            //check no pending approvals
            if (getVariantPathogenicityPendingApprovalStatus(variantNode) != null){
                throw new IllegalArgumentException("Cannot modify classification while pending approval.");
            }

            //request to remove action
            UserAction.removeUserAction(graphDb, pathogenicityNode, userNode, parameters.evidence);

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (IllegalArgumentException e) {
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @POST
    @Path("/authoriseuseraction")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response authoriseUserAction(final String json) {

        try {

            Parameters parameters = objectMapper.readValue(json, Parameters.class);
            Node actionNode, userNode;

            try (Transaction tx = graphDb.beginTx()) {
                actionNode = graphDb.getNodeById(parameters.actionNodeId);
                userNode = graphDb.getNodeById(parameters.userNodeId);
            }

            if (parameters.addorRemove){
                UserAction.authUserAction(graphDb, actionNode, VariantDatabase.getAddAuthorisedByRelationship(), userNode);
            } else {
                UserAction.authUserAction(graphDb, actionNode, VariantDatabase.getRemoveAuthorisedByRelationship(), userNode);
            }

            return Response
                    .status(Response.Status.OK)
                    .build();

        } catch (Exception e) {
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    @GET
    @Path("/getpathogenicityforauthorisation")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPathogenicityForAuthorisation() {

        try {

            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {

                    JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                    jg.writeStartArray();

                    try (Transaction tx = graphDb.beginTx()) {
                        try (ResourceIterator<Node> variantNodeIterator = graphDb.findNodes(VariantDatabase.getVariantLabel())){

                            while (variantNodeIterator.hasNext()) {
                                Node variantNode = variantNodeIterator.next();

                                for (Relationship hasPathogenicityRelationship : variantNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasPathogenicityRelationship())) {

                                    Node pathogenicityNode = hasPathogenicityRelationship.getEndNode();
                                    UserAction.UserActionStatus status = UserAction.getUserActionStatus(graphDb, pathogenicityNode);

                                    //check no pending approvals
                                    if (status.equals(UserAction.UserActionStatus.PENDING_APPROVAL) || status.equals(UserAction.UserActionStatus.PENDING_RETIRE)) {

                                        writeVariantInformation(variantNode, jg);
                                        writeActionHistory(pathogenicityNode, jg);
                                        jg.writeNumberField("classification", (Integer) pathogenicityNode.getProperty("classification"));

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
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity((e.getMessage()).getBytes(Charset.forName("UTF-8")))
                    .build();
        }

    }

    /*helper functions*/
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

                if (variantNode.hasProperty(population.toString())){
                    if ((float) variantNode.getProperty(population.toString()) > maxAlleleFrequency){
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

                if (variantNode.hasProperty(population.toString())){
                    if ((float) variantNode.getProperty(population.toString()) > maxAlleleFrequency){
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
    private UserAction.UserActionStatus getVariantPathogenicityPendingApprovalStatus(Node variantNode){

        try (Transaction tx = graphDb.beginTx()) {

            for (Relationship relationship : variantNode.getRelationships(Direction.OUTGOING, VariantDatabase.getHasPathogenicityRelationship())) {

                Node pathogenicityNode = relationship.getEndNode();
                UserAction.UserActionStatus status = UserAction.getUserActionStatus(graphDb, pathogenicityNode);

                //check no pending approvals
                if (status.equals(UserAction.UserActionStatus.PENDING_APPROVAL) || status.equals(UserAction.UserActionStatus.PENDING_RETIRE)) {
                    return status;
                }

            }

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

        }
    }
    private void writeFeatureInformation(Node featureNode, JsonGenerator jg) throws IOException {
        try (Transaction tx = graphDb.beginTx()) {

            jg.writeNumberField("featureNodeId", featureNode.getId());

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

            jg.writeNumberField("variantNodeId", variantNode.getId());
            jg.writeNumberField("occurrence", getGlobalVariantOccurrence(variantNode));

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
                if (variantNode.hasProperty(population.toString())){
                    jg.writeNumberField(population.toString(), (double) Math.round( ((float) variantNode.getProperty(population.toString()) * 100) * 100d) / 100d);
                }
            }
            for (VariantDatabase.exacPopulation population : VariantDatabase.exacPopulation.values()) {
                if (variantNode.hasProperty(population.toString())){
                    jg.writeNumberField(population.toString(), (double) Math.round( ((float) variantNode.getProperty(population.toString()) * 100) * 100d) / 100d);
                }
            }

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
            String consequence = consequenceRel.getType().name();
            if (consequence.length() > 16) {
                jg.writeStringField("consequence", consequence.substring(4, consequence.length() - 12));
            }

            //biotype
            String biotype = biotypeRel.getType().name();
            if (biotype.length() > 12) jg.writeStringField("biotype", biotype.substring(4, biotype.length() - 8));

        }

    }
    private void writeActionHistory(Node actionNode, JsonGenerator jg) throws IOException {

        Node user;

        jg.writeStartObject();

        try (Transaction tx = graphDb.beginTx()) {

            Relationship addedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getAddedByRelationship(), Direction.OUTGOING);
            Relationship addAuthorisedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getAddAuthorisedByRelationship(), Direction.OUTGOING);
            Relationship removedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getRemovedByRelationship(), Direction.OUTGOING);
            Relationship removeAuthorisedByRelationship = actionNode.getSingleRelationship(VariantDatabase.getRemoveAuthorisedByRelationship(), Direction.OUTGOING);

            if (addedByRelationship != null){

                jg.writeObjectFieldStart("added");

                user = addedByRelationship.getEndNode();
                writeLiteUserInformation(user, jg);

                if (addedByRelationship.hasProperty("date")) {
                    jg.writeNumberField("date", (long) addedByRelationship.getProperty("date"));
                }
                if (addedByRelationship.hasProperty("evidence")) {
                    jg.writeStringField("evidence", addedByRelationship.getProperty("evidence").toString());
                }

                jg.writeEndObject();

            }

            if (addAuthorisedByRelationship != null){
                jg.writeObjectFieldStart("addedAuth");

                user = addAuthorisedByRelationship.getEndNode();
                writeLiteUserInformation(user, jg);

                if (addAuthorisedByRelationship.hasProperty("date")) {
                    jg.writeNumberField("date", (long) addAuthorisedByRelationship.getProperty("date"));
                }

                jg.writeEndObject();
            }

            if (removedByRelationship != null){
                jg.writeObjectFieldStart("removed");

                user = removedByRelationship.getEndNode();
                writeLiteUserInformation(user, jg);

                if (removedByRelationship.hasProperty("date")){
                    jg.writeNumberField("date", (long) removedByRelationship.getProperty("date"));
                }
                if (removedByRelationship.hasProperty("evidence")){
                    jg.writeStringField("evidence", removedByRelationship.getProperty("evidence").toString());
                }

                jg.writeEndObject();
            }

            if (removeAuthorisedByRelationship != null){
                jg.writeObjectFieldStart("removedAuth");

                user = removeAuthorisedByRelationship.getEndNode();
                writeLiteUserInformation(user, jg);

                if (removeAuthorisedByRelationship.hasProperty("date")){
                    jg.writeNumberField("date", (long) removeAuthorisedByRelationship.getProperty("date"));
                }

                jg.writeEndObject();
            }

        }

        jg.writeEndObject();

    }

}