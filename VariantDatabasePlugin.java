package nhs.genetics.cardiff;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;

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
import org.neo4j.graphdb.impl.notification.NotificationDetail;

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
        double version();
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

                        jg.writeFieldName("Version");
                        jg.writeNumber(method.getAnnotation(Workflow.class).version());

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
                                    jg.writeStringField("SampleId", (String) sampleNode.getProperty("SampleId"));
                                    jg.writeStringField("LibraryId", (String) runInfoNode.getProperty("LibraryId"));
                                    jg.writeStringField("RunId", (String) runInfoNode.getProperty("RunId"));
                                    jg.writeNumberField("RunInfoNodeId", runInfoNode.getId());
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
    public Response getPanels() {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> virtualPanelNodes = graphDb.findNodes(Neo4j.getVirtualPanelLabel())) {

                        while (virtualPanelNodes.hasNext()) {
                            Node virtualPanelNode = virtualPanelNodes.next();

                            for (Relationship relationship : virtualPanelNode.getRelationships(Direction.OUTGOING, Neo4j.getHasDesignedBy())) {
                                Node userNode = relationship.getEndNode();

                                if (userNode.hasLabel(Neo4j.getUserLabel())){
                                    jg.writeStartObject();
                                    jg.writeStringField("PanelName", (String) virtualPanelNode.getProperty("PanelName"));
                                    jg.writeNumberField("PanelNodeId", virtualPanelNode.getId());
                                    jg.writeNumberField("Date", (long) relationship.getProperty("Date"));
                                    jg.writeStringField("UserName", (String) userNode.getProperty("UserName"));
                                    jg.writeEndObject();
                                }
                            }
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
    @Path("/autosomaldominantworkflow")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Workflow(name = "Autosomal Dominant", version = 0.1, description = "A workflow to stratify homozygous, sex-linked, and frequent variants (>0.01 MAF).")
    public Response autosomalDominantWorkflow(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                int notHeterozygousVariants = 0, notAutosomalVariants = 0, notRareVariants = 0, passVariants = 0;
                HashSet<String> reportedVariants = new HashSet<>();

                int panelRunTimes = getPanelRunTimes("");

                jg.writeStartObject();

                jg.writeFieldName("Variants");
                jg.writeStartArray();

                WorkflowParameters workflowParameters = objectMapper.readValue(json, WorkflowParameters.class);

                try (Transaction tx = graphDb.beginTx()) {

                    Node runInfoNode = graphDb.getNodeById(workflowParameters.RunInfoNodeId);

                    for (Relationship inheritanceRel : runInfoNode.getRelationships(Direction.OUTGOING)) {
                        Node variantNode = inheritanceRel.getEndNode();

                        if (variantNode.hasLabel(Neo4j.getVariantLabel())) {

                            for (Relationship inSymbolRel : variantNode.getRelationships(Direction.OUTGOING, Neo4j.getHasInSymbolRelationship())) {
                                Node symbolNode = inSymbolRel.getEndNode();

                                for (Relationship containsSymbol : symbolNode.getRelationships(Direction.INCOMING, Neo4j.getHasContainsSymbol())) {
                                    Node virtualPanelNode = containsSymbol.getStartNode();

                                    if (virtualPanelNode.getId() == workflowParameters.PanelNodeId) {

                                        String variant = variantNode.getProperty("VariantId").toString();

                                        //remove variants already reported; caused by multiple gene association
                                        if (reportedVariants.contains(variant)){
                                            continue;
                                        }

                                        jg.writeStartObject();

                                        jg.writeNumberField("VariantNodeId", variantNode.getId());
                                        jg.writeStringField("VariantId", variant);
                                        reportedVariants.add(variant);

                                        //inheritance
                                        String inheritance = inheritanceRel.getType().name();
                                        if (inheritance.length() > 12) jg.writeStringField("Inheritance", inheritance.substring(4, inheritance.length() - 8));

                                        //occurrence
                                        int seen = getSeenTimes(variantNode);
                                        jg.writeNumberField("Occurrence", seen);
                                        jg.writeNumberField("InternalFrequency", (double)Math.round((((double)seen / (panelRunTimes * 2)) * 100) * 100d) / 100d);

                                        //id
                                        if (variantNode.hasProperty("Id")) jg.writeStringField("Id", variantNode.getProperty("Id").toString());

                                        //stratify variants
                                        if (inheritanceRel.isType(Neo4j.getHasHomVariantRelationship())) {
                                            jg.writeNumberField("Filter", 0);
                                            notHeterozygousVariants++;
                                        } else if (!variantNode.hasLabel(Neo4j.getAutoChromosomeLabel())) {
                                            jg.writeNumberField("Filter", 1);
                                            notAutosomalVariants++;
                                        } else if (!isRareVariant(variantNode, 0.01)) {
                                            jg.writeNumberField("Filter", 2);
                                            notRareVariants++;
                                        } else {
                                            jg.writeNumberField("Filter", 3);
                                            passVariants++;
                                        }


                                        //todo
                                        jg.writeArrayFieldStart("Pathogenicity");
                                        jg.writeNumber(0);
                                        jg.writeNumber(1);
                                        jg.writeNumber(2);
                                        jg.writeNumber(3);
                                        jg.writeEndArray();

                                        jg.writeEndObject();
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
                jg.writeStringField("key", "NotHeterozygousVariants");
                jg.writeNumberField("y", notHeterozygousVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "NotAutosomalVariants");
                jg.writeNumberField("y", notAutosomalVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "NotRareVariants");
                jg.writeNumberField("y", notRareVariants);
                jg.writeEndObject();

                jg.writeStartObject();
                jg.writeStringField("key", "PassVariants");
                jg.writeNumberField("y", passVariants);
                jg.writeEndObject();

                jg.writeEndArray();

                jg.writeEndObject();

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
                VariantIdParameter variantIdParameter = objectMapper.readValue(json, VariantIdParameter.class);
                Node variantNode = null;

                jg.writeStartObject();

                try (Transaction tx = graphDb.beginTx()) {
                    try (ResourceIterator<Node> variants = graphDb.findNodes(Neo4j.getVariantLabel(), "VariantId", variantIdParameter.VariantId)) {

                        while (variants.hasNext()) {
                            variantNode = variants.next();
                        }

                        variants.close();
                    }

                    jg.writeNumberField("VariantNodeId", variantNode.getId());
                    jg.writeNumberField("Occurrence", getSeenTimes(variantNode));
                    if (variantNode.hasProperty("Id")) jg.writeStringField("dbSNP", (String) variantNode.getProperty("Id"));

                }

                jg.writeEndObject();

                jg.flush();
                jg.close();
            }

        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();

    }

    @POST
    @Path("/populationfrequency")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPopulationFrequency(final String json) throws IOException {

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {

                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                NodeIdParameter nodeIdParameter = objectMapper.readValue(json, NodeIdParameter.class);

                jg.writeStartArray();
                jg.writeStartObject();

                //write population frequencies
                jg.writeFieldName("values");
                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {

                    Node variantNode = graphDb.getNodeById(nodeIdParameter.NodeId);

                    for (Neo4j.Population population : Neo4j.Population.values()) {

                        if (variantNode.hasProperty(population.toString())){

                            jg.writeStartObject();
                            jg.writeStringField("label", population.toString());
                            jg.writeNumberField("value", (double) Math.round( ((float) variantNode.getProperty(population.toString()) * 100) * 100d) / 100d);
                            jg.writeEndObject();

                        }

                    }

                }

                jg.writeEndArray();

                jg.writeEndObject();
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
                NodeIdParameter nodeIdParameter = objectMapper.readValue(json, NodeIdParameter.class);

                jg.writeStartArray();

                try (Transaction tx = graphDb.beginTx()) {
                    Node variantNode = graphDb.getNodeById(nodeIdParameter.NodeId);

                    for (Relationship consequenceRel : variantNode.getRelationships(Direction.OUTGOING)) {
                        Node annotationNode = consequenceRel.getEndNode();

                        if (annotationNode.hasLabel(Neo4j.getAnnotationLabel())){

                            for (Relationship inFeatureRel : annotationNode.getRelationships(Direction.OUTGOING, Neo4j.getHasInFeatureRelationship())) {
                                Node featureNode = inFeatureRel.getEndNode();

                                if (featureNode.hasLabel(Neo4j.getFeatureLabel())){

                                    for (Relationship biotypeRel : featureNode.getRelationships(Direction.INCOMING)) {

                                        //skip non-protein coding affects
                                        if (!biotypeRel.isType(Neo4j.getHasProteinCodingBiotypeRel())){
                                            continue;
                                        }

                                        Node symbolNode = biotypeRel.getStartNode();

                                        if (symbolNode.hasLabel(Neo4j.getSymbolLabel())){

                                            jg.writeStartObject();

                                            if (featureNode.hasProperty("FeatureId")) jg.writeStringField("Feature", featureNode.getProperty("FeatureId").toString());

                                            String biotype = biotypeRel.getType().name();
                                            if (biotype.length() > 12) jg.writeStringField("Biotype", biotype.substring(4, biotype.length() - 8));

                                            if (annotationNode.hasProperty("Exon")){
                                                jg.writeNumberField("Location", (short) annotationNode.getProperty("Exon"));
                                            } else if (annotationNode.hasProperty("Intron")){
                                                jg.writeNumberField("Location", (short) annotationNode.getProperty("Intron"));
                                            }

                                            String consequence = consequenceRel.getType().name();

                                            if (consequence.length() > 16){
                                                jg.writeStringField("Consequence", consequence.substring(4, consequence.length() - 12));
                                            }else {
                                                jg.writeStringField("Consequence", consequence);
                                            }

                                            if (annotationNode.hasProperty("HGVSc")) jg.writeStringField("HGVSc", annotationNode.getProperty("HGVSc").toString());
                                            if (annotationNode.hasProperty("HGVSp")) jg.writeStringField("HGVSp", annotationNode.getProperty("HGVSp").toString());
                                            if (annotationNode.hasProperty("Sift")) jg.writeStringField("SIFT", annotationNode.getProperty("Sift").toString());
                                            if (annotationNode.hasProperty("Polyphen")) jg.writeStringField("PolyPhen", annotationNode.getProperty("Polyphen").toString());
                                            if (symbolNode.hasProperty("SymbolId")) jg.writeStringField("Symbol", symbolNode.getProperty("SymbolId").toString());

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

    //helper functions
    private int getVariantPathogenicity( Node variantNode){

        try (Transaction tx = graphDb.beginTx()) {
            for (Relationship pathogenicityRelationship : variantNode.getRelationships(Direction.INCOMING, Neo4j.getHasAssociatedPathogenicityRelationship())) {
                if (pathogenicityRelationship.getStartNode().hasLabel(Neo4j.getUserLabel())){

                }
            }
        }

        return 0; //unknown classification
    }
    private int getSeenTimes(Node variantNode){

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
    private int getPanelRunTimes(String panelName){ //todo split out by panel name (i.e. TruSight Cancer)
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
    private boolean isRareVariant(Node variantNode, double maxAlleleFrequency){

        //filter variants
        try (Transaction tx = graphDb.beginTx()) {

            for (Neo4j.Population population : Neo4j.Population.values()) {

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

}