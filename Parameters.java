package nhs.genetics.cardiff;

/**
 * A POJO for REST de-serialisation
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2015-10-18
 */
public class Parameters {
    public String featureId;
    public String variantId;
    public String symbolId;
    public String sampleId;
    public Long runInfoNodeId;
    public Long variantNodeId;
    public Long featureNodeId;
    public Long userNodeId;
    public Long pathogenicityNodeId;
    public Integer classification;
    public String evidence;
    public String virtualPanelName;
    public String[] virtualPanelList;
    public Long[] excludeRunInfoNodes;
    public Long[] includePanelNodes;
    public Long[] variantNodeIds;
    public Long panelNodeId;
    public Long eventNodeId;
    public Boolean addOrRemove;
    public Boolean featurePreference;
    public Boolean passOrFail;
    public String userId;
    public String password;
    public String workflowName;
}
