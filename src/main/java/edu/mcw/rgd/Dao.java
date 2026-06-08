package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.HrdpPortalCache;
import edu.mcw.rgd.datamodel.Map;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.datamodel.ontologyx.Term;

import java.util.List;

/**
 * wrapper for all database access used by the HRDP portal cache pipeline
 */
public class Dao {

    private final StrainDAO strainDAO = new StrainDAO();
    private final SampleDAO sampleDAO = new SampleDAO();
    private final OntologyXDAO ontologyDAO = new OntologyXDAO();
    private final PhenominerDAO phenominerDAO = new PhenominerDAO();
    private final MapDAO mapDAO = new MapDAO();
    private final HrdpPortalCacheDAO cacheDAO = new HrdpPortalCacheDAO();
    private final HrdpPortalAvailabilityDAO availDAO = new HrdpPortalAvailabilityDAO();

    public String getConnectionInfo() {
        return strainDAO.getConnectionInfo();
    }

    public Map getPrimaryRefAssembly(int speciesTypeKey) throws Exception {
        return mapDAO.getPrimaryRefAssembly(speciesTypeKey);
    }

    public List<Strain> getStrainsByGroupNameAndSubGroupName(String groupName, String subGroupName) throws Exception {
        return strainDAO.getStrainsByGroupNameAndSubGroupName(groupName, subGroupName);
    }

    public Strain getStrain(int rgdId) throws Exception {
        return strainDAO.getStrain(rgdId);
    }

    public int getStrainRgdIdByTaglessStrainSymbolNew(String taglessSymbol) throws Exception {
        return strainDAO.getStrainRgdIdByTaglessStrainSymbolNew(taglessSymbol);
    }

    public String getStrainOntIdForRgdId(int rgdId) throws Exception {
        return ontologyDAO.getStrainOntIdForRgdId(rgdId);
    }

    public List<Term> getAllActiveTermDescendants(String termAcc) throws Exception {
        return ontologyDAO.getAllActiveTermDescendants(termAcc);
    }

    public int getRecordCountForTerm(String accId, int speciesTypeKey) throws Exception {
        return phenominerDAO.getRecordCountForTerm(accId, speciesTypeKey);
    }

    public List<Sample> getSamplesByStrainRgdIdAndMapKey(int strainRgdId, int mapKey) throws Exception {
        return sampleDAO.getSamplesByStrainRgdIdAndMapKey(strainRgdId, mapKey);
    }

    public String getAvailableStrainByPrimaryStrainId(int strainId, String subGroupName) throws Exception {
        return availDAO.getAvailableStrainByPrimaryStrainId(strainId, subGroupName);
    }

    public boolean checkStrainExists(int strainId, String groupName) throws Exception {
        return cacheDAO.checkStrainExists(strainId, groupName);
    }

    public int updateHrdpPortalData(HrdpPortalCache hrdp) throws Exception {
        return cacheDAO.updateHrdpPortalData(hrdp);
    }

    public int insertHrdpPortalData(HrdpPortalCache hrdp) throws Exception {
        return cacheDAO.insertHrdpPortalData(hrdp);
    }
}
