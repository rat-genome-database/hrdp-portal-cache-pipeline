package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.spring.StringMapQuery;
import edu.mcw.rgd.datamodel.HrdpPortalCache;
import edu.mcw.rgd.datamodel.Map;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class hrdpPortalManager {

    private String version;
    Logger log = LogManager.getLogger("status");
    StrainDAO strainDAO = new StrainDAO();
    SampleDAO sampleDAO = new SampleDAO();
    AnnotationDAO annotationDAO = new AnnotationDAO();
    OntologyXDAO ontologyDAO = new OntologyXDAO();
    PhenominerDAO phenominerDAO = new PhenominerDAO();

    HrdpPortalCacheDAO cacheDAO = new HrdpPortalCacheDAO();
    HrdpPortalAvailanilityDAO availDAO = new HrdpPortalAvailanilityDAO();

    Map mapKey;

    {
        try {
            mapKey = new MapDAO().getPrimaryRefAssembly(3);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        hrdpPortalManager manager = (hrdpPortalManager) (bf.getBean("manager"));

        try {
            manager.run();
        }catch (Exception e) {
            Utils.printStackTrace(e, manager.log);
            throw e;
        }
    }

    private void run() throws Exception {
        long time0 = System.currentTimeMillis();
        log.info(getVersion());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("   started at "+sdt.format(new Date()));
        log.info("");
        log.info("started inserting classic inbred strains");
        run("Classic Inbred Strains");
        log.info("finished inserting classic inbred strains");
        log.info("");
        log.info("started inserting HXB/BXH Recombinant Inbred Panel strains");
        run("HXB/BXH Recombinant Inbred Panel");
        log.info("finished inserting HXB/BXH Recombinant Inbred Panel strains");
        log.info("");
        log.info("started inserting FXLE/LEXF Recombinant Inbred Panel strains");
        run("FXLE/LEXF Recombinant Inbred Panel");
        log.info("finished inserting FXLE/LEXF Recombinant Inbred Panell strains");
        log.info("");
        log.info("=== OK === elapsed "+ Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    public void run(String groupName) throws Exception{

        List<Strain> hrdpStrains = strainDAO.getStrainsByGroupNameAndSubGroupName("HRDP", groupName);

        if (hrdpStrains!=null){
            for (Strain str : hrdpStrains) {
                String parentOntId = ontologyDAO.getStrainOntIdForRgdId(str.getRgdId());
                List<StringMapQuery.MapPair>childOntIds  = annotationDAO.getChildOntIds(parentOntId);
                int phenoRecCount = phenominerDAO.getRecordCountForTerm(parentOntId, 3);
                List<Sample> parentSamples = sampleDAO.getSamplesByStrainRgdIdAndMapKey(str.getRgdId(), mapKey.getKey());
                boolean hasPhenominer = false;
                boolean hasVariantVisualizer = false;
                boolean childHasPhenominer = false;
                boolean childSamplesExist = false;
                String childOntIdsString="";
                String parentSampleString="";
                String childSampleString="";
                List<String> validChildOntIds = new ArrayList<>();
                List<Sample> allChildSamples = new ArrayList<>();

                if(childOntIds!=null) {
                    for (StringMapQuery.MapPair childOntId : childOntIds) {
                        int childPhenoRecCount = phenominerDAO.getRecordCountForTerm(childOntId.keyValue, 3);
                        if (childPhenoRecCount > 0) {
                            childHasPhenominer = true;
                            validChildOntIds.add(childOntId.keyValue);
                        }
                        List<Sample>childSamples = sampleDAO.getSamplesByStrainRgdIdAndMapKey(Integer.parseInt(childOntId.stringValue), mapKey.getKey());
                        if(childSamples!=null){
                            allChildSamples.addAll(childSamples);
                        }
                    }
                }
                hasPhenominer = phenoRecCount > 0 || childHasPhenominer;

                childSamplesExist = allChildSamples != null && allChildSamples.size() > 0;
                hasVariantVisualizer = (parentSamples != null && parentSamples.size() > 0)||childSamplesExist;

                childOntIdsString = String.join(",", validChildOntIds);

                if (parentSamples != null && !parentSamples.isEmpty()) {
                   parentSampleString = parentSamples.stream()
                            .map(Sample::getId)
                            .map(Object::toString)
                            .collect(Collectors.joining(","));
                }

                if (allChildSamples != null && !allChildSamples.isEmpty()) {
                    childSampleString = allChildSamples.stream()
                            .map(Sample::getId)
                            .map(Object::toString)
                            .collect(Collectors.joining(","));
                }

                //logic for retrieving available strain id and it's symbol
                int availableId = availDAO.getAvailableStrainByPrimaryStrainId(str.getRgdId());
                String availSymbol=null;
                if(availableId!=0){
                    Strain availStrain = strainDAO.getStrainByKey(availableId);
                    availSymbol = availStrain.getSymbol();
                }

                HrdpPortalCache hrdp = new HrdpPortalCache();
                hrdp.setStrainId(str.getRgdId());
                hrdp.setStrainSymbol(str.getSymbol());
                hrdp.setGroupName(groupName);
                hrdp.setParentOntId(parentOntId);
                hrdp.setChildOntIds(childOntIdsString);
                hrdp.setParentSampleIds(parentSampleString);
                hrdp.setChildSampleIds(childSampleString);
                hrdp.setHasParentPhenoCount(phenoRecCount>0?1:0);
                hrdp.setHasChildPhenoCount(childHasPhenominer?1:0);
                hrdp.setHasParentSampleCount((parentSamples != null && parentSamples.size() > 0)?1:0);
                hrdp.setHasChildSampleCount(childSamplesExist?1:0);
                hrdp.setHasPhenominer(hasPhenominer?1:0);
                hrdp.setHasVariantVisualizer(hasVariantVisualizer?1:0);
                hrdp.setAvailableStrainId(availableId);
                hrdp.setAvailableStrainSymbol(availSymbol);

                boolean checkStrainExists = cacheDAO.checkStrainExists(str.getRgdId(),groupName);
                if(checkStrainExists){
                    cacheDAO.updateHrdpPortalData(hrdp);
                    log.info("HRDP strain of rgdId: "+str.getRgdId()+" and symbol: "+str.getSymbol()+" and subgroup: "+groupName+" is updated");
                }
                else{
                    cacheDAO.insertHrdpPortalData(hrdp);
                    log.info("HRDP strain of rgdId: "+str.getRgdId()+" and symbol: "+str.getSymbol()+" and subgroup: "+groupName+" is inserted");
                }


            }

        }
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
