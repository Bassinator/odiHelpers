//Created by ODI Studio
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition;
import oracle.odi.domain.model.*;
import oracle.odi.domain.project.*;
import oracle.odi.domain.topology.*;
import oracle.odi.domain.util.ObfuscatedString;
import oracle.odi.domain.mapping.*;
import oracle.odi.domain.mapping.component.*;
import oracle.odi.generation.support.*;
import oracle.odi.generation.*;
import oracle.odi.domain.runtime.scenario.*;
import oracle.odi.domain.xrefs.expression.Expression;

def odiInstance;

def ensureModelFolder(name) {
  modelFolderFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiModelFolder.class);
  existingFolder = modelFolderFinder.findByName(name,null)
  
  if (existingFolder == null) {
    modelFolder = new OdiModelFolder(name)
    odiInstance.getTransactionalEntityManager().persist(modelFolder)
    println "CREATE: model folder angelegt: "+name
  }
  else {
    println "EXISTS: model folder existiert bereits: "+name
    modelFolder = existingFolder
  }
  return modelFolder
} 

def ensureModel(logicalSchema, name, parentFolder){
  code = name.toUpperCase()
  existingModel = getModel(code)

  if (existingModel == null) {
    model = new OdiModel(logicalSchema, name, code)
    model.setParentModelFolder(parentFolder);
    odiInstance.getTransactionalEntityManager().persist(model)
    println "CREATE: model angelegt: "+name
  }
  
  else {
    println "EXISTS: model existiert bereits: "+name
    model = existingModel
  }
  return model
}

def ensureModel(logicalSchema, name, parentFolder, customRKM){
  code = name.toUpperCase()
  existingModel = getModel(code)

  if (existingModel == null) {
    model = new OdiModel(logicalSchema, name, code)
    model.setParentModelFolder(parentFolder);
    model.setReverseType(OdiModel.ReverseType.CUSTOMIZED)
    model.setRKM(customRKM)
    odiInstance.getTransactionalEntityManager().persist(model)
    println "CREATE: model angelegt: "+name
  }
  
  else {
    println "EXISTS: model existiert bereits: "+name
    model = existingModel
  }
  return model
}


def getModel(code){
  modelFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiModel.class);
  existingModel = modelFinder.findByCode(code)
  return existingModel;
}

def getDataStore(name, modelCode){
  dataStoreFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class);
  existingModel = dataStoreFinder.findByName(name, modelCode)
  return existingModel
}

def compileScenario(scenarioSource){
  name = scenarioSource.getName().toUpperCase();
  scenarioGenerator = new OdiScenarioGeneratorImpl(odiInstance)
  scenarioFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiScenario.class);
  scenario = scenarioFinder.findLatestByName(name)
  if (scenario == null){
    println "CREATE: generate scenario: " + name
    scenario = scenarioGenerator.generateScenario(scenarioSource, name, "001")
    odiInstance.getTransactionalEntityManager().persist(scenario)
  }
  else {
    println "UPDATE: regeneragte Scenario: " + scenario
    scenarioGenerator.regenerateScenario(scenario)
    odiInstance.getTransactionalEntityManager().persist(scenario)
  }
  return scenario
}

def getLatestScenario(mapping){
  scenarioFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiScenario.class);
  existingScenario = scenarioFinder.findLatestBySourceMapping(mapping.getNumericId(),false)
  return existingScenario;
}

def addScenariosToPackage(scenarios, odiPackage, syncMode){
  scenarios.each { scenario ->
    scenarioName = scenario.getName()
    scenarioVersion = scenario.getVersion()
    actualStep = new StepOdiCommand(odiPackage,scenarioName);
    
    startScenCmnd = "OdiStartScen"
    startScenCmnd += " -SCEN_NAME=" + scenarioName
    startScenCmnd += " -SCEN_VERSION=" + scenarioVersion
    startScenCmnd += " -SYNC_MODE=" + syncMode
    
    actualStep.setCommandExpression(new Expression(startScenCmnd,null, Expression.SqlGroupType.NONE));
    if(scenario == scenarios.first()) {
      odiPackage.setFirstStep(actualStep);
      println "UPDATE: added as first step: " + actualStep.getCommandExpression() 
    }
    else {
      lastStep.setNextStepAfterSuccess(actualStep);  
      println "UPDATE: added as following step: " + actualStep.getCommandExpression() 
    }
    lastStep = actualStep
  }
  
  waitForChild = "OdiWaitForChildSession"
  actualStep = new StepOdiCommand(odiPackage,"waitForChildProcess");
  actualStep.setCommandExpression(new Expression(waitForChild, null, Expression.SqlGroupType.NONE)); 
  lastStep.setNextStepAfterSuccess(actualStep);   
  
  
  odiInstance.getTransactionalEntityManager().persist(odiPackage)
}