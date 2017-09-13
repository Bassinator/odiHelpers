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

def createModelFolder(name) {
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

def createModel(logicalSchema, name, parentFolder){
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

def createModel(logicalSchema, name, parentFolder, customRKM){
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


def createDataStore(model,name){
  dataStore = new OdiDataStore(model,name);
  odiInstance.getTransactionalEntityManager().persist(dataStore)
  println dataStore
}

def getDataStore(name, modelCode){
  dataStoreFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class);
  existingModel = dataStoreFinder.findByName(name, modelCode)
  return existingModel
}

def getModel(code){
  modelFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiModel.class);
  existingModel = modelFinder.findByCode(code)
  return existingModel;
}


def createProject(name, code) {
  project = getProject(code)
  if ( project == null) {
    project = new OdiProject(name, code) 
    odiInstance.getTransactionalEntityManager().persist(project)
    println "CREATE: project angelegt: " + project
  }
  else {
   println "EXISTS: project existiert bereits: " + project
  }
  return project
} 

def createProjectFolder(name, project) {
  folder = getProjectFolder(name, project.getCode())
  if ( folder == null) {
    folder = new OdiFolder(project, name)
    odiInstance.getTransactionalEntityManager().persist(folder)
    println "CREATE: project folder angelegt: " + folder
  }
  else {
    println "EXISTS: project folder existiert bereits: " + folder
  }
  return folder
} 

def getProject(code){
  projectFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiProject.class);
  return projectFinder.findByCode(code)
}

def getProjectFolder(name, projectCode){
  folderFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiFolder.class);
  existingFolders = folderFinder.findByName(name, projectCode)
  if ( ! existingFolders.isEmpty() ){
    return existingFolders.first()
  }
  else return null
}

def getMappings(projectCode, folderName){
  mappingFinder = odiInstance.getTransactionalEntityManager().getFinder(Mapping.class);
  existingmappings = mappingFinder.findByProject(projectCode, folderName)
  return existingmappings
}


def getMapping(name, folder){
  mappingFinder = odiInstance.getTransactionalEntityManager().getFinder(Mapping.class);
  existingmapping = mappingFinder.findByName(folder, name)
  return existingmapping
}


// @deprecatede use ProjectHelper
@Deprecated
def getPackage(name, projectCode, folderName){
  packageFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiPackage.class);
  existingPackage = packageFinder.findByName(name, projectCode, folderName)
  return existingPackage.getAt(0)
}

// @deprecatede use ProjectHelper
@Deprecated
def createPackage(name, projectCode, folder){
  odiPackage = getPackage(name, projectCode, folder.getName())
  if ( odiPackage == null ){
    odiPackage = new OdiPackage(folder, name)
    odiInstance.getTransactionalEntityManager().persist(odiPackage)
    println "CREATE: package erstellt: " + odiPackage
  }
  else {
    println "EXISTS: package existiert bereits: " + odiPackage
  }
  return odiPackage
}

  def getDataStoreInModelCaseInsensitive(name, model){
  dataStore = getDataStore(name.toLowerCase(), model.getCode())
  if (dataStore == null) {
    dataStore = getDataStore(name.toUpperCase(), model.getCode())
  }
   return dataStore
}


def createMapping(sourceDataStore,targetDataStore,projectFolder,transformationExpressions,filterExpression){
  name = targetDataStore.getName()
  mapping = getMapping(name, folder)
  if ( mapping == null ) {
    mapping = new Mapping(name, folder)
    println "CREATE: mapping erstellt: " + mapping
    odiInstance.getTransactionalEntityManager().persist(mapping)
  
    sourceDatastoreComponent = new DatastoreComponent(mapping, sourceDataStore)
    targetDatastoreComponent = new DatastoreComponent(mapping, targetDataStore)
    if ( filterExpression != "" ){
      filter = new FilterComponent(mapping, "DevelopmentFilter")
      sourceDatastoreComponent.connectTo(filter)
      filter.connectTo(targetDatastoreComponent)
      filter.setFilterCondition(filterExpression)
    }
    else {
      sourceDatastoreComponent.connectTo(targetDatastoreComponent)
    }
    
    
    sourceColumns = sourceDataStore.getColumns()
    sourceColumns.each { sourceColumn ->
      columnName = sourceColumn.getName().toLowerCase()
      targetColumn = targetDataStore.getColumn(columnName)
      if (targetColumn != null){
        sqlExpression = ""
        if ( transformationExpressions.containsKey(columnName.toUpperCase()) ) {
          function = transformationExpressions[columnName.toUpperCase()]
          sqlExpression =  function.replaceAll('#COLUMN_NAME',columnName).replaceAll('#TABLE_NAME',sourceDataStore.getName())

        }
        else {
          sqlExpression = sourceDataStore.getName()+"."+sourceColumn.getName()
        }
        createExpression(targetDatastoreComponent, targetColumn, sqlExpression)
      }
      else {
        println "WARN  : keine passende spalte in target Table gefunden für source column" + sourceColumn
      }
    }      
    odiInstance.getTransactionalEntityManager().persist(mapping)}
  else {
    println "EXISTS: mapping existiert bereits: " + mapping
  }
  return mapping
}

def setKnowledgeModules(mapping, lkmName, ikmName, ikmOptions){
    mapping.getPhysicalDesign(0).getAllAPNodes().get(0).setLKMByName(lkmName)
    pNode = mapping.getPhysicalDesign(0).getTargetNodes().get(0)
    pNode.setIKMByName(ikmName)
      
    ikmOptions.each { optionName, optionValue ->
      pNode.getIKMOptionValue(optionName).setValue(optionValue)
    }
    println "UPDATE: updated KnowledgeModules for mapping: " + mapping
}



def createExpression(dataStoreComponent, targetColumn, expressionText){
  MapAttribute attribute = DatastoreComponent.findAttributeForColumn(dataStoreComponent, targetColumn);
  attribute.setExpressionText(expressionText)
}

def generateScenariosFromMappings(projectCode, projectFolder, regenerate){
  mappings = getMappings(projectCode, projectFolder.getName())
  scenarioGenerator = new OdiScenarioGeneratorImpl(odiInstance)
  scenarios = []
  mappings.each { mapping ->
    existingScenarios = mapping.getScenarios();
    assert ( existingScenarios.isEmpty() || existingScenarios.size() == 1) : "es gibt schon mehr als ein Scenario für mapping: " + mapping
    if ( existingScenarios.isEmpty() ) {
      println "UPDATE: generate scenario for mapping: " + mapping
      scenario = scenarioGenerator.generateScenario(mapping, mapping.getName().toUpperCase() + "_PHYSICAL", "001")
      odiInstance.getTransactionalEntityManager().persist(scenario)
      scenarios.add(scenario)
    }
    else if ( regenerate ) {
      println "UPDATE: regeneragte Scenario: " + existingScenarios[0]
      scenarioGenerator.regenerateScenario(existingScenarios[0])
      odiInstance.getTransactionalEntityManager().persist(existingScenarios[0])
      scenarios.add(existingScenarios[0])
    }
    else {
      println "EXISTS: not regeneragting existing scenario: " + existingScenarios[0]
      scenarios.add(existingScenarios[0])
    }
  }
  return scenarios
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
      println "UPDATE: added as first step: " + actualStep
    }
    else {
      lastStep.setNextStepAfterSuccess(actualStep);  
      println "UPDATE: added as following step: " + actualStep
    }
    lastStep = actualStep
  }
  
  waitForChild = "OdiWaitForChildSession"
  actualStep = new StepOdiCommand(odiPackage,"waitForChildProcess");
  actualStep.setCommandExpression(new Expression(waitForChild, null, Expression.SqlGroupType.NONE)); 
  lastStep.setNextStepAfterSuccess(actualStep);   
  
  
  odiInstance.getTransactionalEntityManager().persist(odiPackage)
}