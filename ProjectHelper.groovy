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
import oracle.odi.domain.xrefs.expression.Expression;

def odiInstance;

// public methods
def ensureProject(name, code) {
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

def ensureFolder(name, project) {
  folder = getFolder(name, project.getCode())
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

def ensurePackage(name, project, folder){
  odiPackage = getPackage(name, project, folder)
  if ( odiPackage == null ){
    odiPackage = createPackage(name, folder)
  }
  else {
    println "EXISTS: package already exists: " + odiPackage
  }
  return odiPackage
}

def emptyPackage(odiPackage){
  println "UPDATE: remove all steps from package: " + odiPackage
  while ( !odiPackage.getSteps().isEmpty() ) {
    odiPackage.removeStep(odiPackage.getSteps().first())
  }
}


def ensureMapping(name, folder){
  mapping = getMapping(name, folder)
  if ( mapping == null ) {
    mapping = new Mapping(name, folder)
    odiInstance.getTransactionalEntityManager().persist(mapping)
    println "CREATE: mapping created: " + mapping
  }
  return mapping
}

def ensureMapping(mappingConfig, lkm, lkmOptions, ikm, ikmOptions){
  name = mappingConfig.targetDataStore.getName()
  mapping = getMapping(name, mappingConfig.folder)
  if ( mapping == null ) {
    mapping = new Mapping(name, mappingConfig.folder)
    println "CREATE: mapping erstellt: " + mapping
    odiInstance.getTransactionalEntityManager().persist(mapping)
  
    sourceDatastoreComponent = new DatastoreComponent(mapping, mappingConfig.sourceDataStore)
    println "CREATE: created data store component: " + sourceDatastoreComponent
    targetDatastoreComponent = new DatastoreComponent(mapping, mappingConfig.targetDataStore)
    if (mappingConfig.filterExpression != null) {
      filter = new FilterComponent(mapping, "Filter")
      println "CREATE: created data store component: " + targetDatastoreComponent
      sourceDatastoreComponent.connectTo(filter)
      filter.setFilterCondition(mappingConfig.filterExpression)
      filter.connectTo(targetDatastoreComponent)
    }
    else {
      sourceDatastoreComponent.connectTo(targetDatastoreComponent)
    }
    
    sourceColumns = mappingConfig.sourceDataStore.getColumns()
    sourceColumns.each { sourceColumn ->
      columnName = sourceColumn.getName().toLowerCase()
      targetColumn = mappingConfig.targetDataStore.getColumn(columnName)
      if (targetColumn != null){
        sqlExpression = ""
        if ( mappingConfig.targetExpressions.containsKey(columnName.toUpperCase()) ) {
          function = mappingConfig.targetExpressions[columnName.toUpperCase()]
          sqlExpression =  function.replaceAll('#COLUMN_NAME',columnName).replaceAll('#TABLE_NAME',mappingConfig.sourceDataStore.getName())
        }
        else {
          sqlExpression = mappingConfig.sourceDataStore.getName()+"."+sourceColumn.getName()
        }
        expression = createExpression(targetDatastoreComponent, targetColumn, sqlExpression)
      }
      else {
        println "WARN  : no matching column in target table found for source column" + sourceColumn
      }
    }
    setKnowledgeModules(mapping, lkm, lkmOptions, ikm, ikmOptions)
    odiInstance.getTransactionalEntityManager().persist(mapping)}
  else {
    println "EXISTS: mapping existiert bereits: " + mapping
  }
  return mapping
}


// only getting stuff


def getProject(code){
  projectFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiProject.class);
  return projectFinder.findByCode(code)
}


def getFolder(name, projectCode){
  folderFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiFolder.class);
  existingFolders = folderFinder.findByName(name, projectCode)
  if ( ! existingFolders.isEmpty() ){
    return existingFolders.first()
  }
  else return null
}

def getMappings(project, folderName){
  mappingFinder = odiInstance.getTransactionalEntityManager().getFinder(Mapping.class);
  existingmappings = mappingFinder.findByProject(project.getCode(), folderName)
  return existingmappings
}


// private Helpers

private def setKnowledgeModules(mapping, lkm, lkmOptions, ikm, ikmOptions){
    sourceNode = mapping.getPhysicalDesign(0).getAllAPNodes().get(0)
    sourceNode.setLKM(lkm)
    lkmOptions.each { optionName, optionValue ->
      sourceNode.getLKMOptionValue(optionName).setValue(optionValue)
    }

    targetNode = mapping.getPhysicalDesign(0).getTargetNodes().get(0)
    targetNode.setIKM(ikm)
    ikmOptions.each { optionName, optionValue ->
      targetNode.getIKMOptionValue(optionName).setValue(optionValue)
    }
    println "UPDATE: updated KnowledgeModules for mapping: " + mapping
}

private def getPackage(name, project, folder){
  packageFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiPackage.class);
  existingPackage = packageFinder.findByName(name, project.getCode(), folder.getName())
  return existingPackage.getAt(0)
}

private def createPackage(name, folder){
    odiPackage = new OdiPackage(folder, name)
    odiInstance.getTransactionalEntityManager().persist(odiPackage)
    println "CREATE: package created: " + odiPackage
    return odiPackage
}

private def getMapping(name, folder){
  mappingFinder = odiInstance.getTransactionalEntityManager().getFinder(Mapping.class);
  existingmapping = mappingFinder.findByName(folder, name)
  return existingmapping
}


private def createExpression(dataStoreComponent, targetColumn, expressionText){
  MapAttribute attribute = DatastoreComponent.findAttributeForColumn(dataStoreComponent, targetColumn);
  attribute.setExpressionText(expressionText)
}

// helper classes

public class SimpleFilterMappingConfig {
  def folder
  def sourceDataStore
  def filterExpression
  def targetDataStore
  def targetExpressions
  def loadingKnowledgeModule
  def integrationKnowledgeModule
  
  def SimpleFilterMappingConfig(folder, sourceDataStore, filterExpression, targetDataStore, targetExpressions) {
    this.folder = folder
    this.sourceDataStore = sourceDataStore
    this.filterExpression = filterExpression
    this.targetDataStore = targetDataStore
    this.targetExpressions = targetExpressions
  }
}


def static simpleFilterMappingConfig(folder, sourceDataStore, filterExpression, targetDataStore, targetExpressions){
  return new SimpleFilterMappingConfig(folder, sourceDataStore, filterExpression, targetDataStore, targetExpressions)
}

