//Created by ODI Studio
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition;
import oracle.odi.domain.model.*;
import oracle.odi.domain.topology.*;
import oracle.odi.domain.project.*;
import oracle.odi.domain.util.ObfuscatedString;

def odiInstance;
def reader;


def getTechnology(code){
  techFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiTechnology.class);
  technology = techFinder.findByCode(code); 
  
  return technology
}

def map(context, logicalSchema, physicalSchema){
  existingMapping = getContextualSchemaMapping(context, logicalSchema)

  if (existingMapping == null ||  !existingMapping.getPhysicalSchema() == physicalSchema ) {
    mapping = new OdiContextualSchemaMapping(context, logicalSchema, physicalSchema)
    odiInstance.getTransactionalEntityManager().persist(mapping)
    println "Contextual Schema Mapping angelegt"
  }
  else {
    println "Contextual Schema Mapping existiert bereits"
  }
  
}

 
def createLogicalSchema(name,technology) {
  existingSchema = getLogicalSchema(name)
  if (existingSchema == null ||  ! existingSchema.getTechnology() == technology ) {
    schema = new OdiLogicalSchema(technology, name)
    odiInstance.getTransactionalEntityManager().persist(schema)
    println "logical schema angelegt: "+name
  }
  else {
    println "logical schema existiert bereits: "+name
    schema = existingSchema;
  }
  return schema
} 

def getLogicalSchema(name){
  schemaFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiLogicalSchema.class);
  return schemaFinder.findByName(name)
}


def createDataServer(technology, name, jdbcDriver, jdbcUrl, batchUpdateSize, fetchArraySize){
  dataServer = createDataServer(technology, name, jdbcDriver, jdbcUrl)
  dataServer.setBatchUpdateSize(batchUpdateSize)
  dataServer.setFetchArraySize(fetchArraySize)
  odiInstance.getTransactionalEntityManager().persist(dataServer)
  println "CHANGE: dataserver updated: " + name + " batchUpdateSize= " + batchUpdateSize + " fetchArraySize= " + fetchArraySize
  return dataServer
}

def createDataServer(technology, name, jdbcDriver, jdbcUrl){
  dataServerFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiDataServer.class);
  existingDataServer = dataServerFinder.findByName(name)

  if (existingDataServer == null || existingDataServer.getTechnology() != technology) {
    dataServer = new OdiDataServer(technology, name)

    println "Input user to connect to: " + dataServer
    username=reader.readLine()
    println "Input password to connect to: " + dataServer
    password=reader.readLine()
    
    dataServer.setUsername(username)
    obfuscatedPW = ObfuscatedString.obfuscate(password)
    dataServer.setPassword(obfuscatedPW)
    connectionSettings = new AbstractOdiDataServer.JdbcSettings(jdbcUrl, jdbcDriver)
    dataServer.setConnectionSettings(connectionSettings)
    odiInstance.getTransactionalEntityManager().persist(dataServer)
    println "CREATE: dataserver angelegt: "+name
  }
  else {
    println "EXISTS: dataserver existiert bereits: "+name
    dataServer = existingDataServer
  }
  return dataServer
}

def createPhysicalSchema(dataserver, name){
  def schemaExists = false
  existingSchemata = dataserver.getPhysicalSchemas()
  existingSchema = getPhysicalSchema(dataserver.getName()+"."+name)
  if (existingSchema == null){
    schema = new OdiPhysicalSchema(dataServer)
    schema.setSchemaName(name)
    odiInstance.getTransactionalEntityManager().persist(schema)
    println "Schema angelegt: "+schema.getName()
  }
  else {
    println "Schema existiert bereits: "+name
    schema = existingSchema
  }
  return schema
}

def getPhysicalSchema(name){
  physicalSchemaFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiPhysicalSchema.class);
  allSchemata = physicalSchemaFinder.findAll()
  foundSchema = null
  
  allSchemata.each { actualSchema ->
    if (actualSchema.name == name) {
      foundSchema = actualSchema
    }
  }
  return foundSchema;
}

def getContextualSchemaMapping(context, logicalSchema){
  mappingFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiContextualSchemaMapping.class);
  allMappings = mappingFinder.findAll(); 
  
  foundMapping = null
  
  allMappings.each { actualMapping ->
    if (actualMapping.getInternalId().getLogicalSchema() == logicalSchema.getLogicalSchemaId() 
      && actualMapping.getInternalId().getContext() == context.getContextId() ) {
      foundMapping = actualMapping
    }
  }
  return foundMapping
}

def getContext(code){
  contextFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiContext.class);
  return contextFinder.findByCode(code)
}
