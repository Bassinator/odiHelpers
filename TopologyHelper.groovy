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

def ensureContextualSchemaMapping(context, logicalSchema, physicalSchema){
  existingMapping = getContextualSchemaMapping(context, logicalSchema)

  if (existingMapping == null ||  !existingMapping.getPhysicalSchema() == physicalSchema ) {
    mapping = new OdiContextualSchemaMapping(context, logicalSchema, physicalSchema)
    odiInstance.getTransactionalEntityManager().persist(mapping)
    println "CREATE: contextual schema mapping created"
  }
  else {
    println "EXISTS: contextual schema mapping already exists"
  }
  
}

 
def ensureLogicalSchema(name,technology) {
  existingSchema = getLogicalSchema(name)
  if (existingSchema == null ||  ! existingSchema.getTechnology() == technology ) {
    schema = new OdiLogicalSchema(technology, name)
    odiInstance.getTransactionalEntityManager().persist(schema)
    println "CREATE: logical schema created: "+name
  }
  else {
    println "EXISTS: logical schema already exists: "+name
    schema = existingSchema;
  }
  return schema
} 

def getLogicalSchema(name){
  schemaFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiLogicalSchema.class);
  return schemaFinder.findByName(name)
}


def ensureDataServer(technology, name, jdbcConnectionProperties, batchUpdateSize, fetchArraySize){
  dataServerFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiDataServer.class);
  existingDataServer = dataServerFinder.findByName(name)

  if (existingDataServer == null || existingDataServer.getTechnology() != technology) {
    dataServer = new OdiDataServer(technology, name)
    dataServer.setUsername(jdbcConnectionProperties.username)
    obfuscatedPW = ObfuscatedString.obfuscate(jdbcConnectionProperties.password)
    dataServer.setPassword(obfuscatedPW)
    dataServer.setConnectionSettings(jdbcConnectionProperties.connectionSettings)
    dataServer.setBatchUpdateSize(batchUpdateSize)
    dataServer.setFetchArraySize(fetchArraySize)
    
    odiInstance.getTransactionalEntityManager().persist(dataServer)
    println "CREATE: dataserver created: "+name
  }
  else {
    println "EXISTS: dataserver already exists: "+name
    dataServer = existingDataServer
  }
  return dataServer
}


def ensurePhysicalSchema(dataserver, name, workSchemaName){
  def schemaExists = false
  existingSchemata = dataserver.getPhysicalSchemas()
  existingSchema = getPhysicalSchema(dataserver.getName()+"."+name)
  if (existingSchema == null){
    schema = new OdiPhysicalSchema(dataServer)
    schema.setSchemaName(name)
    if (workSchemaName != null){
      schema.setWorkSchemaName(workSchemaName)
    }
    odiInstance.getTransactionalEntityManager().persist(schema)
    println "CREATE: schema created: "+schema.getName()
  }
  else {
    println "EXISTS: schema already exists: "+name
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

// helper classes

public class JdbcConnectionProperties {
  def connectionSettings
  def username = new String()
  def password = new String()
  
  def JdbcConnectionProperties(url, driver, username, password) {
    connectionSettings = new AbstractOdiDataServer.JdbcSettings(url, driver)
    this.username = username
    this.password = password
  }
}

def static createJdbcConnectionConfig(url, driver, username, password){
  return new JdbcConnectionProperties(url, driver, username, password);
}
