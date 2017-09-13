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

def ensureRevEngPackage(model, project, projectFolder){
  reverseType = model.getReverseType()
  if (reverseType == OdiModel.ReverseType.CUSTOMIZED){
    revEngPackage = ensureCustomRevEngPackage(model, project, projectFolder)    
  }
  else {
    revEngPackage = ensureStandardRevEngPackage(model, project, projectFolder)
  }
  return revEngPackage
}


private def ensureStandardRevEngPackage(model, project, projectFolder){
  revEngPackage = createPackage("Pkg_Rev_" + model.getName(), project, projectFolder);
  modelId = model.getGlobalId();

  println "UPDATE: remove all steps from package: " + revEngPackage
  while ( !revEngPackage.getSteps().isEmpty() ) {
   revEngPackage.removeStep(revEngPackage.getSteps().first())
  }


  StepOdiCommand step1 = new StepOdiCommand(revEngPackage,"ResetTable")
  step1.setCommandExpression(new Expression("OdiReverseResetTable \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  StepOdiCommand step2 = new StepOdiCommand(revEngPackage,"GetMetaData")
  step2.setCommandExpression(new Expression("OdiReverseGetMetaData \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  StepOdiCommand step3 = new StepOdiCommand(revEngPackage,"SetMetaData")
  step3.setCommandExpression(new Expression("OdiReverseSetMetaData \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  revEngPackage.setFirstStep(step1)
  step1.setNextStepAfterSuccess(step2)
  step2.setNextStepAfterSuccess(step3)
  
  println "UPDATE: added steps for reverse engineering to package: " + revEngPackage
  odiInstance.getTransactionalEntityManager().persist(revEngPackage)
  
  return revEngPackage
}


private def ensureCustomRevEngPackage(model, project, projectFolder){
  revEngPackage = createPackage("Pkg_Rev_" + model.getName(), project, projectFolder);

  println "UPDATE: remove all steps from package: " + revEngPackage
  while ( !revEngPackage.getSteps().isEmpty() ) {
   revEngPackage.removeStep(revEngPackage.getSteps().first())
  }

  reverseEngModel = new StepModel(revEngPackage, model, "Rev_" + model.getName())
  reverseEngModel.setAction(new StepModel.ReverseModel())
  
  revEngPackage.setFirstStep(reverseEngModel)
  
  println "UPDATE: added steps for reverse engineering to package: " + revEngPackage
  odiInstance.getTransactionalEntityManager().persist(revEngPackage)
  
  return revEngPackage
}


def createPackage(name, project, folder){
  odiPackage = getPackage(name, project, folder.getName())
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


def getPackage(name, project, folderName){
  packageFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiPackage.class);
  existingPackage = packageFinder.findByName(name, project.getCode(), folderName)
  return existingPackage.getAt(0)
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