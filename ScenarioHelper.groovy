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
import oracle.odi.runtime.agent.*;
import oracle.odi.domain.runtime.session.*;

def odiInstance;
  
def run(scenario, user, password){
  print "INFO  : running scenario, this may take some time ... "
  runtimeAgent = new RuntimeAgent(odiInstance, user, password.toCharArray())
  sdkExecInfo = runtimeAgent.startScenario(scenario.getName(), scenario.getVersion(), null, null, "LCL_CONTEXT", 5, null, true)
  sdkSession = odiInstance.getTransactionalEntityManager().getFinder(OdiSession.class).findBySessionId(sdkExecInfo.getSessionId())
  println sdkSession.getStatus()
}

def initRevEngPackage(revEngPackage, model){
  println "UPDATE: remove all steps from package: " + revEngPackage
  while ( !revEngPackage.getSteps().isEmpty() ) {
    revEngPackage.removeStep(revEngPackage.getSteps().first())
  }
  reverseType = model.getReverseType()
    
  if (reverseType == OdiModel.ReverseType.CUSTOMIZED){
      updateCustomRevEngPackage(revEngPackage, model)    
  }
  else {
    updateStandardRevEngPackage(revEngPackage, model)
  } 
}


def generateScenarios(projectCode, projectFolder, mappings){
  scenarioGenerator = new OdiScenarioGeneratorImpl(odiInstance)
  scenarios = []
  mappings.each { mapping ->
    existingScenarios = mapping.getScenarios();
    assert ( existingScenarios.isEmpty() || existingScenarios.size() == 1) : "WARN: more than one scenario found for mapping " + mapping
    if ( existingScenarios.isEmpty() ) {
      println "CREATE: generate scenario for mapping: " + mapping
      scenario = scenarioGenerator.generateScenario(mapping, mapping.getName().toUpperCase() + "_PHYSICAL", "001")
      odiInstance.getTransactionalEntityManager().persist(scenario)
      scenarios.add(scenario)
    }
    else {
      println "EXISTS: not regeneragting existing scenario: " + existingScenarios[0]
      scenarios.add(existingScenarios[0])
    }
  }
  return scenarios
}

def ensureScenario(sourcePackage){
  name = sourcePackage.getName().toUpperCase();
  scenarioGenerator = new OdiScenarioGeneratorImpl(odiInstance)
  scenarioFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiScenario.class);
  existingScenarios = scenarioFinder.findBySourcePackage(sourcePackage.getInternalId())
  assert ( existingScenarios.isEmpty() || existingScenarios.size() == 1) : "WARN: more than one scenario found for package " + odiPackage
  if ( existingScenarios.isEmpty() ) {
    println "CREATE: generate scenario: " + name
    scenario = scenarioGenerator.generateScenario(sourcePackage, name, "001")
    odiInstance.getTransactionalEntityManager().persist(scenario)
  }
  else {
    println "EXISTS: not regeneragting existing scenario: " + existingScenarios[0]
    scenario = existingScenarios[0]
  }
  return scenario
}


// private helper methods
private def updateStandardRevEngPackage(revEngPackage, model){
  modelId = model.getGlobalId();

  StepOdiCommand step1 = new StepOdiCommand(revEngPackage,"ResetTable")
  step1.setCommandExpression(new Expression("OdiReverseResetTable \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  StepOdiCommand step2 = new StepOdiCommand(revEngPackage,"GetMetaData")
  step2.setCommandExpression(new Expression("OdiReverseGetMetaData \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  StepOdiCommand step3 = new StepOdiCommand(revEngPackage,"SetMetaData")
  step3.setCommandExpression(new Expression("OdiReverseSetMetaData \"-MODEL="+modelId+"\"",null, Expression.SqlGroupType.NONE))
  revEngPackage.setFirstStep(step1)
  step1.setNextStepAfterSuccess(step2)
  step2.setNextStepAfterSuccess(step3)
  
  println "UPDATE: added steps for standard reverse engineering to package: " + revEngPackage
  odiInstance.getTransactionalEntityManager().persist(revEngPackage)
}

private def updateCustomRevEngPackage(revEngPackage, model){

  reverseEngModel = new StepModel(revEngPackage, model, "Rev_" + model.getName())
  reverseEngModel.setAction(new StepModel.ReverseModel())
  
  revEngPackage.setFirstStep(reverseEngModel)
  
  println "UPDATE: added steps for custom reverse engineering to package: " + revEngPackage
  odiInstance.getTransactionalEntityManager().persist(revEngPackage)
}