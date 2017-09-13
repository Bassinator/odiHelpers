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


def getRKnowledgeModule(name){
  rkmFinder = odiInstance.getTransactionalEntityManager().getFinder(OdiRKM.class);
  
  rKnowledgeModule = rkmFinder.findByName(name)
  
  return rKnowledgeModule;
}