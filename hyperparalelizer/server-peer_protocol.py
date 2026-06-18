# mensagens externas entre redes peer e servidor (aquelas definidas no docs)

# EXEMPLO DE USO: 
# resultado = TaskResult(id_node=1, accuracy=0.98, precision=0.97, recall=0.96, f1_score=0.96, roc_auc=0.99)
# fila_local.put(resultado)

# PARA TRANSFORMAR EM JSON: model_dump_json()
# PARA RETRANSFORMAR EM OBJETO: model_validate_json()

from pydantic import BaseModel

# ===== PEER -> SERVER ===== 

class TaskResult(BaseModel):
    id_node: int
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    roc_auc: float

class JoinNetwork(BaseModel):
    id_node: int
    ip: str
    porta: int


# ===== SERVER -> PEER ===== 

class DataLocation(BaseModel):
    # TO DO
    # como vamos orientar o peer para montar o dataset???
    pass

class TrainingTask(BaseModel):
    # TO DO
    pass