from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
import numpy as np

try:
    from xgboost import XGBClassifier
except ImportError:  # pragma: no cover - depends on optional dependency availability
    XGBClassifier = None

try:
    from lightgbm import LGBMClassifier
except ImportError:  # pragma: no cover - depends on optional dependency availability
    LGBMClassifier = None

"""
Classe base para todos os wrappers de modelos de classificação.
    
Define a interface comum que todos os modelos devem implementar,
garantindo compatibilidade com o pipeline de treinamento distribuído.
"""
class BaseModel:
    
    def __init__(self, params: dict):
        self.params = params
        self.model = None       # instância do modelo sklearn/xgboost/lightgbm
        self.n_classes_ = None  # número de classes detectado durante o fit

    def fit(self, X, y):
        """Treina o modelo nos dados fornecidos. Deve ser implementado pelas subclasses."""
        raise NotImplementedError

    def predict(self, X):
        """Retorna as classes preditas para as amostras em X."""
        if self.model is None:
            raise ValueError("O modelo ainda não foi treinado. Chame o método 'fit' antes de 'predict'.")
        
        return self.model.predict(X)

    def predict_proba(self, X):
        """Retorna as probabilidades de classe para as amostras em X.
        
        Para classificação binária, retorna apenas a probabilidade da classe positiva (coluna 1).
        Para classificação multiclasse, retorna a matriz completa de probabilidades.
        Retorna None se o modelo subjacente não suportar predict_proba.
        """
        if self.model is None:
            raise ValueError("O modelo ainda não foi treinado. Chame o método 'fit' antes de 'predict_proba'.")

        if not hasattr(self.model, "predict_proba"):
            return None
        proba = self.model.predict_proba(X)
        # binário: retorna prob da classe positiva; multiclasse: retorna matriz completa
        if self.n_classes_ == 2:
            return proba[:, 1]
        return proba

class MLPWrapper(BaseModel):

    def __init__(self, params):
        super().__init__(params)
        self.model = MLPClassifier(**params)

    def fit(self, X, y):
        self.model.fit(X, y)
        self.n_classes_ = len(np.unique(y))

class RFWrapper(BaseModel):

    def __init__(self, params):
        super().__init__(params)
        self.model = RandomForestClassifier(**params)

    def fit(self, X, y):
        self.model.fit(X, y)
        self.n_classes_ = len(np.unique(y))

class XGBWrapper(BaseModel):
    # Wrapper para o XGBClassifier do XGBoost.
    # Força eval_metric='logloss' e verbosity=0 para suprimir saídas de log desnecessárias.

    def __init__(self, params):
        super().__init__(params)
        if XGBClassifier is None:
            raise ImportError("xgboost não está instalado")
        self.model = XGBClassifier(**params, eval_metric="logloss", verbosity=0)

    def fit(self, X, y):
        self.model.fit(X, y)
        self.n_classes_ = len(np.unique(y))

class LGBMWrapper(BaseModel):
    # Wrapper para o LGBMClassifier do LightGBM.
    # Força verbosity=-1 para suprimir saídas de log desnecessárias.

    def __init__(self, params):
        super().__init__(params)
        if LGBMClassifier is None:
            raise ImportError("lightgbm não está instalado")
        self.model = LGBMClassifier(**params, verbosity=-1)

    def fit(self, X, y):
        self.model.fit(X, y)
        self.n_classes_ = len(np.unique(y))


# 🔹 Factory
def get_model(model_name: str, params: dict) -> BaseModel:
    """Cria e retorna uma instância do wrapper correspondente ao nome do modelo.
    
    Funciona como uma factory function: mapeia o nome (string) ao wrapper correto,
    permitindo que o restante do sistema instancie modelos sem conhecer as classes concretas.
    
    Args:
        model_name: Nome do modelo ('mlp', 'random_forest', 'xgboost' ou 'lightgbm').
        params:     Dicionário de hiperparâmetros repassado ao construtor do wrapper.
    
    Returns:
        Instância de BaseModel pronta para ser treinada via fit().
    
    Raises:
        ValueError: Se model_name não corresponder a nenhum modelo suportado.
    """
    model_name = model_name.lower()

    if model_name == "mlp":
        return MLPWrapper(params)
    elif model_name == "random_forest":
        return RFWrapper(params)
    elif model_name == "xgboost":
        return XGBWrapper(params)
    elif model_name == "lightgbm":
        return LGBMWrapper(params)
    else:
        raise ValueError(f"Modelo {model_name} não suportado")