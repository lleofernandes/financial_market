from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score
from imblearn.over_sampling import SMOTE
import pandas as pd




def training_model():
  
    # Carregar o arquivo
    df = pd.read_excel('reports/ativos_resultado.xlsx')

    # Criar variações de preço
    df['price_variation'] = df['close'] - df['open']

    # Criar a variável target (0 ou 1 baseado na variação do preço)
    df['target'] = (df['price_variation'] > 0).astype(int)

    # Selecionar as features e a variável target
    features = ['open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume']
    target = 'target'

    # Tratar valores ausentes
    df.dropna(inplace=True)

    # Normalizar as features
    scaler = StandardScaler()
    df[features] = scaler.fit_transform(df[features])

    # Dividir os dados em treino e teste
    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Aplicar SMOTE para balancear as classes
    smote = SMOTE()
    X_resampled, y_resampled = smote.fit_resample(X_train, y_train)

    # Inicializar o modelo
    model = LogisticRegression()

    # Treinar o modelo com os dados resampleados
    model.fit(X_resampled, y_resampled)

    # Fazer previsões
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)

    # Ajustar threshold
    threshold = 0.3
    y_pred_custom = (y_pred_proba[:, 1] > threshold).astype(int)

    # Avaliar o modelo
    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba[:, 1])

    print(f"\nAcuracia do modelo: {accuracy:.2f}")
    print(f"AUC-ROC: {roc_auc:.2f}")

    print("\nMetricas detalhadas")
    print(classification_report(y_test, y_pred_custom))

    print("\nMatriz de confusao")
    print(confusion_matrix(y_test, y_pred_custom))

    # Validação cruzada
    scores = cross_val_score(model, X, y, cv=5)
    print(f"\nValidacao cruzada: {scores.mean():.2f}")

    # Refinar o modelo com novas features
    df['pct_change'] = df['close'].pct_change()
    df['rolling_mean'] = df['close'].rolling(window=5).mean()
    df['volatility'] = df['close'].rolling(window=5).std()

    # Simular estratégia de compra e venda
    capital = 500  # Capital inicial
    positions = 10  # Inicialmente sem ações

    for i, prediction in enumerate(y_pred_custom):
        price = X_test.iloc[i]['close']
        if prediction == 1 and capital > 0:  # Previsão de alta
            positions += capital / price
            capital = 0
        elif prediction == 0 and positions > 0:  # Previsão de baixa
            capital += positions * price
            positions = 0

    print(f"Capital final: {capital + (positions * price):.2f}")



training_model()