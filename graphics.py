import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_graphics(df_list, ativos):
    """
    Parametros:
    - df: DataFrame com os dados do ativo
    - ativo: Nome do ativo    
    """

    qtd_ativos = len(ativos)
    
    # Criar uma figura com subplots em colunas
    fig = make_subplots(
        rows=qtd_ativos, cols=1, vertical_spacing=0.1, shared_xaxes=False,
        subplot_titles=[f"{ativo}" for ativo in ativos]
    )
    
    # Adicionar gráficos para cada ativo
    for i, (df, ativo) in enumerate(zip(df_list, ativos)):
        # Índice para posicionamento em coluna
        # col = i + 1  # As colunas começam de 1 no Plotly
        row = i + 1  # As linhas começam de 1 no Plotly
        
        # Gráfico de linha
        fig.add_trace(
            go.Scatter(
                x=df['date_time'], y=df['close'], mode='lines+markers', name='Fechamento',
                text=df.apply(lambda row: f"{row['date_time'].strftime('%Y-%m-%d %H:%M')} - Close: {row['close']:.2f}", axis=1),
                textposition="top center", texttemplate="%{text}"
            ), 
            # row=1, col=col
            row=row, col=1
        )

        
        # Gráfico de Candlestick
        fig.add_trace(
            go.Candlestick(
                x=df['date_time'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='Candlestick'
            ),
            # row=2, col=col
            row=row, col=1
        )

        fig.update_xaxes(            
            # title_text="Data/Hora",
            rangeslider_visible=False,
            tickformat="%Y-%m-%d", 
            tickmode='linear',
            # tickangle=45, #formato dos ticks no eixo X            
            row=row,
            col=1,
        )
    
    # Atualizar layout
    fig.update_layout(
        # title="Gráficos de Fechamento e Candlestick para Múltiplos Ativos", #titulo do gráfico (opcional)
        height=1500,        
    )
    
    return fig