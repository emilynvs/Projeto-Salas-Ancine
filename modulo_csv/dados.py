from leitor_csv import ler_csv_df
import pandas as pd


# =======================================================
# Função de log simples e objetiva
# =======================================================
def log(msg):
    print(f"[OK] {msg}")


# =======================================================
# Identificação do esquema original (antes da limpeza)
# =======================================================
def identificar_esquema_original(df):
    print("\n=== ESQUEMA ORIGINAL (ANTES DA LIMPEZA) ===")
    print(f"Total de colunas: {len(df.columns)}")
    print("Colunas:")
    for col in df.columns:
        print(" -", col)
    print("\nTipos originais:")
    print(df.dtypes)
    
    print("\nNulos por coluna (ANTES DA LIMPEZA):")
    print(df.isnull().sum())
    print("===========================================\n")


# =======================================================
# Identificação do esquema final (após limpeza)
# =======================================================
def identificar_esquema_final(df):
    print("\n=== ESQUEMA FINAL (APÓS A LIMPEZA) ===")
    print(f"Total de colunas: {len(df.columns)}")
    print("Colunas:")
    for col in df.columns:
        print(" -", col)
    print("\nTipos finais:")
    print(df.dtypes)
    
    print("\nNulos por coluna (APÓS A LIMPEZA):")
    print(df.isnull().sum())
    print("========================================\n")


# =======================================================
# Limpeza de datas
# =======================================================
def tratar_datas(df):
    datas = [
        "DATA_SITUACAO_SALA",
        "DATA_INICIO_FUNCIONAMENTO_SALA",
        "DATA_SITUACAO_COMPLEXO",
    ]
    for col in datas:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    log("Datas convertidas.")
    return df


# =======================================================
# Limpeza de números
# =======================================================
def tratar_numeros(df):
    numeros = [
        "REGISTRO_SALA",
        "ASSENTOS_SALA",
        "ASSENTOS_CADEIRANTES",
        "ASSENTOS_MOBILIDADE_REDUZIDA",
        "ASSENTOS_OBESIDADE",
        "NUMERO_ENDERECO_COMPLEXO",
        "NUMERO",   # agora garantimos conversão REAL para int
    ]
    for col in numeros:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .fillna(0)
                .astype(int)
            )

    log("Colunas numéricas tratadas.")
    return df


# =======================================================
# Normalização de categorias
# =======================================================
def tratar_categorias(df):
    if "UF_COMPLEXO" in df.columns:
        df["UF_COMPLEXO"] = df["UF_COMPLEXO"].astype(str).str.upper().fillna("XX")

    if "MUNICIPIO_COMPLEXO" in df.columns:
        df["MUNICIPIO_COMPLEXO"] = (
            df["MUNICIPIO_COMPLEXO"]
            .astype(str)
            .str.strip()
            .str.title()
            .fillna("Desconhecido")
        )

    log("Categorias padronizadas.")
    return df


# =======================================================
# Padronização de sinônimos
# =======================================================
def tratar_sinonimos(df):

    sinonimos_municipio = {
        "Sao Paulo": "São Paulo",
        "Sao-Paulo": "São Paulo",
        "S.paulo": "São Paulo",
        "Fortaleza-ce": "Fortaleza",
        "Rio De Janeiro": "Rio de Janeiro",
    }

    if "MUNICIPIO_COMPLEXO" in df.columns:
        df["MUNICIPIO_COMPLEXO"] = df["MUNICIPIO_COMPLEXO"].replace(sinonimos_municipio)

    sinonimos_sala = {
        "Sala 3-d": "Sala 3D",
        "Sala3d": "Sala 3D",
        "Imax": "Sala IMAX",
        "Sala Imax": "Sala IMAX",
    }

    if "TIPO_SALA" in df.columns:
        df["TIPO_SALA"] = df["TIPO_SALA"].astype(str).replace(sinonimos_sala)

    log("Sinônimos padronizados.")
    return df


# =======================================================
# Conversão de SIM/NÃO para 1/0
# =======================================================
def tratar_booleanos(df):
    mapa = {"SIM": 1, "NÃO": 0, "NAO": 0, "Não": 0, "S": 1, "N": 0}

    booleanas = [
        "ACESSO_ASSENTOS_COM_RAMPA",
        "ACESSO_SALA_COM_RAMPA",
        "BANHEIROS_ACESSIVEIS",
    ]

    for col in booleanas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().map(mapa).fillna(0).astype(int)

    log("Colunas booleanas convertidas.")
    return df


# =======================================================
# Tratamento do endereço (corrigido + robusto)
# =======================================================
def tratar_endereco(df):

    if "ENDERECO_COMPLEXO" not in df.columns:
        log("Coluna de endereço não encontrada.")
        return df

    df["ENDERECO_COMPLEXO"] = df["ENDERECO_COMPLEXO"].fillna("").astype(str)

    # === Extrair número (primeira sequência de dígitos)
    df["NUMERO"] = df["ENDERECO_COMPLEXO"].str.extract(r"(\d+)")

    # === Extrair logradouro (tudo antes do número)
    df["LOGRADOURO"] = (
        df["ENDERECO_COMPLEXO"].str.extract(r"^(.*?)(\d+)")[0].fillna("").str.strip()
    )

    # === Extrair complemento (texto após número)
    df["RESTO"] = (
        df["ENDERECO_COMPLEXO"]
        .str.extract(r"\d+(.*)$")[0]
        .fillna("")
        .str.strip()
    )

    # === BAIRRO ================================
    if "BAIRRO_COMPLEXO" in df.columns:
        df["BAIRRO"] = df["BAIRRO_COMPLEXO"].fillna("").astype(str)
    else:
        df["BAIRRO"] = ""

    # Se bairro estiver vazio → tentar extrair após "-"
    df.loc[df["BAIRRO"] == "", "BAIRRO"] = (
        df["RESTO"].str.split("-", n=1).str[1].fillna("").str.strip()
    )

    log("Endereços tratados.")
    return df


# =======================================================
# Pipeline de tratamento completo
# =======================================================
def limpar_dataframe(df):
    df = tratar_datas(df)
    df = tratar_endereco(df)   # antes de tratar_numeros!
    df = tratar_numeros(df)    # agora NUMERO já existe e é convertido corretamente
    df = tratar_categorias(df)
    df = tratar_sinonimos(df)
    df = tratar_booleanos(df)

    df = df.drop_duplicates()
    df = df.fillna("")

    log("Pipeline completo de limpeza executado.")
    return df


# =======================================================
# Execução principal
# =======================================================
df = ler_csv_df()
log("CSV carregado.")

identificar_esquema_original(df)

df = limpar_dataframe(df)

identificar_esquema_final(df)

df.to_excel("salas_df_normal.xlsx", index=False)
log("Arquivo salas_df_normal.xlsx exportado.")
