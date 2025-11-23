# Nossa aplicação tem o objetivo de utilizar uma base de dados semiestruturada do dados.gov
# A base escolhida foi a de Salas da Ancine por todo o país.
# Diante disso escolhemos a temática de analisar os dados e descobrimos que daria para fazer uma análise de dados com relação a 
# acessibilidade das salas.

import pandas as pd

# =======================================================
# Carregar o dataframe limpo gerado pelo dados.py
# =======================================================
try:
    df_global = pd.read_excel("salas_df_normal.xlsx")
    print("[OK] DataFrame limpo carregado com sucesso!")
except Exception as erro:
    print("[ERRO] Falha ao carregar salas_df_normal.xlsx:", erro)
    df_global = None


# =======================================================
# 1. ANALISAR ESTADOS
# =======================================================
def analiseEstados():
    global df_global

    print("\n=== ANÁLISE POR ESTADOS ===\n")

    if df_global is None:
        print("ERRO: dataframe não carregado.")
        return
    
    df_global["SALA_ACESSIVEL"] = (
        (df_global["ACESSO_SALA_COM_RAMPA"] == 1) |
        (df_global["BANHEIROS_ACESSIVEIS"] == 1)
    )

    resumo = df_global.groupby("UF_COMPLEXO").agg(
        total_salas=("REGISTRO_SALA", "count"),
        salas_acessiveis=("SALA_ACESSIVEL", "sum")
    )

    resumo["percentual_acessibilidade"] = (
        resumo["salas_acessiveis"] / resumo["total_salas"] * 100
    ).round(2)

    print(resumo.sort_values("percentual_acessibilidade", ascending=False))
    print("\n====================================\n")


# =======================================================
# 2. PERCENTUAL DE SALAS COM ACESSIBILIDADE
# =======================================================
def percentualSalaComacessibilidade():
    global df_global
    
    print("\n=== PERCENTUAL DE SALAS COM ACESSIBILIDADE ===\n")

    if df_global is None:
        print("ERRO: dataframe não carregado.")
        return
    
    df_temp = df_global.copy()

    df_temp["total_assentos"] = df_temp["ASSENTOS_SALA"]

    df_temp["assentos_adaptados"] = (
        df_temp["ASSENTOS_CADEIRANTES"] +
        df_temp["ASSENTOS_MOBILIDADE_REDUZIDA"] +
        df_temp["ASSENTOS_OBESIDADE"]
    )

    df_temp["percentual_adaptado"] = (
        df_temp["assentos_adaptados"] / df_temp["total_assentos"] * 100
    ).fillna(0).round(2)

    print(
        df_temp[[
            "UF_COMPLEXO", "MUNICIPIO_COMPLEXO",
            "REGISTRO_SALA", "percentual_adaptado"
        ]].sort_values("percentual_adaptado", ascending=False)
    )

    print("\n====================================\n")


# =======================================================
# 3. MELHORES CIDADES
# =======================================================
def melhoresCidades():
    global df_global
    
    print("\n=== MELHORES CIDADES COM ACESSIBILIDADE ===\n")

    if df_global is None:
        print("ERRO: dataframe não carregado.")
        return

    df_global["SALA_ACESSIVEL"] = (
        (df_global["ACESSO_SALA_COM_RAMPA"] == 1) |
        (df_global["BANHEIROS_ACESSIVEIS"] == 1)
    )

    ranking = df_global.groupby("MUNICIPIO_COMPLEXO").agg(
        total_salas=("REGISTRO_SALA", "count"),
        salas_acessiveis=("SALA_ACESSIVEL", "sum")
    )

    ranking["percentual"] = (
        ranking["salas_acessiveis"] / ranking["total_salas"] * 100
    ).round(2)

    print(ranking.sort_values("percentual", ascending=False).head(20))

    print("\n====================================\n")


# =======================================================
# MENU PRINCIPAL
# =======================================================
def menu():
    print("===== MENU DE OPÇÕES =====\n")
    print("1 - ANALISAR POR ESTADO")
    print("2 - PERCENTUAL SALA COM ACESSIBILIDADE")
    print("3 - MELHORES CIDADES COM ACESSIBILIDADE")
    print("0 - SAIR\n")

    escolha = int(input("DIGITE SUA OPÇÃO PARA NAVEGAR: "))

    while escolha != 0:
        if escolha == 1:
            print("EXIBINDO ANÁLISE POR ESTADOS...\n")
            analiseEstados()
        elif escolha == 2:
            print("EXIBINDO ANÁLISE POR PERCENTUAL DE SALAS COM ACESSIBILIDADE...\n")
            percentualSalaComacessibilidade()
        elif escolha == 3:
            print("EXIBINDO ANÁLISE DE MELHORES CIDADES COM ACESSIBILIDADE...\n")
            melhoresCidades()
        else:
            print("VOCÊ DIGITOU ALGO INVÁLIDO! TENTE NOVAMENTE\n")

        escolha = int(input("DIGITE SUA OPÇÃO PARA NAVEGAR: "))

    print("SAINDO...")


# =======================================================
# EXECUÇÃO
# =======================================================
menu()
