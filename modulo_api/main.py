import pandas as pd
import os

# =======================================================
# Criar pasta de saída
# =======================================================
OUTPUT_DIR = "modulo_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =======================================================
# Carregar o dataframe limpo
# =======================================================
try:
    df_global = pd.read_excel("salas_df_normal.xlsx")
    print("[OK] DataFrame limpo carregado com sucesso!")
except Exception as erro:
    print("[ERRO] Falha ao carregar salas_df_normal.xlsx:", erro)
    df_global = None


# =======================================================
# Função auxiliar para salvar apenas XLSX
# =======================================================
def salvar(df, nome_base):
    path = f"{OUTPUT_DIR}/{nome_base}.xlsx"
    df.to_excel(path, index=False)
    print(f"[OK] Arquivo gerado e salvo em: {path}")


# =======================================================
# 1. ANALISAR ESTADOS
# =======================================================
def analiseEstados():
    global df_global

    if df_global is None:
        print("[ERRO] dataframe não carregado.")
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

    # PRINTAR NA TELA
    print("\n=== ANÁLISE POR ESTADOS ===\n")
    print(resumo.sort_values("percentual_acessibilidade", ascending=False))
    print("\n====================================\n")

    # SALVAR XLSX
    salvar(resumo.reset_index(), "analise_estados")


# =======================================================
# 2. PERCENTUAL DE ACESSIBILIDADE DAS SALAS
# =======================================================
def percentualSalaComacessibilidade():
    global df_global

    if df_global is None:
        print("[ERRO] dataframe não carregado.")
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

    saida = df_temp[[
        "UF_COMPLEXO", "MUNICIPIO_COMPLEXO",
        "REGISTRO_SALA", "percentual_adaptado"
    ]]

    # PRINTAR NA TELA
    print("\n=== PERCENTUAL DE SALAS COM ACESSIBILIDADE ===\n")
    print(saida.sort_values("percentual_adaptado", ascending=False))
    print("\n====================================\n")

    # SALVAR XLSX
    salvar(saida, "percentual_acessibilidade_salas")


# =======================================================
# 3. MELHORES CIDADES
# =======================================================
def melhoresCidades():
    global df_global

    if df_global is None:
        print("[ERRO] dataframe não carregado.")
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

    ranking = ranking.sort_values("percentual", ascending=False)

    # PRINTAR NA TELA
    print("\n=== MELHORES CIDADES COM ACESSIBILIDADE ===\n")
    print(ranking.head(20))
    print("\n====================================\n")

    # SALVAR XLSX
    salvar(ranking.reset_index(), "ranking_melhores_cidades")


# =======================================================
# MENU PRINCIPAL
# =======================================================
def menu():
    print("\n===== MENU DE OPÇÕES =====\n")
    print("1 - ANALISAR POR ESTADO")
    print("2 - PERCENTUAL SALA COM ACESSIBILIDADE")
    print("3 - MELHORES CIDADES COM ACESSIBILIDADE")
    print("0 - SAIR\n")

    escolha = int(input("DIGITE SUA OPÇÃO PARA NAVEGAR: "))

    while escolha != 0:
        if escolha == 1:
            analiseEstados()
        elif escolha == 2:
            percentualSalaComacessibilidade()
        elif escolha == 3:
            melhoresCidades()
        else:
            print("Opção inválida! Tente novamente.\n")

        escolha = int(input("DIGITE SUA OPÇÃO PARA NAVEGAR: "))

    print("SAINDO...")


# =======================================================
# EXECUÇÃO
# =======================================================
menu()
