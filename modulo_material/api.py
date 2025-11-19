import requests 


def consultar_grupo_material(pagina=1):
    # é utilizada para fazer as requisições HTTP (get post e etc)
    url = "https://dadosabertos.compras.gov.br/modulo-material/1_consultarGrupoMaterial"
    # É a url de acordo com endpoint da api que estamos utilizando, a URL aponta para o recurso que retorna grupos de materiais no módulo de materiais.


    params = {
        "pagina": pagina
    }
    # Cria um dicionário Python chamado params que contém os parâmetros da query string (os parâmetros que viram ?pagina=1 na URL).
    # No caso, estamos pedindo a página 1 dos resultados (a API usa paginação).

    response = requests.get(url, params=params)
    # Faz a requisição HTTP do tipo GET para a url, anexando os parâmetros params.
    # Internamente ela monta a URL final, retornando o objeto que contem o codigo HTTP
    # corpo da resposta como string e apos isso inverte para JSON 


    if response.status_code == 200: #Verifica se o servidor respondeu com o código HTTP 200, que significa OK (requisição bem sucedida).
        dados = response.json() #Converte o corpo da resposta (que vem em JSON) para estruturas Python (tipicamente dict e list) e salva em dados.
        return dados
    else:
        print("Erro:", response.status_code, response.text)

