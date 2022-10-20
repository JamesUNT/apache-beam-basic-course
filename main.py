#Imports:
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

#Setting options:
pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

#-----------------------------------------------------------------

#Arguments:
colunas_dengue = ['id',
                  'data_iniSE',
                  'casos',
                  'ibge_code',
                  'cidade',
                  'uf',
                  'cep',
                  'latitude',
                  'longitude']

#Helpers:
def texto_para_lista(elemento, delimitador="|"):
    """
    Recebe um texto e um delimitador e
    retorna uma lista de elementos pelo
    delimitador.
    """
    
    return elemento.split(delimitador)
    
def lista_para_dicionario(elemento, colunas):
    """
    Recebe um array the elementos(elemento) e
    um array de chaves(colunas), e os transfor-
    ma rem um objeto python.
    
    Ex: dict(zip(['a','b','c'],[1,2,3])) -> {'a':1,'b':2,'c':3}
    """
    
    return dict(zip(colunas, elemento))
    
def trata_datas(elemento):
    """
    Recebe um dicionario e cria um novo campo
    com o formato ANO_MES; retorna o mesmo di-
    cionario, com um novo campo.
    
    Ex: "2016-08-01" -> ['2016','08'] -> "2016-08"
    """
    
    elemento['ano_mes'] = "-".join(elemento['data_iniSE'].split('-')[:2])
    return elemento
    
def chave_uf(elemento):
    """
    Receber um dicionario e ira retornar uma
    tupla com estado (UF) e o elemento (UF, dicionario)
    
    Ex: [0,
         2015-11-08,
         0.0,
         230010,
         Abaiara,
         CE,
         63240-000,
         -7.3364,
         -39.0613] -> (CE,[0,2015-11-08,0.0,230010,...])
    """
    
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{},...]) e retorna
    uma tupla ('RS-2014-12', 8.0)
    """
    
    uf, registros = elemento
    
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
            
def chave_uf_ano_mes_de_lista(elemento):
    """
    Recebe uma lista de elementos elemento e
    retorna uma tupla contendo uma chave e o 
    valor de chuva em mm.
    
    Ex: ['2016-01-24','0.0','TO'] -> ('UF_ANO_MES', 0.0)
    """
    
    data, mm, uf = elemento
    ano_mes = '-'.join(elemento[0].split('-')[:2])
    chave = f"{uf}-{ano_mes}"
    
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
        
    return (chave, mm)
    
def arredonda(elemento):
    """
    Recebe um tupla e retorna um tupla
    com o valor arredondado
    
    Ex: ('RO-2019-04', 950.8000000000028) -> ('RO-2019-04', 950.80)
    """
    
    chave, mm = elemento
    
    return (chave, round(mm, 1))
    
def filtra_campos_vazios(elemento):
    """
    Recebe um tupla e retorna a mesma tupla,
    porem remove elementos que tenham chaves vazias.
    """
    
    chave, dados = elemento
    if all([
        dados['chuva'],
        dados['dengue']
    ]):
        return True
    return False
#-----------------------------------------------------------------

# ***\\\Starting pipeline///*** #
dengue = (
    pipeline
    
    | "Ler do dataset de dengue" 
        >> ReadFromText("sample_casos_dengue.txt", skip_header_lines=1)
        
    | "Transforma de texto para lista" 
        >> beam.Map(texto_para_lista)
        
    | "Transforma de lista para dicionario"
        >> beam.Map(lista_para_dicionario, colunas_dengue)
        
    | "Cria campo 'ano_mes'"
        >> beam.Map(trata_datas)
        
    | "Cria chave pelo estado"
        >> beam.Map(chave_uf)
        
    | "Agrupa pelo estado (UF)"
        >> beam.GroupByKey()
        
    | "Descompacta casos de dengue"
        >> beam.FlatMap(casos_dengue)
        
    | "Soma dos casos pela chave"
        >> beam.CombinePerKey(sum)
        
    # | "Mostra resultados"
        # >> beam.Map(print)   
)# --> PCollection

chuvas = (
    pipeline
    
    | "Leitura do dataset de chuvas"
        >> ReadFromText("sample_chuvas.csv", skip_header_lines=1)
        
    | "Transforma de texto(chuvas.csv) para lista" 
        >> beam.Map(texto_para_lista, delimitador=',')
        
    | "Criando chave UF_ANO_MES" 
        >> beam.Map(chave_uf_ano_mes_de_lista)
        
    | "Soma dos casos pela chave UF_ANO_MES"
        >> beam.CombinePerKey(sum)
        
    | "Arrendondar resultados de chuvas" 
        >> beam.Map(arredonda)
        
    # | "Mostra resultados chuvas"
        # >> beam.Map(print) 
)# --> PCollection

resultado = (
    ({'chuva':chuvas, 'dengue':dengue})
        
    | "Agrupa PCollections" 
        >> beam.CoGroupByKey()
    | "Filtra campos vazios" 
        >> beam.Filter(filtra_campos_vazios)
    
    | "Mostra resultados da unificacao"
        >> beam.Map(print)
) # --> PCollection



pipeline.run()

"""
Anotacoes:
    # Map: Funcao que realiza um processamento 1-to-1, ou seja, 
      dada uma determinada funcao, uma transformacao sera 
      realizada em um elemento e seu retorno sera um elemento 
      processado.
      
      Mais info em: https://beam.apache.org/documentation/transforms/python/elementwise/map/
      
    # FlatMap: Funcao que realiza um processamento 1-to-many, ou seja,
      dada uma determinada funcao, uma transformacao sera realizada em
      um elemento, sendo este elemento iteravel ou nao; caso seja iteravel,
      retornara um elemento processado para cada item contido.
      
      Mais info em: https://beam.apache.org/documentation/transforms/python/elementwise/flatmap/
      
    # CoGroupByKey: Funcao que realiza o agrupamento de duas ou
      mais PCollections atraves de uma chave commum.
      
      Mais info em: 
    
"""