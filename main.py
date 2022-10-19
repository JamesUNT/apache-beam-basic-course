#Imports:
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

#Setting options:
pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

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

# ***\\\Starting pipeline///*** #
dengue = (
    pipeline
    
    | "Ler do dataset de dengue" 
        >> ReadFromText("casos_dengue.txt", skip_header_lines=1)
        
    | "De texto para lista" 
        >> beam.Map(texto_para_lista)
        
    | "De lista para dicionario"
        >> beam.Map(lista_para_dicionario, colunas_dengue)
        
    | "Cria campo 'ano_mes'"
        >> beam.Map(trata_datas)
        
    | "Mostra resultados"
        >> beam.Map(print)   
)# --> PCollection

pipeline.run()