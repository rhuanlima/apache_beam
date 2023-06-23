import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

id_dengue = [
            'id',
            'data_iniSE',
            'casos',
            'ibge_code',
            'cidade',
            'uf',
            'cep',
            'latitude',
            'longitude'
]

def cria_anomes(elemento):
    """cria variável ano-mes no dicionário do elemento

    Args:
        elemento (_type_): _description_

    Returns:
        elemento novo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """consttói uma tupla de uf e elemento

    Args:
        elemento (_type_): _description_

    Returns:
        tupla: UF,elemento
    """
    chave = elemento['uf']
    return (chave, elemento)

# Criando uma pcollection
dengue = (
    pipeline
    | "Captura do Dataset" >> ReadFromText('./data/casos_dengue.txt', skip_header_lines=1)
    | "Split da linha em uma lista" >> beam.Map(lambda x: x.split('|'))
    | "Lista para dict" >> beam.Map(lambda x: dict(zip(id_dengue, x)))
    | "Cria Ano_mes" >> beam.Map(cria_anomes)
    | "Cria tupla de estado" >> beam.Map(chave_uf)
    | "Agrupándo pelo estado" >> beam.GroupByKey()
    | "Resultados" >> beam.Map(print)
)
pipeline.run()