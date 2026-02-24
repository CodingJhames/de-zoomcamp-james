import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Definimos la fuente: API de NYC Taxi del Zoomcamp
@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator=PageNumberPaginator(base_page=1, total_path=None)
    )
    for page in client.paginate():
        yield page

# Configuramos el pipeline para que guarde en DuckDB
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# Ejecutamos la carga de datos
load_info = pipeline.run(ny_taxi)
print(load_info)
