from google.cloud import bigquery
from google.cloud import documentai_v1 as documentai
from google.cloud.exceptions import NotFound
from google.api_core.client_options import ClientOptions
import json

def send_processing_req(project_id, location, processor_id, file_path, mime_type, GCS_INPUT_URI = None):
    """
    Process a document through Document AI and return a document ai object
    """
    
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options = ClientOptions(api_endpoint=f'{location}-documentai.googleapis.com')
    )

    RESOURCE_NAME = docai_client.processor_path(project_id, location, processor_id)

    # load file into memory
    with open(file_path, 'rb') as image:
        image_content = image.read()

    raw_doc = documentai.RawDocument(content=image_content, mime_type=MIME_TYPE)
    request = documentai.ProcessRequest(name=RESOURCE_NAME, raw_document=raw_doc)

    result = docai_client.process_document(request=request)

    document_object = result.document
    print('Document processing complete')
    return document_object

def write_to_bq(dataset_name, table_name, entities_extracted_dict):
    """
    Write output data to bigquery
    """
    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    def table_exists(client, table_ref):
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    if not table_exists(bq_client, table_ref):
        print('table does not exist, creating table')
        schema = [bigquery.SchemaField(
            'input_file_name', 'STRING', mode='NULLABLE')]
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')

    row_to_insert = []
    row_to_insert.append(entities_extracted_dict)

    json_data = json.dumps(row_to_insert, sort_keys=False)
    json_object = json.loads(json_data)

    schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
    ]
    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    job_config = bigquery.LoadJobConfig(
        schema_update_options=schema_update_options,
        source_format=source_format
    )

    job = bq_client.load_table_from_json(
        json_object, table_ref, job_config=job_config)
    print(job.result())


def extract_document_entities(document: documentai.Document) -> dict:
    """
    Get all entities from a document and output as a dictionary
    Flattens nested entities/properties
    Format: entity.type_: entity.mention_text OR entity.normalized_value.text
    """

    document_entities: Dict[str, Any] = {}

    def extract_document_entity(entity: documentai.Document.Entity):
        """
        Extract Single Entity and Add to Entity Dictionary
        """
        entity_key = entity.type_.replace('/', '_')
        entity_key = entity.type_.replace(' ', '_')
        entity_key = entity.type_.replace('-', '_')
        normalized_value = getattr(entity, 'normalized_value', None)

        new_entity_value = (
            normalized_value.text if normalized_value else entity.mention_text
        )

        existing_entity = document_entities.get(entity_key)
        #  for entities that can have multiple lines
        if existing_entity:
            #  change entity type to a list
            if not isinstance(existing_entity, list):
                existing_entity = list([existing_entity])

            existing_entity.append(new_entity_value)
            document_entities[entity_key] = existing_entity
        else:
            document_entities.update({entity_key: new_entity_value})

    for entity in document.entities:
        extract_document_entity(entity)

        for prop in entity.properties:
            extract_document_entity(prop)

    return document_entities

def format_keys(entities_dict):
    new_dict = {}
    for key, value in entities_dict.items():
        new_dict[key.replace(' ', '_')] = value
    return new_dict

def process_document(document_object, input_file_name, dataset_name, entities_table_name):
    """
    Save the extracted entities to bigquery
    """

    # reading all entities into a dict to write to bq table
    entities = extract_document_entities(document_object)
    entities = format_keys(entities)
    entities['input_file_name'] = input_file_name
    entities['text'] = document_object.text

    print('Entities: ', entities)
    print('Writing DocAI Entities to bq')

    write_to_bq(dataset_name, entities_table_name, entities)

    return
