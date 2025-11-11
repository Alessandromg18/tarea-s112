import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    print(event)

    # --- Entrada ---
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["INGEST_BUCKET"]

    # --- Proceso ---
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        },
        'timestamp': datetime.utcnow().isoformat()
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Guardar JSON en S3 (estrategia "Ingesta Push")
    s3 = boto3.client('s3')
    file_name = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    print(f"Comentario guardado en S3: s3://{bucket_name}/{file_name}")

    # --- Salida ---
    return {
        'statusCode': 200,
        'body': json.dumps({
            'mensaje': 'Comentario guardado exitosamente',
            'comentario': comentario,
            's3_path': f"s3://{bucket_name}/{file_name}"
        })
    }
