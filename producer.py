# producer.py

import json
import uuid
from flask import Flask, request, jsonify
import pika
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, QUEUE_NAME

app = Flask(__name__)

def get_rabbitmq_connection():
    """Устанавливает соединение с RabbitMQ."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)

@app.route('/generate_report', methods=['POST'])
def generate_report():
    """Принимает задачу на генерацию отчета и отправляет в очередь."""
    data = request.get_json()
    if not data or 'report_type' not in data or 'user_email' not in data:
        return jsonify({'error': 'Missing report_type or user_email'}), 400

    task_id = str(uuid.uuid4())
    message = {
        'task_id': task_id,
        'report_type': data['report_type'],
        'user_email': data['user_email']
    }

    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Создаём очередь, если её нет (durable - переживёт перезагрузку RabbitMQ)
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Публикуем сообщение
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # сообщение сохраняется на диск
            )
        )
        connection.close()

        return jsonify({
            'status': 'accepted',
            'task_id': task_id,
            'message': 'Report generation task has been queued.'
        }), 202

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Эндпоинт для проверки работоспособности API."""
    return jsonify({'status': 'ok'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
