# consumer.py

import json
import time
import pika
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, QUEUE_NAME

def process_task(ch, method, properties, body):
    """Обработчик сообщения из очереди."""
    task = json.loads(body)
    print(f"[x] Received task: {task['task_id']}")
    print(f"    Type: {task['report_type']}")
    print(f"    Email: {task['user_email']}")

    # Имитация тяжёлой работы
    print(f"[ ] Generating report...")
    time.sleep(5)

    print(f"[✓] Task {task['task_id']} completed.\n")

    # Подтверждаем успешную обработку (удаляем сообщение из очереди)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Создаём очередь, если её нет
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Не даём воркеру взять больше одной задачи за раз
    channel.basic_qos(prefetch_count=1)

    # Подписываемся на очередь
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_task)

    print('[*] Waiting for messages. To exit press CTRL+C')
    print('=' * 50)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('\n[!] Interrupted. Shutting down...')
        channel.stop_consuming()

    connection.close()

if __name__ == '__main__':
    main()
