import requests
import time

# Параметры подключения к Management API RabbitMQ
RABBITMQ_HOST = "rabbitmq"          # Имя сервиса в docker-compose (доступно по сети)
API_URL = f"http://{RABBITMQ_HOST}:15672/api"
USER = "admin"
PASS = "strongpassword123"

# Список очередей, которые нужно создать
QUEUES = ["orders", "notifications", "logs"]

def create_queue(queue_name):
    """
    Создаёт durable очередь с заданным именем через HTTP API RabbitMQ.
    """
    url = f"{API_URL}/queues/%2F/{queue_name}"   # %2F — это кодированный слэш для vhost "/"
    data = {
        "durable": True,      # очередь переживёт перезагрузку RabbitMQ
        "auto_delete": False  # не удаляется при отключении всех потребителей
    }
    response = requests.put(url, auth=(USER, PASS), json=data)
    if response.status_code == 201:
        print(f"Queue '{queue_name}' created successfully.")
    elif response.status_code == 204:
        print(f"Queue '{queue_name}' already exists.")
    else:
        print(f"Failed to create queue '{queue_name}': {response.status_code} - {response.text}")

if __name__ == "__main__":
    # Небольшая пауза, чтобы RabbitMQ точно инициализировался
    # (healthcheck уже гарантирует готовность портов, но API может стартовать чуть позже)
    print("Waiting for RabbitMQ API to be fully ready...")
    time.sleep(10)

    print("Starting queue creation...")
    for q in QUEUES:
        create_queue(q)
    print("Queue creation script finished.")
