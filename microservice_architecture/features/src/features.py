import pika
import numpy as np
import json
import time
from datetime import datetime
from sklearn.datasets import load_diabetes

while True:
    try:
        # загружаем датасет и генерируем уникальный идентификатор
        X, y = load_diabetes(return_X_y=True)
        random_row = np.random.randint(0, X.shape[0]-1)
        message_id = datetime.timestamp(datetime.now())

        # создаем подключение к Rabbitmq
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # создаем очередь y_true и features
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # формируем сообщение с ID
        message_y_true = {'id': message_id, 'body': y[random_row]}
        message_features = {'id': message_id, 'body': list(X[random_row])}

        # публикуем сообщения в соответствующую очередь
        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps(message_y_true))
        channel.basic_publish(exchange='', routing_key='features', body=json.dumps(message_features))

        print(f"Сообщение {message_id} отправлено.")
        # закрываем подключение
        connection.close()
        # добавляем задержку 10 сек
        time.sleep(10)

    except Exception as e:
        print(f"Ошибка: {e}")
        time.sleep(10)
