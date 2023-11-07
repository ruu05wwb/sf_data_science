import pika
import pandas as pd
import numpy as np
import json
 
# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет, будем использовать рандомные строки из него в качестве тестовых данных для построения предсказания
        df = pd.read_csv('data'csv')
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, df.shape[0]-1)
 
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
 
        # Создаём очередь features
        channel.queue_declare(queue='features')
 
        # Публикуем сообщение в очередь features
        channel.basic_publish(exchange='',
                            routing_key='features',
                            body=json.dumps(list(df.drop('target', axis=1)[random_row])))
        print('Сообщение с вектором признаков отправлено в очередь')
 
        # Закрываем подключение
        connection.close()
    except:
        print('Не удалось подключиться к очереди')