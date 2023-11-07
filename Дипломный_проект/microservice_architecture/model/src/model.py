import pika
import pickle
import numpy as np
import json
from statistics import mean 
 
# Читаем файл с сериализованной моделью
with open('my_pipeline.pkl', 'rb') as pkl_file:
    pipe = pickle.load(pkl_file)
 
try:
    # Создаём подключение по адресу rabbitmq:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
 
    # Объявляем очередь features
    channel.queue_declare(queue='features')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
    
    x_features = ['status', 'propertyType', 'street', 'baths', 'homeFacts', 'fireplace',
       'city', 'schools', 'sqft', 'zipcode', 'beds', 'state', 'stories',
       'PrivatePool']
    oh_coding_features = ['propertyType', 'stories']
    bin_coding_features = ['status', 'zipcode']
    
#Функция для первичной обработки вектора признаков
    def prep_data(df):
        def remove_non_num(lst):
        res = []
        if type(lst) == list:
            for elem in lst:
                try: res.append(float(elem))
                except: pass            
        return res
        df['PrivatePool'] = df['PrivatePool']\
                             .combine_first(df['private pool'])\
                             .map({'Yes': '1', 'yes': '1'})\
                             .fillna('0')\
                             .astype(int)

       for elem in range(0,7):
           field_name = ast.literal_eval(df['homeFacts'].iloc[0])['atAGlanceFacts'][elem]['factLabel']
           df[field_name] = df['homeFacts'].apply(lambda x: ast.literal_eval(x)['atAGlanceFacts'][elem]['factValue'])
           df['sch_rating'] = df['schools'].apply(lambda x: ast.literal_eval(x)[0]['rating'])
           df['sch_distance'] = df['schools'].apply(lambda x: ast.literal_eval(x)[0]['data']['Distance'])
           df.sch_rating = df.sch_rating.apply(lambda x: [i.replace('/10', '') for i in x] if len(x)>0 else np.nan)
           df['sch_rating'] = df.sch_rating.apply(remove_non_num)\
                                      .apply(lambda x: mean(x) if len(x)>0 else np.nan)
          df['sch_distance'] = df['sch_distance'].apply(lambda x: [i.replace('mi', '') for i in x] if len(x)>0 else np.nan)
          df['sch_distance'] = df['sch_distance'].apply(lambda x: min(x) if type(x)==list else x).astype(float)
      df = df.drop(['private pool','mls-id','MlsId','homeFacts','Price/sqft','Remodeled year','Heating','Cooling','Parking','lotsize','fireplace','street', 'city',            'state','beds','schools'], axis=1)
      df['sqft'].apply(lambda x: ''.join(re.findall(r'\d+', str(x)))).replace('',np.nan).astype(float)
      if df['sqft'].apply(lambda x: 1 if '.' in str(x) else 0).sum() == 0:
           df[field] = df[field].apply(lambda x: ''.join(re.findall(r'\d+', str(x))))\
                                                   .replace('',np.nan)\
                                                   .astype(float)   
      df.baths = df.baths.str.replace('[a-zA-Z]','', regex=True)\
                             .str.replace(' ','', regex=True)\
                             .str.replace(':','', regex=True)\
                             .str.replace(',','.', regex=True)\
                             .str.replace('~','', regex=True)\
                             .str.replace('\.\.','', regex=True)\
                             .str.replace('--','', regex=True)\
                             .str.replace('—','', regex=True)\
                             .str.replace('^\s*$','', regex=True)\
                             .str.replace('1-0/1-0/1','', regex=True)\
                             .str.replace('116/116/116','', regex=True)\
                             .str.replace('3-1/2-2','', regex=True)\
                             .str.replace('1/1/1/1','', regex=True)\
                             .str.replace('2-1/2-1/1-1/1-1','', regex=True)\
                             .str.replace('1/1-0/1-0/1-0','', regex=True)\
                             .str.replace('.*unknown.*','', regex=True)\
                             .str.replace('1-2','', regex=True)\
                             .str.replace('+','')\
                             .str.replace('/-0','')\
                             .str.replace('/0','')\
                             .replace('',np.nan, regex=True)\
                             .astype(float)
      df['Year built'] = df['Year built'].str.replace("'",'', regex=True)
      df['Year built'] = df['Year built'].str.replace('^\s*$','None', regex=True)
      df['Year built'] = df['Year built'].astype(float)
      df['zipcode'] = df['zipcode'].str.replace("-.+",'', regex=True) 
      df.stories = df.stories.fillna('0')
      return df




    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        print(f'Получен вектор признаков {body}')
        features = json.loads(body)
        features_df = pd.DataFrame(np.array(features).reshape(1, -1), columns = x_features)
        data = prep_data(features_df)
        data_onehot = loaded_pipe[0].transform(data[oh_coding_features])
        column_names = loaded_pipe[0].get_feature_names_out(oh_coding_features)
        data_onehot = pd.DataFrame(data_onehot.todense(), columns=column_names)
        data = pd.concat([data.reset_index(drop=True).drop(oh_coding_features, axis=1), data_onehot], axis=1)

        type_bin = loaded_pipe[1].transform(data[bin_coding_features])
        data = pd.concat([data.reset_index(drop=True).drop(bin_coding_features, axis=1), type_bin], axis=1)

        X_scaled = loaded_pipe[2].transform(data)

        y_pred_log = pipe[3].predict(X_scaled)
        y_pred = np.exp(y_pred_log)-1


        channel.basic_publish(exchange='',
                        routing_key='y_pred',
                        body=json.dumps(y_pred[0]))
        print(f'Предсказание {y_pred[0]} отправлено в очередь y_pred')
 
    # Извлекаем сообщение из очереди features
    # on_message_callback показывает, какую функцию вызвать при получении сообщения
    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
 
    # Запускаем режим ожидания прихода сообщений
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')