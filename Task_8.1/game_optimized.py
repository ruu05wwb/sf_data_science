"""Игра угадай число.
Компьютер сам загадывает и угадывает число
"""
import numpy as np

def random_predict(number:int=1) -> int:
    """Рандомно угадываем число

    Args:
        number (int, optional): Загаданное число. Defaults to 1.

    Returns:
        int: Число попыток
    """   
    count = 0
    upper_border = 100 # Upper border of the range of predictions
    lower_border = 0  # Lower border of the range of predictions
    running_middle = int(upper_border/2) # Middle of the range of predictions.
    """
    On each iteration, range becomes more narrow, 
    
    and finally, running_middle results in the correct number
    """
    
    while True:
        count += 1

        if number > running_middle:
            lower_border = running_middle
            running_middle = round((upper_border+running_middle) / 2,0)
        elif number < running_middle:
            upper_border = running_middle
            running_middle = round(running_middle / 2, 0)
        else:            
            break # выход из цикла, если угадали
        
    return(count)

def score_game(random_predict) -> int:
    """За какое количество попыток в среднем из 1000 подходов угадывает наш алгоритм

    Args:
        random_predict ([type]): функция угадывания

    Returns:
        int: среднее количество попыток
    """

    count_ls = [] # список для сохранения количества попыток
    np.random.seed(1) # фиксируем сид для воспроизводимости
    random_array = np.random.randint(1, 101, size=(1000)) # загадали список чисел

    for number in random_array:
        count_ls.append(random_predict(number))

    score = int(np.mean(count_ls)) # находим среднее количество попыток

    print(f'Ваш алгоритм угадывает число в среднем за: {score} попыток')
    return(score)

# RUN
if __name__ == '__main__':
    score_game(random_predict)