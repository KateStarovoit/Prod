import json
import requests
import datetime
import sys
from random import random
import itertools


def triple_encoding(strng, n):
    lst = []
    count = -1
    for i in strng:
        count += 1
        if i == " ":
            lst.append(count)
    for k in range(n):
        strng = strng.replace(" ", "")
        strng = strng.split()
        strng = strng[0][len(strng) - n - 1:] + strng[0][:len(strng) - n - 1]
        for i in range(len(lst)):
            strng = strng[:lst[i]] + " " + strng[lst[i]:]
        strng = strng.split(" ")
        for i in range(len(strng)):
            for j in range(n):
                strng[i] = strng[i][-1] + strng[i][:-1]
        result = ""
        for i in range(len(strng)):
            if i == len(strng) - 1:
                result += strng[i]
            else:
                result += strng[i] + " "
        strng = result
    x = random()
    # print(strng)
    strng = str(x)[:10] + strng[:]
    # print(strng)
    count = 1
    mem = ""
    lst = []
    returned_str = ""
    for i in range(len(strng)):
        if count == n:
            lst.append((strng[i], count))
            count -= 1
            mem = "-"
        elif count == 1:
            lst.append((strng[i], count))
            count += 1
            mem = "+"
        elif mem == "+":
            lst.append((strng[i], count))
            count += 1
        elif mem == "-":
            lst.append((strng[i], count))
            count -= 1

    def takeSecond(elem):
        return elem[1]

    lst.sort(key=takeSecond)
    for i in lst:
        returned_str += i[0]
    coding_word = "drebotiy"
    keytext = ""

    if len(keytext) >= n:
        keytext = coding_word;
    else:
        while True:
            if len(keytext) < n:
                keytext = keytext + coding_word
                continue
            elif len(keytext) >= n:
                break

    alp = 'abcdefghijklmnopqrstuvwxyz.,@/ABCDEFGHIJKLMNOPQRSTUVWXYZ~`!#$%^&*() _+-=:.<>?/1234567890'
    f = lambda arg: alp[(alp.index(arg[0]) + alp.index(arg[1]) % 88) % 88]
    return ''.join(map(f, zip(returned_str, itertools.cycle(keytext))))



class Producer:

    def __init__(self, address, count_of_retries, time_of_expectations):
        self.address = address
        self.count_of_retries = count_of_retries
        self.time_of_expectations = time_of_expectations

    def get_statistics(self):

        response = requests.get('http://127.0.0.1//..../')
        for i in range(self.count_of_retries):
            today_time = datetime.datetime.today()

            with open('get_statistics.json', 'w+') as f:
                content = requests.get("http://" + (self.address) + ":5000/get_statistic/")

            for node_stat in content:
                f.write(json.dumps(node_stat + '\n' + '{'))
                for stat in content[node_stat]:
                    f.write("{} {}".format(stat, content[node_stat][stat]))
                f.write('}')

            f.write(json.dumps(today_time))
            if content.status_code == 200:
                break
        content = json.dumps(content)
        return content,today_time

    def send(self, queue_name, message, key):

        returned_encoded_message = triple_encoding(message, key)
        # передаємо повідомлення та ключ для розкодування
        for i in range(self.count_of_retries):
            response = requests.post('http://127.0.0.1//statisitcs/',
                                 json={'queue_name': queue_name, 'message': returned_encoded_message, 'key': key})
            if response.status_code == 200:
                break
        y = json.loads(response)
        
        type_of_balancing = requests.get('http://127.0.0.1:/')

        # if type_of_balancing == 'memory':
        #     min_value = sys.float_info.max
        #     for keys in y.keys():
        #         load = y[keys]['write_message_duration'] - y[keys]['read_message_duration']
        #         if load < min_value:
        #             min_value = load
        #             min_index = keys
        #     result = requests.post('http://127.0.0.1//statisitcs/', json={'queue_name': min_index})
        min_index=0

        if type_of_balancing == 'num_of_messages':
            min_value = sys.float_info.max
            for keys in y.keys():
                if queue_name in y[keys]:
                    load = y[keys][queue_name]

                if load < min_value:
                    min_value = load
                    min_index = keys
            else:
                raise Exception


        for i in range(self.count_of_retries):
            port = requests.get("http://" + self.address + ":5000/get_port")
            if port.status_code==200:
                break


        for i in range(self.count_of_retries):
            result = requests.post("http://" + self.address+str(port) + ":5000/write_message",json={'queue_name': min_index,'message':message})
            if port.status_code==200:
                break

    def create_queue(self, queue_name):

        for i in range(self.count_of_retries):
            response = requests.post("http://" + self.address + ":/create_data_node/",
                                     json={"queue_name": queue_name})
            if response.status_code == 200:
                break

    def delete_queue(self, queue_name):

        for i in range(self.count_of_retries):
            response = requests.post("http://127.0.0.1:/delete_queue/", json={"queue_name": queue_name})
            if response.status_code == 200:
                break
