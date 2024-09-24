import random
import time
import pickle
import socket
import threading

SIZE = 10
DELAY = {'min': 1, 'max': 3}
PORT = 8081

def random_sec():
    return random.randint(DELAY['min'], DELAY['max'])

def run_temp(temp, Mat1, Mat2):
    row, col = temp['row'], temp['col']
    temp_row = Mat1[row]
    temp_col = [Mat2[i][col] for i in range(SIZE)]
    temp_result = sum(temp_row[i] * temp_col[i] for i in range(SIZE))
    run_time = random_sec() 
    success = random.random() < 0.8
    
    return {'success': success, 'result': temp_result, 'run_time': run_time}

def worker_thread(worker_num):
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.connect(('ec2-43-203-247-248.ap-northeast-2.compute.amazonaws.com', PORT))

        while True:
            temp_data = pickle.loads(worker_socket.recv(4096))
            if not temp_data:
                break
            temp = temp_data['temp']
            Mat1 = temp_data['Mat1']
            Mat2 = temp_data['Mat2']
            result = run_temp(temp, Mat1, Mat2)
            worker_socket.sendall(pickle.dumps(result))

def main():
    threads = []
    for worker_num in range(4):
        t = threading.Thread(target=worker_thread, args=(worker_num,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
