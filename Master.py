import random
import time
import logging
import pickle
import socket
import threading
from queue import Queue 

SIZE = 10
WORKERS = 4
DELAY = {'min': 1, 'max': 3}
ONE_SEC_DELAY = 1
PORT = 8081

def set_logging(file):
    logger = logging.getLogger(file)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file, mode='w', encoding='utf-8')
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    return logger

def log_print(log, text):
    log.info(text)

def random_sec():
    return random.randint(DELAY['min'], DELAY['max'])

def create_matrix(size):
    return [[random.randint(0, 99) for _ in range(size)] for _ in range(size)]

def manage_worker(worker_socket, worker_num, temps_queue, Mat1, Mat2, result, count, total_times, lock, worker_log, master_log):
    while True: 
        temp = None
        with lock:
            if not temps_queue.empty():
                temp = temps_queue.get()  
            else:
                break
        if temp is None:
            break
        
        temp_data = {'temp': temp, 'Mat1': Mat1, 'Mat2': Mat2}
        worker_socket.sendall(pickle.dumps(temp_data)) 
        worker_response = pickle.loads(worker_socket.recv(4096))  
        success = worker_response['success']
        temp_result = worker_response['result']
        row, col = temp['row'], temp['col']
        run_time = worker_response['run_time']
        current_location = f"[{row}, {col}]"
        worker_logger = worker_log[worker_num]
        with lock:
            if success:
                result[row][col] = temp_result
                count[worker_num] += 1
                total_times[worker_num] += run_time
                log_print(master_log, f'워커 {worker_num + 1} : {current_location} = {temp_result}')
                log_print(worker_logger, f'연산 성공: {current_location}, 소요된 시간 {run_time}초')
                print(f'작업 {current_location} 성공')
            else:
                temps_queue.put(temp)
                log_print(master_log, f'워커 {worker_num + 1} : {current_location} 연산 실패, 재할당...')
                log_print(worker_logger, f'연산 실패: {current_location}')
                print(f'작업 {current_location} 실패')

        time.sleep(ONE_SEC_DELAY)

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('ec2-43-203-247-248.ap-northeast-2.compute.amazonaws.com', PORT))
    server.listen(WORKERS)

    Mat1 = create_matrix(SIZE)
    Mat2 = create_matrix(SIZE)
    result = [[0] * SIZE for _ in range(SIZE)]
    temps_queue = Queue()
    for i in range(SIZE):
        for j in range(SIZE):
            temps_queue.put({'row': i, 'col': j})
    count = [0] * WORKERS
    start_time = time.time()
    total_times = [0] * WORKERS
    worker_threads = []
    master_log = set_logging('master.txt')
    worker_log = [set_logging(f'worker{i + 1}.txt') for i in range(WORKERS)]
    lock = threading.Lock()
    
    print(f'워커의 접속을 기다리는 중...')
    
    for worker_num in range(WORKERS):
        worker_socket, addr = server.accept()
        log_print(master_log, f'워커 {worker_num + 1} 연결됨: {addr}')
        print(f'워커 {worker_num + 1} 연결 성공: {addr}')
        t = threading.Thread(target=manage_worker, args=(worker_socket, worker_num, temps_queue, Mat1, Mat2, result, count, total_times, lock, worker_log, master_log))
        t.start()
        worker_threads.append(t)
        
    log_print(master_log, f'{SIZE}x{SIZE} 크기의 행렬 2개를 {WORKERS}개의 워커로 연산 시작')
    for t in worker_threads:
        t.join()
        
    total_complete = sum(count)
    total_time = round(time.time() - start_time)
    log_print(master_log, f'총 작업 수: {total_complete}')
    log_print(master_log, f'총 연산 수행 시간: {total_time}초')
    for i in range(WORKERS):
        log_print(worker_log[i], f'워커 {i + 1}이 처리한 작업 수: {count[i]}')
        log_print(worker_log[i], f'워커 {i + 1}이 수행한 시간: {total_times[i]}초')

    server.close()

if __name__ == '__main__':
    main()
