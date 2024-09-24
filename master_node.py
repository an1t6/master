import random
import time
import logging
import pickle
import socket
import threading
from queue import Queue  # Queue 모듈 추가

SIZE = 10
WORKERS = 4
DELAY = {'min': 1, 'max': 3}  
ONE_SEC_DELAY = 1 
PORT = 8081

def set_logging(filename):
    logger = logging.getLogger(filename)
    handler = logging.FileHandler(filename, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

def log_print(logger, message):
    logger.info(message)
    print(message)

def random_sec():
    return random.randint(DELAY['min'], DELAY['max'])

def create_matrix(size):
    return [[random.randint(0, 99) for _ in range(size)] for _ in range(size)]

def manage_worker(worker_socket, worker_num, task_queue, Mat1, Mat2, result, temp_counts, total_times, lock, worker_loggers, master_logger):
    while not task_queue.empty():
        temp = None
        with lock:
            if not task_queue.empty():
                temp = task_queue.get() 
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
        time_taken = worker_response['time_taken']
        temp_description = f"[{row}, {col}]"
        logger = worker_loggers[worker_num]

        log_print(logger, f"작업 {temp_description}을 완료, 소요된 시간 {time_taken}초")

        # 행렬 곱 계산
        calculated_result = 0
        for k in range(SIZE):
            calculated_result += Mat1[row][k] * Mat2[k][col]

        # 로그 출력: "작업 [ x,y ] = [z]"
        log_print(master_logger, f"작업 {temp_description} = {calculated_result}")

        with lock:
            if success:
                result[row][col] = temp_result
                temp_counts[worker_num] += 1
                total_times[worker_num] += time_taken
            else:
                task_queue.put(temp)  # 실패 시 다시 큐에 작업 추가
                log_print(master_logger, f'워커 {worker_num + 1}이 행렬 [{temp["row"]}, {temp["col"]}] 처리를 실패, 재할당')
        
        time.sleep(ONE_SEC_DELAY)
        
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', PORT))
    server_socket.listen(WORKERS)
    
    Mat1 = create_matrix(SIZE)
    Mat2 = create_matrix(SIZE)
    result = [[0] * SIZE for _ in range(SIZE)]
    
    # 큐에 작업 추가
    task_queue = Queue()
    for i in range(SIZE):
        for j in range(SIZE):
            task_queue.put({'row': i, 'col': j})
    
    temp_counts = [0] * WORKERS
    start_time = time.time()
    total_times = [0] * WORKERS
    worker_threads = []
    lock = threading.Lock()
    
    master_logger = set_logging('master.txt')
    worker_loggers = [set_logging(f'worker{i + 1}.txt') for i in range(WORKERS)]
    print(f'워커의 접속을 기다리는 중...')

    for worker_num in range(WORKERS):
        worker_socket, addr = server_socket.accept()
        log_print(master_logger, f'워커 {worker_num + 1}이 연결됨 {addr}')
        print(f'워커 {worker_num + 1} 연결 성공 {addr}')
        t = threading.Thread(target=manage_worker, args=(worker_socket, worker_num, task_queue, Mat1, Mat2, result, temp_counts, total_times, lock, worker_loggers, master_logger))
        t.start()
        worker_threads.append(t)
        
    log_print(master_logger, f'{SIZE}x{SIZE} 크기의 행렬 2개를 {WORKERS}개의 워커로 연산 시작')
    
    for t in worker_threads:
        t.join()

    total_complete = sum(temp_counts)
    total_time = time.time() - start_time

    log_print(master_logger, f'총 작업 수 {total_complete}')
    log_print(master_logger, f'총 연산 수행 시간 {total_time} 초')
    for i in range(WORKERS):
        log_print(worker_loggers[i], f'worker {i + 1}이 처리한 작업 수 {temp_counts[i]}')
        log_print(worker_loggers[i], f'worker {i + 1}이 수행한 시간 {total_times[i]} 초')

    server_socket.close()

if __name__ == '__main__':
    main()
