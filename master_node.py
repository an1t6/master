import random
import time
import logging
import pickle
import socket
import threading

SIZE = 10
WOKERS = 4
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

def manage_worker(worker_socket, worker_num, temps, Mat1, Mat2, result, temp_counts, total_times, lock, worker_loggers, master_logger):
    while temps:
        with lock:
            if temps:
                temp = temps.pop(0)
            else:
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
        
        log_print(logger, 
                    f"작업 {temp_description}을 완료, 소요된 시간 {time_taken}초")
        with lock:
            if success:
                result[row][col] = temp_result
                temp_counts[worker_num] += 1
                total_times[worker_num] += time_taken
            else:
                temps.append(temp)
                log_print(master_logger, f'워커 {worker_num + 1}이 행렬 [{temp["row"]}, {temp["col"]}] 처리를 실패, 재할당')
        
        time.sleep(ONE_SEC_DELAY)
        
def main():
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('ec2-43-203-247-248.ap-northeast-2.compute.amazonaws.com', PORT))
    server_socket.listen(WOKERS)
    
    Mat1 = create_matrix(SIZE)
    Mat2 = create_matrix(SIZE)
    result = [[0] * SIZE for _ in range(SIZE)]
    temps = [{'row': i, 'col': j} for i in range(SIZE) for j in range(SIZE)]
    temp_counts = [0] * WOKERS
    start_time = time.time()
    total_times = [0] * WOKERS
    worker_threads = []
    lock = threading.Lock()
    
    master_logger = set_logging('master.txt')
    worker_loggers = [set_logging(f'worker{i + 1}.txt') for i in range(WOKERS)]
    print(f'워커의 접속을 기다리는 중...')

    for worker_num in range(WOKERS):
        worker_socket, addr = server_socket.accept()
        log_print(master_logger, f'워커 {worker_num + 1}이 연결됨 {addr}')
        print(f'워커 {worker_num + 1} 연결 성공 {addr}')
        t = threading.Thread(target=manage_worker, args=(worker_socket, worker_num, temps, Mat1, Mat2, result, temp_counts, total_times, lock, worker_loggers, master_logger))
        t.start()
        worker_threads.append(t)
        
    log_print(master_logger, f'{SIZE}x{SIZE} 크기의 행렬 2개를 {WOKERS}개의 워커로 연산 시작')
    
    for t in worker_threads:
        t.join()

    total_complete = sum(temp_counts)
    total_time = time.time() - start_time

    log_print(master_logger, f'총 작업 수 {total_complete}')
    log_print(master_logger, f'총 연산 수행 시간 {total_time} 초')
    for i in range(WOKERS):
        log_print(worker_loggers[i], f'worker {i + 1}이 처리한 작업 수 {temp_counts[i]}')
        log_print(worker_loggers[i], f'worker {i + 1}이 수행한 시간 {total_times[i]} 초')

    server_socket.close()

if __name__ == '__main__':
    main()