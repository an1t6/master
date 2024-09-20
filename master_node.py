import socket
import threading
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor

MAX_SIZE = 10
WORKER_NUM = 4
DELAY = {'min': 1, 'max': 3}  # 1~3초 랜덤 딜레이
SUCCESS_RATE = 0.8  # 80% 확률로 작업 성공
COMMUNICATION_DELAY = 1  # 통신 지연 1초
ip_address = '127.0.0.1'
port_number = 8000

def setup_logging(filename):
    logger = logging.getLogger(filename)
    handler = logging.FileHandler(filename, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

def log_message(logger, message):
    logger.info(message)

# 행렬 생성 함수
def create_matrix(size):
    return [[random.randint(0, 99) for _ in range(size)] for _ in range(size)]

def handle_worker_connection(worker_socket, worker_id, tasks, result, task_counts, total_times, lock, worker_loggers):
    while tasks:
        task = tasks.pop(0) if tasks else None
        if task is None:
            break
        
        # 작업을 워커에게 전송
        worker_socket.sendall(f"{task['row']},{task['col']}".encode())

        # 워커로부터 결과 받기
        data = worker_socket.recv(1024).decode()
        task_result, success, time_taken = data.split(',')
        task_result = int(task_result)
        success = success == 'True'
        time_taken = int(time_taken)

        with lock:
            if success:
                row, col = task['row'], task['col']
                result[row][col] = task_result
                task_counts[worker_id] += 1
                total_times[worker_id] += time_taken
                log_message(worker_loggers[worker_id], f"작업 [{row}, {col}]을 완료, 소요된 시간 {time_taken}초")
            else:
                log_message(worker_loggers[worker_id], f"작업 [{row}, {col}] 실패, 재할당 필요")
                tasks.append(task)  # 작업 실패 시 다시 재할당

# 메인 함수
def main():
    Mat1 = create_matrix(MAX_SIZE)
    Mat2 = create_matrix(MAX_SIZE)
    result = [[0] * MAX_SIZE for _ in range(MAX_SIZE)]
    tasks = [{'row': i, 'col': j} for i in range(MAX_SIZE) for j in range(MAX_SIZE)]
    task_counts = [0] * WORKER_NUM
    total_times = [0] * WORKER_NUM
    lock = threading.Lock()

    # 로그 설정
    master_logger = setup_logging('master.txt')
    worker_loggers = [setup_logging(f'worker{i + 1}.txt') for i in range(WORKER_NUM)]

    log_message(master_logger, f'{MAX_SIZE}x{MAX_SIZE} 크기의 행렬 2개의 곱 연산을 {WORKER_NUM}개의 워커를 생성하여 시작합니다.')

    # 소켓 서버 설정
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 8080))
    server_socket.listen(WORKER_NUM)

    log_message(master_logger, "워커 노드들의 연결을 기다립니다...")

    start_time = time.time()
    worker_sockets = []
    worker_threads = []

    with ThreadPoolExecutor(max_workers=WORKER_NUM) as executor:
        # 워커 노드들과 연결 대기
        for worker_id in range(WORKER_NUM):
            worker_socket, _ = server_socket.accept()
            worker_sockets.append(worker_socket)
            log_message(master_logger, f"워커 {worker_id + 1}가 연결되었습니다.")
            # 워커 처리 스레드 시작
            worker_thread = executor.submit(handle_worker_connection, worker_socket, worker_id, tasks, result, task_counts, total_times, lock, worker_loggers)
            worker_threads.append(worker_thread)

        # 워커 처리 완료 후 소켓 닫기
        for worker_thread in worker_threads:
            worker_thread.result()  # 결과 기다림

        # 총 작업 시간 계산
        total_time = time.time() - start_time
        log_message(master_logger, f'총 연산 수행 시간 {total_time} 초')
        for i in range(WORKER_NUM):
            log_message(worker_loggers[i], f'worker {i + 1}이 처리한 작업 수 : {task_counts[i]}')
            log_message(worker_loggers[i], f'worker {i + 1}이 수행한 시간 : {total_times[i]} 초')

        # 모든 워커 소켓 닫기
        for worker_socket in worker_sockets:
            worker_socket.close()

if __name__ == '__main__':
    main()
