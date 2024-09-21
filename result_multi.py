import threading
import random
import time
import logging

MAX_SIZE = 10
WORKER_NUM = 4
DELAY = {'min': 1, 'max': 3}  # 1~3초 랜덤 딜레이
SUCCESS_RATE = 0.8  # 80% 확률로 작업 성공
COMMUNICATION_DELAY = 1  # 통신 지연 1초

# 로그 설정
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

# 랜덤 딜레이 함수
def random_delay():
    return random.randint(DELAY['min'], DELAY['max'])

def create_matrix(size):
    return [[random.randint(0, 99) for _ in range(size)] for _ in range(size)]

# 작업 처리 함수
def process_task(worker_id, Mat1, Mat2, task, reassigned, result, task_counts, total_times, lock, worker_loggers, master_logger):
    row, col = task['row'], task['col']
    row_data = Mat1[row]
    col_data = [Mat2[i][col] for i in range(MAX_SIZE)]
    task_result = sum(row_data[i] * col_data[i] for i in range(MAX_SIZE))
    time_taken = random_delay()
    
    time.sleep(time_taken)  # 작업 지연
    
    success = random.random() < SUCCESS_RATE

    task_description = f"[{row}, {col}]"
    logger = worker_loggers[worker_id]
    log_message(logger, 
                f"재할당된 작업 {task_description}을 완료, 소요된 시간 {time_taken}초" if reassigned else 
                f"작업 {task_description}을 완료, 소요된 시간 {time_taken}초")

    with lock:
        if success:
            result[row][col] = task_result
            task_counts[worker_id] += 1
            total_times[worker_id] += time_taken
        else:
            return False  # 작업 실패

    return True  # 작업 성공

# 워커 스레드 함수
def worker_thread(worker_id, tasks, Mat1, Mat2, result, task_counts, total_times, lock, worker_loggers, master_logger):
    while tasks:
        with lock:
            if tasks:
                task = tasks.pop(0)
            else:
                break

        # 작업 처리
        success = process_task(worker_id, Mat1, Mat2, task, False, result, task_counts, total_times, lock, worker_loggers, master_logger)

        if not success:
            # 작업 실패 시 다시 할당 및 master 로그 기록
            with lock:
                tasks.append(task)
                next_worker_id = (worker_id + 1) % WORKER_NUM
                log_message(master_logger, f'워커 {worker_id + 1}이 행렬 [{task["row"]}, {task["col"]}] 처리를 실패, 워커 {next_worker_id + 1}로 재할당')

        time.sleep(COMMUNICATION_DELAY)  # 통신 지연

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

    start_time = time.time()

    # 스레드 생성 및 실행
    worker_threads = []
    for worker_id in range(WORKER_NUM):
        t = threading.Thread(target=worker_thread, args=(worker_id, tasks, Mat1, Mat2, result, task_counts, total_times, lock, worker_loggers, master_logger))
        t.start()
        worker_threads.append(t)

    # 모든 스레드 종료 대기
    for t in worker_threads:
        t.join()

    total_time = time.time() - start_time

    # 총 작업 수 계산
    total_tasks_completed = sum(task_counts)

    # 로그 기록
    log_message(master_logger, f'총 작업 수 {total_tasks_completed}')
    log_message(master_logger, f'총 연산 수행 시간 {total_time} 초')
    for i in range(WORKER_NUM):
        log_message(worker_loggers[i], f'worker {i + 1}이 처리한 작업 수 {task_counts[i]}')
        log_message(worker_loggers[i], f'worker {i + 1}이 수행한 시간 {total_times[i]} 초')

if __name__ == '__main__':
    main()
