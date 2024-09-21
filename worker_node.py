import socket
import threading
import pickle
import random
import time

MAX_SIZE = 10
DELAY = {'min': 1, 'max': 3}  # 1~3초 랜덤 딜레이
SUCCESS_RATE = 0.8  # 80% 확률로 작업 성공
PORT = 8081

# 랜덤 딜레이 함수
def random_delay():
    return random.randint(DELAY['min'], DELAY['max'])

# 작업 처리 함수
def process_task(task, Mat1, Mat2):
    row, col = task['row'], task['col']
    row_data = Mat1[row]
    col_data = [Mat2[i][col] for i in range(MAX_SIZE)]
    task_result = sum(row_data[i] * col_data[i] for i in range(MAX_SIZE))
    time_taken = random_delay()
    time.sleep(time_taken)
    success = random.random() < SUCCESS_RATE

    return {
        'success': success,
        'result': task_result,
        'time_taken': time_taken
    }

# 워커 스레드 함수
def worker_thread(worker_id):
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    worker_socket.connect(('ec2-43-203-247-248.ap-northeast-2.compute.amazonaws.com', PORT))

    while True:
        # 마스터로부터 작업 수신
        task_data = pickle.loads(worker_socket.recv(4096))

        if not task_data:
            break

        task = task_data['task']
        Mat1 = task_data['Mat1']
        Mat2 = task_data['Mat2']

        # 작업 처리 및 결과 전송
        result = process_task(task, Mat1, Mat2)
        worker_socket.sendall(pickle.dumps(result))

    worker_socket.close()

# 메인 함수
def main():
    threads = []
    for worker_id in range(4):  # 4개의 워커 스레드 생성
        t = threading.Thread(target=worker_thread, args=(worker_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
