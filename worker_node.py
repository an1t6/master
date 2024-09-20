import socket
import random
import time

MAX_SIZE = 10
SUCCESS_RATE = 0.8  # 80% 확률로 작업 성공
DELAY = {'min': 1, 'max': 3}  # 1~3초 랜덤 딜레이
ip_address = '127.0.0.1'
port_number = 8000

# 랜덤 딜레이 함수
def random_delay():
    return random.randint(DELAY['min'], DELAY['max'])

# 행렬 생성 함수
def create_matrix(size):
    return [[random.randint(0, 99) for _ in range(size)] for _ in range(size)]

def process_task(Mat1, Mat2, row, col):
    row_data = Mat1[row]
    col_data = [Mat2[i][col] for i in range(MAX_SIZE)]
    task_result = sum(row_data[i] * col_data[i] for i in range(MAX_SIZE))
    time_taken = random_delay()

    time.sleep(time_taken)  # 작업 지연

    success = random.random() < SUCCESS_RATE

    return task_result, success, time_taken

def main():
    Mat1 = create_matrix(MAX_SIZE)
    Mat2 = create_matrix(MAX_SIZE)

    # 소켓 클라이언트 설정
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 8080))

    while True:
        # 마스터로부터 작업 받기
        data = client_socket.recv(1024).decode()
        if not data:
            break
        row, col = map(int, data.split(','))

        # 작업 처리
        task_result, success, time_taken = process_task(Mat1, Mat2, row, col)

        # 마스터에게 결과 전송
        client_socket.sendall(f"{task_result},{success},{time_taken}".encode())

    # 소켓 닫기
    client_socket.close()

if __name__ == '__main__':
    main()
