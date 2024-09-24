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
    
    run_time = random_sec()  # 임의의 지연 시간 설정
    time.sleep(run_time)     # 처리 시간을 시뮬레이션하기 위한 대기
    
    # 80% 확률로 성공 처리
    success = random.random() < 0.8
    
    return {'success': success, 'result': temp_result, 'run_time': run_time}

def worker_thread(worker_num):
    try:
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.connect(('127.0.0.1', PORT))

        while True:
            # 마스터로부터 작업 받기
            temp_data = pickle.loads(worker_socket.recv(4096))
            if not temp_data:  # 데이터가 없으면 작업 완료
                break
            
            temp = temp_data['temp']
            Mat1 = temp_data['Mat1']
            Mat2 = temp_data['Mat2']
            
            # 받은 작업에 대한 행렬 곱셈 처리
            result = run_temp(temp, Mat1, Mat2)
            
            # 결과를 마스터에게 다시 전송
            worker_socket.sendall(pickle.dumps(result))
            
    except Exception as e:
        print(f"Worker {worker_num}에서 오류 발생: {e}")
    finally:
        worker_socket.close()

def main():
    threads = []
    for worker_num in range(4):
        t = threading.Thread(target=worker_thread, args=(worker_num,))
        t.start()
        threads.append(t)
    
    # 모든 쓰레드가 작업을 끝낼 때까지 대기
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
