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
    
    run_time = random_sec()  # Simulate a random delay
    time.sleep(run_time)     # Delay to mimic processing time
    
    # 80% chance of success for each computation
    success = random.random() < 0.8
    
    return {'success': success, 'result': temp_result, 'run_time': run_time}

def worker_thread(worker_num):
        worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        worker_socket.connect(('127.0.0.1', PORT))

        while True:
            # Receive task from the master
            temp_data = pickle.loads(worker_socket.recv(4096))
            if not temp_data:  # No data received, means the job is done
                break
            
            temp = temp_data['temp']
            Mat1 = temp_data['Mat1']
            Mat2 = temp_data['Mat2']
            
            # Perform matrix multiplication on the received task
            result = run_temp(temp, Mat1, Mat2)
            
            # Send result back to master
            worker_socket.sendall(pickle.dumps(result))

def main():
    threads = []
    for worker_num in range(4):
        t = threading.Thread(target=worker_thread, args=(worker_num,))
        t.start()
        threads.append(t)
    
    # Wait for all threads to finish
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
