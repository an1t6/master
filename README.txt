Master node와 Worker node 4개를 소켓 통신으로 구현하여 1000x1000 행렬을 계산하는 프로그램

이벤트 

1. 80% 확률로 성공, 20% 확률로 실패
2. 실패 시 다른 워커로 재할당하여 연산

조건

1. master node는 aws 등의 외부 서버에 구현되어야 한다.
2. master node와 worker node는 소켓 통신으로 구현되어야 한다
3. worker node는 스레드로 동작한다.