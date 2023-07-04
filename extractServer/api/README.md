해당 directory에서 개발용 fastapi 서버를 구축하여 테스트 하는 방법
```
docker build -t fastapi:test .
```
해당 command로 fastpi 서버 커스텀 이미지 생성

```
docker run -it --restart=always -d -p 8899:3300 --name testapi fastapi:test
```
해당 command로 fastapi 서버를 컨테이너로 띄움

```
file tree는 다음과 같다.

.
├── app
│   ├── __init__.py
│   ├── config
│   │   └── config.ini
│   ├── main.py
│   ├── routers
│   │   ├── __init__.py
│   │   └── router_test.py
│   └── utils
│       ├── __init__.py
│       └── hdfs_func.py
├── dockerfile
├── get_stock_server.py
└── requirements.txt

```
