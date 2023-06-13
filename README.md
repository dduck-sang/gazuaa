# 주식 데이터 파이프라인 구축

Project overview

해당 프로젝트는 코스피/코스닥 및 각종 금융 지표를 수집하여, dashboard 구축 및 api serving이 목적인 프로젝트입니다. \
수집한 다양한 금융정보 data를 적재하여, 주가정보 dashboard 및 의사결정에 도움이 되는 다양한 지표를 구현해봅니다. \
적재된 data를 외부로 부터 제공하는 api 를 빌드 해보는 걸 목표로 가지고있습니다.

---

**컨텐츠 목차**

- [Project 구조](#project-structure)
- [개발환경](#requirements)
- [dag 구조](#dag-structure)
- 
---

## Project Structure

히히히 

---

## Requirements

gazuaa는 다음과 같은 환경에서 테스트 하였습니다 :

|             | Main version (dev)           |
|-------------|------------------------------|
| Python      | 3.8                          |
| Platform    | AMD64/ARM64                  | 
| Airflow     | 2.6.1                        |
| PostgreSQL  | 12                           |
| MySQL       | 5.7, 8                       | 
| Hadoop      | 3.3.4                        |

* note : 개발환경 기입하시오

* note : 통합 개발환경 링크 \
      -> airflow : https://github.com/dduck-sang/gazuaa/issues/1#issue-1744926153 \
      -> vim & zsh : https://github.com/dduck-sang/gazuaa/issues/2#issue-1744957934


## Dag Structure

gazuaa가 수집하는 data flow는 다음과 같습니다.

```
. 
└──dags 
      ├── kind.dag 
      ├── base_info.dag 
      ├── marketPrice.dag 
      ├── currency.dag 
      └── 기타 등등 
```
<h5> kind.dag </h5>
v0.0.1 : kospi 종목번호 ticker num. 으로 처리 \
[pic]

<img width="1128" alt="Screen Shot 2023-06-08 at 11 15 23 AM" src="https://github.com/dduck-sang/gazuaa/assets/23203791/ab2b4dea-4620-4139-8993-816fc7be263c">

** base_info.dag 

[ 사진 ]

airflow port : 1111
