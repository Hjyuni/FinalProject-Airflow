import os
import requests
import pendulum
import pandas as pd
from anyio import TASK_STATUS_IGNORED
from numpy import apply_over_axes
from bs4 import BeautifulSoup as bs
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from mod.slackbot import Slack

# 서울 시간대로 맞추기
kst = pendulum.timezone('Asia/Seoul')
sm = Slack('#msg')

"""
depends_on_past: 이전 날짜의 task 인스턴스 중에서 동일한 task인스턴스가 실패한 경우 실행되지 않고 대기
wait_for_downstream: 이전 날짜의 task 인스턴스 중에서 동일한 task 인스턴스 중 하나라도 실패한 경우 해당 DAG는 실행되지 않고 대기
"""

# 기본 인수
default_args = {
    "owner" : "admin",
    "depends_on_past" : False,
    "wait_for_downstream" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}

dag = DAG(
    dag_id='s_Airflow',
    default_args=default_args,
    # 매일 오전 1시부터 2시간씩 가져오기
    schedule_interval='0 1-23/2 * * *',
    start_date=datetime(2022, 5, 10, tzinfo=kst),
    end_date=datetime(2022, 5, 14, tzinfo=kst),
    catchup=False
)

def getSeoul():
    """서울 식당 API"""
    try:
        # 재정
        """
        import json
        import requests

        # location = './'
        location = '/home/ubuntu/git/Final_Team04/data/apiSeoul/'
        key = '754542594e6d6a6a34334368754b4b'

        # 총 개수와 응답받은 json key이름 반환
        def getCountList():
            url = f'http://openapi.seoul.go.kr:8088/{key}/json/LOCALDATA_072404/1/1/'

            response = requests.get(url)
            count_check = json.loads(response.text)
            res_keys = list(count_check.keys())         # LOCALDATA_072404
            loc_keys = list(count_check[res_keys[0]])   # list_total_count, RESULT, row
            total_count = count_check[res_keys[0]][loc_keys[0]]
            print(total_count)
            count_list = list(range(0, total_count, 1000))
            count_list.append(total_count)

            api_dict = dict()
            api_dict['res_keys'] = res_keys
            api_dict['loc_keys'] = loc_keys
            api_dict['count_list'] = count_list

            return api_dict


        # 파일 생성
        def createFile(api_dict):
            dict_keys = list(api_dict.keys())         # res_keys, loc_keys, count_list

            seoul_list = list()
            cnt = 0

            for i in range(1, len(api_dict[dict_keys[2]])):
                pre, end = api_dict[dict_keys[2]][i-1]+1, api_dict[dict_keys[2]][i]
                url = f'http://openapi.seoul.go.kr:8088/{key}/json/LOCALDATA_072404/{pre}/{end}/'
                response = requests.get(url)

                json_file = json.loads(response.text)
                row_list = json_file[api_dict[dict_keys[0]][0]][api_dict[dict_keys[1]][2]]

                seoul_list = seoul_list + row_list
                print(pre, end)

                if i % 100 == 0 or i == 476:
                    # result = dict()
                    # result['seoul'] = seoul_list

                    with open((location + 'api_seoul'+str(cnt)+'.json'), 'w', encoding='utf8') as file:
                        json.dump(seoul_list, file, ensure_ascii=False)

                    cnt += 1
                    seoul_list = list()


        if __name__ == '__main__':
            api_dict = getCountList()
            # for i in range(1, len(api_dict['count_list'])):
            #     if i%100 == 0 or i == 476:
            #         print(i)

            # createFile(api_dict)
        """
        sm.dbgout(f"getSeoul SUCCESS")
    except Exception as ex:
        sm.dbgout(f"getSeoul FAIL -> {str(ex)}")

def kakaoCrawling():
    """카카오 리뷰 크롤링"""
    # 정윤
    try:
        """
        from bs4 import BeautifulSoup as bs
        from selenium import webdriver
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.common.exceptions import NoSuchElementException
        from collections import OrderedDict
        ######################################## *음식점 데이터 ##########################################
        ## 동준님 코드
        # 서울 음식점 데이터 불러오기
        df = pd.read_csv('seoul_total_id.csv')
        # id
        s_id = df['id']
        # 식당명
        name = df['s_name']

        # XX구 -> XX로 바꾸는 작업
        # expand = True : split을 통해 나눈 데이터로 컬럼 나누기
        gu = df['s_add'].str.split(' ', expand=True)[1].str[:-1]

        # 도로명 주소(대로)
        road = df['s_road'].str.split(' ', expand=True)[2]
        ######################################## *드라이버 설정 ##########################################
        options = webdriver.ChromeOptions()

        # 창을 띄우지 않고 렌더링(화면을 그려주는 작업)을 가상으로 진행해주는 방법
        options.add_argument('headless')
        # 가짜 플러그인 (사람처럼 보이게 하기)
        options.add_argument('lang=ko_KR')
        chromedriver_path = "chromedriver"
        options.add_experimental_option("detach", True)
        options.add_experimental_option("excludeSwitches",["enable-logging"] )
        service = Service(executable_path=ChromeDriverManager().install())

        # getcwd(): 현재 디렉토리의 위치를 알려줌
        driver = webdriver.Chrome(os.path.join(os.getcwd(), 'C:/chromedriver.exe'), service=service, options=options)
        driver.implicitly_wait(5) # 웹페이지가 로딩 될때까지 5초는 기다림
        # 카카오맵
        driver.get("https://map.kakao.com/")

        seoul = list()
        # incheon = list()
        # gyeongi = list()
        count = 0
        ######################################## *크롤링 시작 ##########################################
        # 데이터 개수와 데이터 넣을 리스트 세팅
        for i in range(0, len(df)):
            # 데이터 넣을 dict
            store_dict=dict()
            # id
            store_dict['id'] = int(s_id[i])
            # name
            store_dict['s_name'] = name[i]
            # 검색창
            search = driver.find_element(By.XPATH, '//*[@id="search.keyword.query"]')
            # 검색창에 검색어 입력하기
            try:
                search.send_keys(gu[i] + ' ' + name[i])
            except:
                try:
                    search.send_keys(road[i] + ' ' + name[i])
                except:
                    search.send_keys("no")
            time.sleep(3)
            
            # 식당 이름 전처리 ex>리메이크(remake) -> 리메이크:
            if '(' in name[i]:
                name[i] = re.sub(r'\([^)]*\)','',name[i])
                pd.set_option('mode.chained_assignment', 'warn') 

            # 입력 후 클릭
            driver.find_element(By.ID, 'search.keyword.query').send_keys(Keys.ENTER)
            time.sleep(3)

            # html.text 가져오기
            html = driver.page_source
            soup = bs(html, 'html.parser')
            review = list()
            try:
                cnt = 1
                # 식당이 존재하면 try, 아니라면 except
                exist_store = soup.select('#info\.search\.place\.list > li:nth-child(1) > div.info_item > div.contact.clickArea > a.moreview')[0].attrs['href']
                while True:
                    url_review = "https://place.map.kakao.com/commentlist/v/" + exist_store.split('/')[-1] + f"/{cnt}"
                    resp = requests.get(url_review)
                    # 댓글 페이지 수 계산
                    totalPage = math.ceil(resp.json()['comment']['kamapComntcnt']/5)
                    # 리뷰 있으면
                    try:
                        for i in range(len(resp.json()['comment']['list'])):
                            try:
                                review.append(resp.json()['comment']['list'][i]['contents'].replace('\n', ' '))
                            except:
                                pass
                        store_dict['s_review'] = review
                    except:
                        break
                    if totalPage == cnt:
                        break
                    cnt += 1
                    
                    # 응답 상태가 정상(200)이 아닐 때에는 pass시키기
                    if resp.status_code != 200:
                        pass
            # 식당이 검색되지 않으면 list index out of range
            # 리뷰가 존재하지 않으면 comment
            except Exception as e:
                print(e)
                pass
            seoul.append(store_dict)
            search.clear()
            count += 1
            print(s_id[i])
            # 하나하나씩 써줘
            if count % 1 == 0:
                kakao = OrderedDict()
                kakao = seoul
                with open('kakao_28121-29999.json', 'w', encoding='utf-8') as f:
                    json.dump(kakao, f, ensure_ascii=False, indent = "\t")

            # 500번씩 새로고침 해줘(메모리 과부하)
            if count % 500 == 0:
                driver.refresh()
                """
        sm.dbgout(f"kakaoCrawling SUCCESS")
    except Exception as ex:
        sm.dbgout(f"kakaoCrawling FAIL -> {str(ex)}")            

def naverCrawling():
    """네이버 식당 크롤링"""
    # 채원
    try:
        """
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup
        import time
        import csv
        import os
        import json
        from selenium.webdriver.common.keys import Keys

        dir_path = os.path.dirname(os.path.realpath(__file__))
        seoul = [] # 124,500
        incheon = [] # 16,384
        gyeonggi = [] # 139,481
        naver_list = []

        # 딕셔너리 덮어쓰기 위한 함수
        def toJson(naver_list,name):
            with open(f'naver_{name}.json', 'w', encoding='utf-8') as file :
                json.dump(naver_list, file, ensure_ascii=False, indent='\t')

        with open(dir_path + '/data/api/seoul_total_id.csv') as f:
            seoul_csv = csv.reader(f)
            for sl in seoul_csv: seoul.append(sl)
        seoul.pop(0)

        with open(dir_path + '/data/api/incheon_total.csv') as f :
            incheon_csv = csv.reader(f)
            for ic in incheon_csv : incheon.append(ic)
        incheon.pop(0)

        with open(dir_path + '/data/api/gyeonggi_total.csv') as f :
            gyeonggi_csv = csv.reader(f)
            for gg in gyeonggi_csv : gyeonggi.append(gg)
        gyeonggi.pop(0)


        def s_info_seoul_gyeonggi(sl):
            id = sl[0]
            s_name = sl[1].replace('(주)', '')
            s_add_arr = []
            if len(sl[2]) != 0 | len(sl[2].split()) > 3:
                s_add_arr = sl[2].split()
                s_add_gu = s_add_arr[1]
                s_add_dong = s_add_arr[2]
            else:
                s_add_gu, s_add_dong = '', ''
            s_add_ro = ''
            if len(sl[3].strip()) != 0: s_add_ro = sl[3].split()[2].replace(' ', '')

            l = 0
            # 도로명 주소에서 숫자빼고 한글만 가져오기
            for index, s in enumerate(s_add_ro):
                if str(s).isnumeric():
                    l = index
                    s_add_ro = s_add_ro[:l]
                    break
                else:
                    pass

            return id,s_name, s_add_arr, s_add_gu, s_add_dong, s_add_ro
        def s_info_incheon():
            id = ic[0]
            s_name = ic[1]

            # 인천은 모두 도로명주소 => 동(s_add_dong)이 없음
            # 아예 없는 경우를 대비하여 변수 미리 설정
            s_add_arr = []
            s_add_gu = ''
            s_add_ro = ''

            if len(ic[2]) != 0 | len(ic[2].split()) > 3:
                s_add_arr = ic[2].split()
                s_add_gu = s_add_arr[1]
                s_add_ro = s_add_arr[2].replace(' ', '')

            l = 0
            # 도로명 주소에서 숫자빼고 한글만 가져오기
            for index, s in enumerate(s_add_ro):
                if str(s).isnumeric():
                    l = index
                    s_add_ro = s_add_ro[:l]
                    break
                else:
                    pass

            return id,s_name,s_add_arr,s_add_gu,s_add_ro,s_add_ro
        def driver_get():
            # 옵션 생성
            options = webdriver.ChromeOptions()

            # 창 숨기는 옵션 추가
            # options.add_argument("headless")
            options.add_argument('disable-gpu')
            service_key = webdriver.chrome.service.Service('../drivers/chromedriver')
            url = f'https://map.naver.com/v5/search/서울/place'
            driver = webdriver.Chrome(service=service_key)
            # driver = webdriver.Chrome(service=service_key, options=options)
            driver.implicitly_wait(2)
            driver.get(url)  # url 가져오기
            time.sleep(7)
            return driver

        def s_info_get(naver_dict):
            driver.switch_to.default_content()
            driver.switch_to.frame('entryIframe')
            time.sleep(2)

            # 영업시간 더보기 클릭
            try:
                driver.find_elements(By.CSS_SELECTOR, '._20Y9l')[0].click()
            except:
                pass

            # 주소 더보기 클릭
            driver.find_element(By.CSS_SELECTOR, '._2yqUQ').click()
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            body = soup.find('body', class_='place_on_pcmap')

            # 가져와야 하는 정보 : 홈 - 모든 ul(전화번호, 주소, 영업시간, 기타 정보) / 메뉴
            ## 홈 - ul - li들 ( 전화번호, 주소, 영업시간, 기타 정보 ) / 리뷰 - 리뷰 / 메뉴 - 메뉴
            ul_home = body.find('ul', class_='_6aUG7')
            time.sleep(1)

            # -----------------------------------------------------------------------
            # a. 가게 이름, 가게 연락처, 사진
            naver_dict['id'] = id
            naver_dict['s_name'] = s_name
            naver_dict['s_tel'] = ul_home.find('span', class_='_3ZA0S').get_text()
            # 사진
            photo_arr = list()
            for pt_idx in range(3): photo_arr.append(
                body.find_all('div', class_='_1ZDCY')[pt_idx].find('div', class_='cb7hz _div')['style'].split('(')[
                    -1].split(')')[0].replace('\"', ''))
            naver_dict['s_photo'] = photo_arr
            # -----------------------------------------------------------------------
            # b. 주소 & 영업시간
            ul_home_add_div = ul_home.find_all('div', class_='TDq8t')
            # 주소_도로명
            if ul_home.find('span', class_='_3SYpB').get_text() == '도로명': naver_dict['s_road'] = ul_home.find('div',
                                                                                                            class_='TDq8t').get_text().replace(
                '도로명', '').replace('복사', '')
            # 주소_지번
            if ul_home_add_div[-1].find('span', class_='_3SYpB').get_text() == '지번':
                add_text = naver_dict['s_road'].split()[0] + ' ' + naver_dict['s_road'].split()[1] + ' ' + \
                        ul_home_add_div[-1].get_text().replace('지번', '').replace('복사', '')
                naver_dict['s_add'] = add_text[:add_text.find('우')]

            # 영업시간
            try:
                naver_dict['s_hour'] = body.find('span', class_='_20pEw').get_text()
            except:
                naver_dict['s_hour'] = body.find('div', class_='_2ZP3j').get_text()
            # -----------------------------------------------------------------------
            # c. 편의
            for span in body.find_all('span', class_='place_blind'):
                if span.get_text().replace(' ', '') == '편의': naver_dict[
                    's_etc'] = span.find_parent().find_next_sibling().get_text()

            ul_arr = body.find_all('span', class_='_3aXen')
            for i in range(len(ul_arr)):
                if ul_arr[i].get_text() == '메뉴': menu_idx = i
                if ul_arr[i].get_text() == '리뷰': review_idx = i
            # -----------------------------------------------------------------------
            # d. 리뷰
            driver.find_elements(By.CSS_SELECTOR, '._3aXen')[review_idx].click()
            time.sleep(1)
            driver.switch_to.default_content()
            driver.switch_to.frame('entryIframe')
            time.sleep(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            body = soup.find('body', class_='place_on_pcmap')

            review_cnt = int(body.find('span', class_='place_section_count').get_text())
            review_arr = list()
            # 더보기 버튼 있을 경우 - 최대 200개만 가져오기로
            if review_cnt > 10:
                if (review_cnt // 10) >= 20:
                    for_cnt = 20  # 리뷰가 200개가 넘는다면, 최대 20번 더보기 클릭
                else:
                    for_cnt = review_cnt // 10  # 리뷰가 200개가 되지 않는다면, 횟수만큼 더보기 클릭
                    if review_cnt % 10 == 0: for_cnt - 1  # ex. 리뷰가 20개라면 1번(20//2 - 1)만 누르도록

                for f_cnt in range(for_cnt):
                    time.sleep(1)
                    driver.switch_to.default_content()
                    driver.switch_to.frame('entryIframe')
                    time.sleep(1)
                    driver.find_element(By.CSS_SELECTOR, '._3iTUo').click()

                driver.switch_to.default_content()
                driver.switch_to.frame('entryIframe')
                time.sleep(2)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                body = soup.find('body', class_='place_on_pcmap')
                time.sleep(2)
                review_li_arr = body.find_all('li', class_='_3FaRE')
                for j in range(len(review_li_arr)):
                    try : driver.find_elements(By.CSS_SELECTOR,'.WoYOw')[j].click()
                    except : pass

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                body = soup.find('body', class_='place_on_pcmap')
                time.sleep(2)
                review_li_arr = body.find_all('li', class_='_3FaRE')

                for j in range(len(review_li_arr)):
                    try:
                        review_text = review_li_arr[j].find('span', class_='WoYOw').get_text()
                        review_arr.append(review_text)
                    except:
                        pass

            # 더보기 버튼 없을 경우 (전체 리뷰 10개 이하)
            else:
                review_li_arr = body.find_all('li', class_='_3FaRE')
                for j in range(len(review_li_arr)): driver.find_elements(By.CSS_SELECTOR, '.WoYOw')[j].click()

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                body = soup.find('body', class_='place_on_pcmap')
                time.sleep(3)
                review_li_arr = body.find_all('li', class_='_3FaRE')

                for j in range(len(review_li_arr)):
                    driver.find_elements(By.CSS_SELECTOR,'.WoYOw')[j].click()
                    review_text = review_li_arr[j].find('span', class_='WoYOw').get_text()
                    review_arr.append(review_text)
            naver_dict['s_review'] = review_arr
            # -----------------------------------------------------------------------
            # e. 메뉴
            try:
                # 메뉴 리스트 형태 1번 : 일반적인 메뉴 리스트
                driver.switch_to.default_content()
                driver.switch_to.frame('entryIframe')
                ul_arr = driver.find_elements(By.CSS_SELECTOR, '._3aXen')
                ul_arr[menu_idx].click()
                time.sleep(1)
                driver.switch_to.default_content()
                driver.switch_to.frame('entryIframe')
                time.sleep(1)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                body = soup.find('body', class_='place_on_pcmap')
                menu_li_arr = body.find_all('li', class_='_3j-Cj')

                menu_arr = list()
                for i in range(len(menu_li_arr)):
                    menu_dict = dict()
                    menu_dict[menu_li_arr[i].find('span', class_='_3yfZ1').get_text()] = menu_li_arr[i].find('div',
                                                                                                            class_='_3qFuX').get_text()
                    menu_arr.append(menu_dict)

                # 메뉴 리스트 형태 2번 : 주문형태의 메뉴 리스트
                if len(menu_arr) == 0:
                    menu_div_arr = driver.find_elements(By.CLASS_NAME, 'd_menu_list')
                    # 메뉴 리스트 개수만큼 클릭 후 더보기 버튼 클릭까지 진행
                    for menu_i in range(len(menu_div_arr)):
                        menu_div = menu_div_arr[menu_i]
                        # 2개는 이미 펼쳐져 있음
                        if menu_i >= 2: menu_div.click()

                        # 더보기 버튼 클릭
                        while True:
                            try:
                                menu_div.find_element(By.CLASS_NAME, 'sc_extend_view').click()
                            except:
                                break

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    body = soup.find('body', class_='place_on_pcmap')
                    menu_name_arr = body.find_all('div', class_='tit ellp2')

                    for menu_j in range(len(menu_name_arr)):
                        menu_dict = dict()
                        menu_name = menu_name_arr[menu_j].get_text()
                        menu_price = menu_name_arr[menu_j].find_next_siblings('div')[-1].get_text()
                        menu_dict[menu_name] = menu_price
                        menu_arr.append(menu_dict)

                naver_dict['s_menu'] = menu_arr
            except:
                pass
            return naver_dict

        def crawling(naver_list,driver,name):
            naver_dict = dict()
            menu_idx, review_idx, diff_cnt = 0, 0, 0
            
            driver.switch_to.default_content()
            # 검색 입력창 찾기
            Search_store = driver.find_element(By.XPATH,
                                            'html/body/app/layout/div[3]/div[2]/shrinkable-layout/div/app-base/search-input-box/div/div/div/input')
            Search_store.click()
            time.sleep(2)

            # 기존 입력 지우기
            Search_store.send_keys(Keys.COMMAND + "a")
            Search_store.send_keys(Keys.DELETE)
            Search_store.clear()

            time.sleep(1)

            # 식당명 입력 후 검색하기
            Search_store.send_keys(s_add_dong + ' ' + s_name)
            time.sleep(0.5)
            Search_store.send_keys(Keys.ENTER)
            time.sleep(2)

            print(id, s_name)

            ## 검색어 입력 후, 목록으로 뜰 경우
            try:
                driver.switch_to.frame('searchIframe')
                time.sleep(3)
                search_ul = driver.find_elements(By.TAG_NAME, 'ul')
                search_li_list = search_ul[0].find_elements(By.TAG_NAME, 'li')

                # 모든 목록에 대하여 하나씩
                for i in range(len(search_li_list)):
                    # 가게 선택
                    search_li_div = search_li_list[i].find_elements(By.TAG_NAME, 'div')
                    search_li_div[1].click()

                    # 가장 상위의 html로 이동
                    driver.switch_to.default_content()
                    time.sleep(1)

                    # 두번째 아이프레임 찾기 (가게 클릭 후 아이프레임)
                    driver.switch_to.frame('entryIframe')
                    time.sleep(2)

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    body = soup.find('body', class_='place_on_pcmap')

                    search_add = body.find('span', class_='_2yqUQ').get_text()
                    search_add_ro = search_add.split()[2]

                    l = 0
                    # 도로명 주소에서 숫자빼고 한글만 가져오기
                    for index, s in enumerate(search_add_ro):
                        if str(s).isnumeric():
                            l = index
                            search_add_ro = search_add_ro[:l]
                            break

                    ## 목록에서 같은 가게를 찾았을 경우
                    if search_add_ro == s_add_ro:
                        naver_dict = s_info_get(naver_dict)
                        break

                    ## 목록에서 가게가 다를 경우 : 도로명 주소가 다름
                    else:
                        diff_cnt += 1
                        if (diff_cnt >= 5) | (diff_cnt == len(search_li_list)):
                            naver_dict['id'] = id
                            break

                    ## 목록 내 다음 가게 선택 전 driver 위치 재조정
                    driver.switch_to.default_content()
                    driver.switch_to.frame('searchIframe')
                print()
                # 다음 음식점 검색 전 driver 위치 재조정 ( 검색용 driver 위치 )
                driver.switch_to.default_content()

            ## 검색어 입력 후, 목록으로 뜨지 않을 경우 ( 바로 가게로 뜨거나 목록 ul이 없는 경우 )
            except:
                # 바로 가게로 뜨는 경우
                try:
                    naver_dict = s_info_get(naver_dict)
                # 목록 ul이 없는 경우
                except:
                    naver_dict['id'] = id

            # 저장하기
            naver_list.append(naver_dict)
            toJson(naver_list,name)

        # for rep_cnt in range(3700):
        driver = driver_get()
        naver_list = []


        crawling_num = 55001 # 여기만 숫자 바꿔가면서 하면 됨
        for sl in seoul[crawling_num:crawling_num+1] :
            id, s_name, s_add_arr, s_add_gu, s_add_dong, s_add_ro = s_info_seoul_gyeonggi(sl)
            crawling(naver_list,driver,f'{crawling_num+100-1}')
        driver.close()
        """
        sm.dbgout(f"naverCrawling SUCCESS")
    except Exception as ex:
        sm.dbgout(f"naverCrawling FAIL -> {str(ex)}")

def mangoCrawling():
    """망고 플레이트 식당 정보 크롤링"""
    try:
        # 동준
        """
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.common.exceptions import NoAlertPresentException, NoSuchElementException, TimeoutException, ElementNotInteractableException,NoSuchWindowException, NoSuchFrameException, WebDriverException
        from bs4 import BeautifulSoup
        import requests
        from time import sleep
        import pandas as pd
        import json
        from collections import OrderedDict
        import datetime


        class Mango():

            def __init__(self, file_name):
                self.file_name = file_name

            def crawling(self):
                file_name = self.file_name
                # 음식점 데이터 불러오기
                df = pd.read_csv(f'{file_name}.csv')
                num = int(input())
                df = df[df['id'] >= num]
                df = df.reset_index()
                ## id
                s_id = df['id']
                ## 식당 명
                name = df['s_name']
                ## XX구 -> XX으로 바꾸는 작업. ex) 용산구 -> 용산
                ### 서울,경기는 's_add', 's_road' 둘 다 존재하지만 두 컬럼 다 결측치가 존재해서 결측치 처리
                ### 인천은 's_road' 컬럼만 가지고 있어 따로 처리해줬다.
                if 's_add' in df.columns:
                    gu_add = df['s_add'].str.split(' ', expand=True)[1].str[:-1].fillna(0)
                    gu_road = df['s_road'].str.split(' ', expand=True)[1].str[:-1].fillna(0)
                    gu = []
                    for i in range(len(df)):
                        if gu_road[i] == 0:
                            gu.append(gu_add[i])
                        else:
                            gu.append(gu_road[i])
                else:
                    df['gu'] = df['s_road'].str.split(' ', expand=True)[1].str[:-1]
                    gu = df['gu'].fillna('남')

                ## 도로명 주소(대로)
                road = df['s_road'].str.split(' ', expand=True)[2]


                # 망고플레이트 검색 페이지로 시작
                url = f'https://www.mangoplate.com/search/시작'

                # 다운 받은 webdriver를 가져와
                service = webdriver.chrome.service.Service('chromedriver.exe')
                # 경로를 설정해주고, 브라우저를 자동으로 실행하는 명령을 driver에 저장해준다.
                driver = webdriver.Chrome(service=service)
                # 기다렸다가 url 가져오기
                driver.implicitly_wait(1) 
                driver.get(url)



                # 팝업창 끄기
                ## 프레임으로 이동 
                driver.switch_to.frame(driver.find_element_by_tag_name('iframe'))
                ## 닫기 버튼 누르고
                driver.find_element(By.CSS_SELECTOR, '.ad_btn_wrap:nth-child(2)').click()
                ## 기존 HTML문서로 다시 복귀
                driver.switch_to.default_content()




                # 개수 세기위한 count 정의
                count = 0
                city = []






                # 음식점 데이터 가져와서 그 크기만큼 반복문 실행

                for i in range(len(df)):
                    sleep(3)
                    # 검색창에 값 입력 후 Enter키
                    driver.find_element(By.XPATH, '/html/body/header/div/label/input').send_keys(gu[i] + ' ' + name[i])
                    sleep(2)
                    driver.find_element(By.XPATH, '/html/body/header/div/label/input').send_keys(Keys.ENTER)
                    sleep(2)


                    
                    # dict 형태 만들기
                    store = {}
                    


                    # 식당이 존재한다면 클릭 시도
                    try:
                        store['id'] = int(s_id[i])
                        # 자꾸 webdriver 어쩌구 대처하기
                        try:
                            # 검색 후 처음 나오는 식당을 선택
                            first = driver.find_element(By.XPATH, '/html/body/main/article/div[2]/div/div/section/div[3]/ul/li[1]/div[1]')
                            first.click()
                            sleep(1)
                            
                            print(road[i])
                            print(driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split(' ')[2])
                            # 해당 주소가 맞는지 도로명주소의 3번째 요소를 가지고 비교
                            if road[i] == driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split(' ')[2]:
                                
                                # 상점명
                                # 만약 지점 데이터가 있다면 상점명 + 지점명
                                if driver.find_element(By.CSS_SELECTOR, '.branch') == NoSuchElementException:
                                    store['s_name'] = driver.find_element(By.CSS_SELECTOR, '.restaurant_name').text
                                else:
                                    store['s_name'] = (driver.find_element(By.CSS_SELECTOR, '.restaurant_name').text + ' ' + driver.find_element(By.CSS_SELECTOR, '.branch').text).strip()
                                    

                                # 주소명
                                ## 지번이 있으면 지번도 가져오기!
                                if driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td/span[2]') == NoSuchElementException:
                                    store['s_road'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split('\n')[0]
                                else:
                                    store['s_road'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split('\n')[0]
                                    store['s_add'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td/span[2]').text
                                    
                                
                                
                                ## 상세정보는 주소가 같은 위치에 고정으로 나와있다.
                                ## 그 외의 것들에는 ' 전화번호, 음식 종류, 메뉴, 주차, 영업시간, 쉬는시간, 마지막 주문, 휴일, 웹 사이트 ' 가 있다.
                                
                                # 상세정보 테이블을 가져와서
                                table = driver.find_element(By.CSS_SELECTOR, 'body > main > article > div.column-wrapper > div.column-contents > div > section.restaurant-detail > table > tbody')
                                # tr의 개수를 세준다.
                                tr = table.find_elements_by_tag_name('tr')
                                print(len(tr))
                                # 그리고 5번째부터 tr의 총 개수까지 반복
                                for i in range(2, len(tr)+1):
                                    what = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]')
                                    if what == NoSuchElementException:
                                        pass
                                    else:
                                        # 메뉴(문자 형태만)
                                        if driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '메뉴' and driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text != '' : 
                                            menus = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                            split_menus = menus.split('\n')
                                            # 메뉴에 사진이 많아서 '+ 숫자'  이런 형태로 될 경우를 제외
                                            if menus[0] != '+':
                                                store['s_menu'] = []
                                                for i in range(0, len(split_menus), 2):
                                                    try:
                                                        menu = {}
                                                        f_name = split_menus[i]
                                                        f_price = split_menus[i+1]
                                                        menu[f_name] = f_price
                                                        store['s_menu'].append(menu)
                                                    except IndexError:
                                                        pass
                                        
                                        # 전화번호
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '전화번호':
                                            store['s_tel'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 대표메뉴    
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '음식 종류':
                                            store['s_repre'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 주차
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '주차':
                                            store['s_parking'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 영업시간
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '영업시간':
                                            store['s_hour'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 쉬는시간
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '쉬는시간':
                                            store['s_break'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 마지막주문
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '마지막주문':
                                            store['s_lastorder'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 휴일
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '휴일':
                                            store['s_holiday'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 웹 사이트
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '웹 사이트':
                                            store['s_link'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td/a').get_attribute('href')

                                



                                # 리뷰 따오기
                                review_count = int(driver.find_element(By.CLASS_NAME, 'RestaurantReviewList__AllCount').text) 
                                ## 리뷰가 없으면 넘어가기
                                if review_count == 0:
                                    pass

                                ## 리뷰가 5개 이하면 바로 리뷰가져오기
                                elif review_count <= 5:
                                    print('5개 이하임')
                                    print(f'리뷰 개수 : {review_count}')
                                    store['s_review'] = []
                                    for i in range(1, review_count + 1):
                                        review = []
                                        re_content = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > p').text
                                        # re_date = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > span').text
                                        
                                        # 리뷰 날짜가 'O일 전' 이렇게 나오는 것들 때문에 현재 시간을 구해서 빼줘야함
                                        # 'O 시간 전'도 손봐줬음
                                        # if '일' in re_date:
                                            # day = int(re_date[0])
                                            # dt_now = datetime.datetime.now()
                                            # current = dt_now.date()
                                            # re_date = str(current - datetime.timedelta(days=day))
                                        # elif '시' in re_date:
                                            # hours = int(re_date.split()[0])
                                            # print(hours)
                                            # dt_now = datetime.datetime.now()
                                            # re_date = str((dt_now - datetime.timedelta(hours=hours)).date())
                                        # else:
                                            # pass

                                        sleep(5)
                                        # 날짜와 내용을 list형태로 만들어서 append   
                                        # review.append(re_date)
                                        # review.append(re_content)
                                        store['s_review'].append(re_content)

                                # 그 이상이면 더보기를 일정 횟수 눌러주도록 만들기
                                else:
                                    print('5개 이상임')
                                    print(f'리뷰 개수 : {review_count}')
                                    store['s_review'] = []
                                    # 5개마다 더보기가 생긴다. 그리고 한 번당 5개의 리뷰가 또 나온다.
                                    for i in range(int(review_count//5)):
                                        moreview_button = driver.find_element(By.CLASS_NAME,'RestaurantReviewList__MoreReviewButton')
                                        driver.execute_script('arguments[0].click();', moreview_button)
                                        sleep(2)
                                    for i in range(1, review_count + 1):
                                        review = []
                                        re_content = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > p').text
                                        # re_date = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > span').text
                                        
                                        # 리뷰 날짜가 'O일 전' 이렇게 나오는 것들 때문에 현재 시간을 구해서 빼줘야함
                                        # 'O 시간 전'도 손봐줬음
                                        # if '일' in re_date:
                                        #     day = int(re_date[0])
                                        #     dt_now = datetime.datetime.now()
                                        #     current = dt_now.date()
                                        #     re_date = str(current - datetime.timedelta(days=day))
                                        # elif '시' in re_date:
                                        #     hours = int(re_date.split()[0])
                                        #     print(hours)
                                        #     dt_now = datetime.datetime.now()
                                        #     re_date = str((dt_now - datetime.timedelta(hours=hours)).date())
                                        # else:
                                        #     pass

                                        sleep(5)
                                        # 날짜와 내용을 list형태로 만들어서 append   
                                        # review.append(re_date)
                                        # review.append(re_content)
                                        store['s_review'].append(re_content)

                                # 식당 소개 가져오기
                                if driver.find_element(By.TAG_NAME, 'h3') == NoSuchElementException:
                                    pass
                                else:
                                    store['s_intro'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[2]/div/section/div/p').text


                                city.append(store)
                                count += 1


                            else:
                                city.append(store)
                                count += 1
                                pass


                        except WebDriverException as e:
                            # Target Detached
                            sleep(10)
                            # 검색 후 처음 나오는 식당을 선택
                            first = driver.find_element(By.XPATH, '/html/body/main/article/div[2]/div/div/section/div[3]/ul/li[1]/div[1]')
                            first.click()
                            sleep(1)
                            
                            print(road[i])
                            print(driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split(' ')[2])
                            # 해당 주소가 맞는지 도로명주소의 3번째 요소를 가지고 비교
                            if road[i] == driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split(' ')[2]:
                                
                                # 상점명
                                # 만약 지점 데이터가 있다면 상점명 + 지점명
                                if driver.find_element(By.CSS_SELECTOR, '.branch') == NoSuchElementException:
                                    store['s_name'] = driver.find_element(By.CSS_SELECTOR, '.restaurant_name').text
                                else:
                                    store['s_name'] = (driver.find_element(By.CSS_SELECTOR, '.restaurant_name').text + ' ' + driver.find_element(By.CSS_SELECTOR, '.branch').text).strip()
                                    

                                # 주소명
                                ## 지번이 있으면 지번도 가져오기!
                                if driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td/span[2]') == NoSuchElementException:
                                    store['s_road'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split('\n')[0]
                                else:
                                    store['s_road'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td').text.split('\n')[0]
                                    store['s_add'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[1]/td/span[2]').text
                                    
                                
                                
                                ## 상세정보는 주소가 같은 위치에 고정으로 나와있다.
                                ## 그 외의 것들에는 ' 전화번호, 음식 종류, 메뉴, 주차, 영업시간, 쉬는시간, 마지막 주문, 휴일, 웹 사이트 ' 가 있다.
                                
                                # 상세정보 테이블을 가져와서
                                table = driver.find_element(By.CSS_SELECTOR, 'body > main > article > div.column-wrapper > div.column-contents > div > section.restaurant-detail > table > tbody')
                                # tr의 개수를 세준다.
                                tr = table.find_elements_by_tag_name('tr')
                                print(len(tr))
                                # 그리고 5번째부터 tr의 총 개수까지 반복
                                for i in range(2, len(tr)+1):
                                    what = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]')
                                    if what == NoSuchElementException:
                                        pass
                                    else:
                                        # 메뉴(문자 형태만)
                                        if driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '메뉴' and driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text != '' : 
                                            menus = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                            split_menus = menus.split('\n')
                                            # 메뉴에 사진이 많아서 '+ 숫자'  이런 형태로 될 경우를 제외
                                            if menus[0] != '+':
                                                store['s_menu'] = []
                                                for i in range(0, len(split_menus), 2):
                                                    try:
                                                        menu = {}
                                                        f_name = split_menus[i]
                                                        f_price = split_menus[i+1]
                                                        menu[f_name] = f_price
                                                        store['s_menu'].append(menu)
                                                    except IndexError:
                                                        pass
                                        
                                        # 전화번호
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '전화번호':
                                            store['s_tel'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 대표메뉴    
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '음식 종류':
                                            store['s_repre'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 주차
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '주차':
                                            store['s_parking'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 영업시간
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '영업시간':
                                            store['s_hour'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 쉬는시간
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '쉬는시간':
                                            store['s_break'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 마지막주문
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '마지막주문':
                                            store['s_lastorder'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 휴일
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '휴일':
                                            store['s_holiday'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td').text
                                        # 웹 사이트
                                        elif driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/th').text == '웹 사이트':
                                            store['s_link'] = driver.find_element(By.XPATH, f'/html/body/main/article/div[1]/div[1]/div/section[1]/table/tbody/tr[{i}]/td/a').get_attribute('href')

                                



                                # 리뷰 따오기
                                review_count = int(driver.find_element(By.CLASS_NAME, 'RestaurantReviewList__AllCount').text) 
                                ## 리뷰가 없으면 넘어가기
                                if review_count == 0:
                                    pass

                                ## 리뷰가 5개 이하면 바로 리뷰가져오기
                                elif review_count <= 5:
                                    print('5개 이하임')
                                    print(f'리뷰 개수 : {review_count}')
                                    store['s_review'] = []
                                    for i in range(1, review_count + 1):
                                        review = []
                                        re_content = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > p').text
                                        # re_date = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > span').text
                                        
                                        # 리뷰 날짜가 'O일 전' 이렇게 나오는 것들 때문에 현재 시간을 구해서 빼줘야함
                                        # 'O 시간 전'도 손봐줬음
                                        # if '일' in re_date:
                                            # day = int(re_date[0])
                                            # dt_now = datetime.datetime.now()
                                            # current = dt_now.date()
                                            # re_date = str(current - datetime.timedelta(days=day))
                                        # elif '시' in re_date:
                                            # hours = int(re_date.split()[0])
                                            # print(hours)
                                            # dt_now = datetime.datetime.now()
                                            # re_date = str((dt_now - datetime.timedelta(hours=hours)).date())
                                        # else:
                                            # pass

                                        sleep(5)
                                        # 날짜와 내용을 list형태로 만들어서 append   
                                        # review.append(re_date)
                                        # review.append(re_content)
                                        store['s_review'].append(re_content)

                                # 그 이상이면 더보기를 일정 횟수 눌러주도록 만들기
                                else:
                                    print('5개 이상임')
                                    print(f'리뷰 개수 : {review_count}')
                                    store['s_review'] = []
                                    # 5개마다 더보기가 생긴다. 그리고 한 번당 5개의 리뷰가 또 나온다.
                                    for i in range(int(review_count//5)):
                                        moreview_button = driver.find_element(By.CLASS_NAME,'RestaurantReviewList__MoreReviewButton')
                                        driver.execute_script('arguments[0].click();', moreview_button)
                                        sleep(2)
                                    for i in range(1, review_count + 1):
                                        review = []
                                        re_content = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > p').text
                                        # re_date = driver.find_element(By.CSS_SELECTOR, f'body > main > article > div.column-wrapper > div.column-contents > div > section.RestaurantReviewList > ul > li:nth-child({i}) > a > div.RestaurantReviewItem__ReviewContent > div > span').text
                                        
                                        # 리뷰 날짜가 'O일 전' 이렇게 나오는 것들 때문에 현재 시간을 구해서 빼줘야함
                                        # 'O 시간 전'도 손봐줬음
                                        # if '일' in re_date:
                                        #     day = int(re_date[0])
                                        #     dt_now = datetime.datetime.now()
                                        #     current = dt_now.date()
                                        #     re_date = str(current - datetime.timedelta(days=day))
                                        # elif '시' in re_date:
                                        #     hours = int(re_date.split()[0])
                                        #     print(hours)
                                        #     dt_now = datetime.datetime.now()
                                        #     re_date = str((dt_now - datetime.timedelta(hours=hours)).date())
                                        # else:
                                        #     pass

                                        sleep(5)
                                        # 날짜와 내용을 list형태로 만들어서 append   
                                        # review.append(re_date)
                                        # review.append(re_content)
                                        store['s_review'].append(re_content)

                                # 식당 소개 가져오기
                                if driver.find_element(By.TAG_NAME, 'h3') == NoSuchElementException:
                                    pass
                                else:
                                    store['s_intro'] = driver.find_element(By.XPATH, '/html/body/main/article/div[1]/div[1]/div/section[2]/div/section/div/p').text


                                city.append(store)
                                count += 1


                            else:
                                city.append(store)
                                count += 1
                                pass

                    except NoSuchElementException:
                        count += 1
                        city.append(store)
                        pass
                    
                    # 상점마다 파일 덮어쓰기
                    if count % 1 == 0:
                        mango = OrderedDict()
                        mango['store'] = city
                        with open(f'mango_{file_name}_{num}.json', 'w', encoding='utf-8') as f:
                            json.dump(mango, f, ensure_ascii=False, indent = "\t")

            
        seoul = Mango('seoul')
        gyeonggi = Mango('gyeonggi')
        incheon = Mango('incheon')

        # seoul.crawling()
        # gyeonggi.crawling()
        incheon.crawling()

        """
        sm.dbgout(f"mangoCrawling SUCCESS")
    except Exception as ex:
        sm.dbgout(f"mangoCrawling FAIL -> {str(ex)}")

def diningCrawling():
    """다이닝 코드 식당 정보 크롤링"""
    try:
        sm.dbgout(f"diningCrawling SUCCESS")
    except Exception as ex:
        sm.dbgout(f"diningCrawling FAIL -> {str(ex)}")

def getTime(hour):
    """날씨 데이터 시간"""
    # hour = int(hour)
    # am 1시부터 2시간 간격 시작
    if hour == '01':
        temp_hour ='00'
    elif hour == '03':
        temp_hour = '02'
    elif hour == '05':
        temp_hour = '04'
    elif hour == '07':
        temp_hour = '06'
    elif hour == '09':
        temp_hour = '08'
    elif hour == '11':
        temp_hour = '10'
    elif hour == '13':
        temp_hour = '12'
    elif hour == '15':
        temp_hour = '14'
    elif hour == '17':
        temp_hour = '16'
    elif hour == '19':
        temp_hour = '18'
    elif hour == '21':
        temp_hour = '20'
    elif hour == '23':
        temp_hour = '22'        
    
    return temp_hour + '30'

def getWeather():
    """날씨 데이터"""
    try:
        key = "G8%2F6kLlqz2GninZfrl6HupkMdDQuH84vtXL9uJ7Pp8fYP7EhO8JJADYKZCJlTCZd0AbiIy9pCJP%2B151EAPYwRw%3D%3D"
        now = datetime.now()
        now_hour = now.strftime('%H')
        base_date = now.strftime('%Y%m%d')
        base_time = getTime(now_hour)
        # 서울(nx, ny)
        nx=60
        ny=127

        url="https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst?serviceKey={}&pageNo=1&numOfRows=1000&dataType=JSON&base_date={}&base_time={}&nx={}&ny={}".format(key, base_date, base_time, nx, ny)

        # T1H: 기온
        # SKY: 하늘 -> (1)맑음, (2)구름많음, (3)흐림
        # PTY: 강수형태 -> (0)없음, (1)비, (2)비/눈, (3)눈, (5)빗방울->비, (6)빗방울눈날림->비/눈, (7)눈날림->눈

        resp = requests.get(url,verify=False)
        resp.encoding = "UTF-8"
        weather_df = pd.DataFrame(resp.json()['response']['body']['items']['item'])
        weather_df=weather_df[weather_df['category'].isin(['PTY','SKY','T1H'])]
        weather_df=weather_df[['category','fcstDate','fcstTime','fcstValue']]

        # 강수형태 df
        rain_df=pd.DataFrame({'fcstValue':[0,1,2,3,5,6,7],'rain':['비/눈 없음','비','비/눈','눈','비','비/눈','눈']})
        # 구름 df
        cloud_df=pd.DataFrame({'fcstValue':[1,3,4],'cloud':['맑음','구름많음','흐림']})

        # csv로 저장하기
        weather_df.to_csv('/home/ubuntu/final_data/weather_df.csv',mode='w',index=False)
        rain_df.to_csv('/home/ubuntu/final_data/rain_df.csv',mode='w',index=False)
        cloud_df.to_csv('/home/ubuntu/final_data/cloud_df.csv',mode='w',index=False)
        sm.dbgout("getWeather SUCCESS")
    except Exception as ex:
        sm.dbgout(f'getWeather FAIL!->{str(ex)}')

def hdfsmsg():
    try:
        # aws에 맞는 경로로 바꿔야 함
        os.system('hdfs dfs -copyFromLocal -f /home/ubuntu/final_data /home')
        sm.dbgout("HDFS PUT SUCCESS")
    except Exception as ex:
        sm.dbgout(f"HDFS PUT FAIL! -> {str(ex)}")

# def review_hdfsmsg():
#     try:
#         os.system('hdfs dfs -getmerge /home/hjyoon/final_data/review/mango /home/hjyoon/final_data/mango_review.json')
#         os.system('hdfs dfs -getmerge /home/hjyoon/final_data/review/naver /home/hjyoon/final_data/naver_review.json')
#         os.system('hdfs dfs -getmerge /home/hjyoon/final_data/review/kakao /home/hjyoon/final_data/kakao_review.json')
#         os.system('hdfs dfs -getmerge /home/hjyoon/final_data/review/dining /home/hjyoon/final_data/dining_review.json')
#         sm.dbgout(f'REVIEW HDFS PUT DONE')
#     except Exception as ex:
#         sm.dbgout(f"REVIEW HDFS PUT FAIL! -> {str(ex)}")
#=================================================
#                      Python                    #
#=================================================

getSeoul = PythonOperator(
    task_id='getSeoul',
    python_callable=getSeoul,
    dag=dag
)

kakaoCrawling = PythonOperator(
    task_id='kakaoCrawling',
    python_callable=kakaoCrawling,
    dag=dag
)


naverCrawling = PythonOperator(
    task_id='naverCrawling',
    python_callable=naverCrawling,
    dag=dag
)


mangoCrawling = PythonOperator(
    task_id='mangoCrawling',
    python_callable=mangoCrawling,
    dag=dag
)


diningCrawling = PythonOperator(
    task_id='diningCrawling',
    python_callable=diningCrawling,
    dag=dag
)


getWeather = PythonOperator(
    task_id='getWeather',
    python_callable=getWeather,
    dag=dag
)


#=================================================
#                      Hadoop                    #
#=================================================

checkJPS = BashOperator(
    task_id='checkJPS',
    bash_command="""
                    if [ -z $(jps | grep "ResourceManager") ] || [ -z $(jps | grep NodeManager) ]
                    then echo "Not Running"; start-yarn.sh
                    else echo "Running"
                    fi

                    if [ -z $(jps | grep "NameNode") ] || [ -z $(jps | grep "DataNode") ]
                    then echo "Not Running"; start-dfs.sh
                    else echo "Running"
                    fi
                """
)

toHDFS = PythonOperator(
    task_id='toHDFS',
    python_callable=hdfsmsg,
    dag=dag
)


#=================================================
#                      Spark                     #
#=================================================

apiFileMerge = SparkSubmitOperator(
    task_id='apiFileMerge',
    application='/home/ubuntu/airflow/dags/spark_code/apiFileMerge.py'
)

apiPre = SparkSubmitOperator(
    task_id='apiPre',
    application='/home/ubuntu/airflow/dags/spark_code/apiPre.py'
)

getLatLot = SparkSubmitOperator(
    task_id='getLatLot',
    application='/home/ubuntu/airflow/dags/spark_code/getLatLot.py'
)

apiAlltoSQL = SparkSubmitOperator(
    task_id='apiAlltoSQL',
    application='/home/ubuntu/airflow/dags/spark_code/apiAlltoSQL.py'
)

apiAlltoMongo = SparkSubmitOperator(
    task_id='apiAlltoMongo',
    application='/home/ubuntu/airflow/dags/spark_code/apiAlltoMongo.py'
)

crawlingPre = SparkSubmitOperator(
    task_id='crawlingPre',
    application='/home/ubuntu/airflow/dags/spark_code/crawlingPre.py'
)

reviewPre = SparkSubmitOperator(
    task_id='reviewPre',
    application='/home/ubuntu/airflow/dags/spark_code/reviewPre.py'
)

getWeather = SparkSubmitOperator(
    task_id='getWeather',
    # aws 경로로 바꿔줘야 함
    application='/home/ubuntu/airflow/dags/spark_code/getWeather.py',
    conn_id='spark_default',
    dag=dag
)

#=================================================
#                      Slack                     #
#=================================================

slack = PythonOperator(
    task_id='sendmsg',
    python_callable=sm.dbgout,
    op_args=['ALL DONE!'],
    dag=dag
)

[getSeoul, kakaoCrawling, naverCrawling, mangoCrawling, diningCrawling, getWeather] >> checkJPS >> toHDFS >> apiFileMerge >> apiPre >> getLatLot >> apiAlltoSQL >> apiAlltoMongo >>[crawlingPre,reviewPre,getWeather] >> slack
