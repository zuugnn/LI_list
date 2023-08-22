from bs4 import BeautifulSoup as bs
import urllib.request
from fake_useragent import UserAgent
import pandas as pd
import datetime
import math
import os
import timeit
import aiohttp
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, after_log
from concurrent.futures import ThreadPoolExecutor
from win32comext.shell import shell, shellcon
import logging
import sys

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class LessonInCrawler:
    def __init__(self, max_pages=None):
        if max_pages is None:
            resp_total = urllib.request.urlopen("http://www.lessoninfo.co.kr/resume/index.php")
            soup = bs(resp_total, "lxml", from_encoding="utf-8")
            tutor_total = soup.select_one("#listForm:last-child em span").string
            self.tutor_total = int(tutor_total.replace(",", ""))
            self.max_pages = math.ceil(self.tutor_total / 25) - 1
        else:
            self.max_pages = max_pages
            self.tutor_total = int(max_pages * 25)

        # self.executor = ThreadPoolExecutor(max_workers=min(self.max_pages + 3, os.cpu_count()))
        self.limit = asyncio.Semaphore(7)
        self.tutor_cnt = 0
        self.folder_root = os.path.join(shell.SHGetFolderPath(0, shellcon.CSIDL_DESKTOP, 0, 0), "LessonIn")
        excel_name = "tutor_list_" + str(datetime.datetime.now().date())
        self.excel_path = os.path.join(self.folder_root, f"{excel_name}.xlsx")

    def Get_Tutor_info(self, tutor_no, soup):
        t_phone = soup.select_one("#smsSendFrm input[name='rphone']")["value"]
        t_name = soup.select_one("#smsSendFrm input[name='wr_person']")["value"]
        t_id = soup.select_one("#smsSendFrm input[name='wr_receive']")["value"]

        content_1 = soup.select_one("#content > div.content3_wrap.clearfix > div.listWrap.positionR.mt10")
        t_update = content_1.select_one("div.readBtn.clearfix > ul > li:nth-child(1) > span").string[16:]
        t_title = content_1.select_one("div.resumeDetail.positionR > table:nth-child(1) > tbody > tr:nth-child(1) > th > div > p").string
        t_photo_tmp = content_1.select_one("div.resumeDetail.positionR > table:nth-child(1) > tbody div.personphoto > img")
        t_photo_src = t_photo_tmp["src"][2:]
        if t_photo_src != "/images/basic/bg_noPhoto.gif":
            t_photo = '=HYPERLINK("http://www.lessoninfo.co.kr' + t_photo_src + '"' + "," + '"' + t_photo_src + '"' + ")"
        else:
            t_photo = ""
        t_highest_edu = content_1.find(string="최종학력").parent.parent.parent.find("td").string.strip()

        content_2 = soup.select_one("#content > div.content3_wrap.clearfix > div:nth-child(7) > div")
        t_subject_tmp = content_2.select("table > tbody > tr:nth-child(1) > td > ul li")
        t_subject = "|".join(str(i.string) for i in t_subject_tmp)
        t_region_tmp = content_2.select("table > tbody > tr:nth-child(2) > td > ul li")
        t_region = "|".join(str(i.string) for i in t_region_tmp)
        t_career = content_2.select_one("table > tbody > tr:nth-child(3) > td").string.strip()
        t_pay_tmp = content_2.select("table > tbody > tr:nth-child(4) > td p")
        t_pay = "|".join(str(i.string) for i in t_pay_tmp if i.string is not None).replace(",", "")

        t_edu_tmp = soup.select_one("#content > div.content3_wrap.clearfix > div:nth-child(8) > div > table > tbody > tr:nth-child(1)").find_next_siblings()
        t_edu_l1 = []
        for i in t_edu_tmp:
            t_edu_l2 = []
            for j in i.find_all("td"):
                t_edu_l2.append(str(j.string).strip())
            t_edu_s_tmp = "|".join(t_edu_l2)
            t_edu_l1.append(t_edu_s_tmp)
        t_edu_s = "||".join(t_edu_l1)

        t_introduct_tmp = soup.select_one("#content > div.content3_wrap.clearfix > div:nth-child(12) > div > div > ul > li:nth-child(1)").stripped_strings
        t_introduct = "|".join(t_introduct_tmp).strip("|------------ 이하 생략 --------------")

        data = pd.DataFrame(
            {
                "업데이트날짜": [t_update],
                "강사번호": [int(tutor_no)],
                "이름": [t_name],
                "연락처": [t_phone],
                "아이디": [t_id],
                "사진": [t_photo],
                "제목": [t_title],
                "자기소개": [t_introduct],
                "과목": [t_subject],
                "지역": [t_region],
                "경력": [t_career],
                "희망급여": [t_pay],
                "최종학력": [t_highest_edu],
                "학력사항": [t_edu_s],
            }
        )
        # print(data)

        return data

    def Check_dir(self):
        if not os.path.isdir(self.folder_root):
            os.mkdir(self.folder_root)

    def delete_file(self, path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            pass

    def Save_Tutor_info_to_excel(self, data_list):
        self.Check_dir()

        data = pd.concat(data_list)

        """
        if os.path.isfile(self.excel_path):
            data.to_csv(self.excel_path, mode="a", header=False, index=False, encoding="utf-8-sig")

        else:
            data.to_csv(self.excel_path, mode="w", header=True, index=False, encoding="utf-8-sig")
        """

        if os.path.isfile(self.excel_path):
            with pd.ExcelWriter(
                self.excel_path,
                mode="a",
                engine="openpyxl",
                if_sheet_exists="overlay",
            ) as writer:
                data.to_excel(
                    writer,
                    header=False,
                    index=False,
                    startrow=writer.sheets["Sheet1"].max_row,
                )
        else:
            with pd.ExcelWriter(self.excel_path, mode="w", engine="openpyxl") as writer:
                data.to_excel(writer, index=False)

        self.tutor_cnt = self.tutor_cnt + len(data)

        print("**** " + str(round(self.tutor_cnt / self.tutor_total * 100, 1)) + "% 강사정보 입력 완료! (" + str(self.tutor_cnt) + "/" + str(self.tutor_total) + ") ****")

    @retry(stop=stop_after_attempt(15), wait=wait_exponential(multiplier=1, min=4, max=10), after=after_log(logger, logging.DEBUG))
    async def fetch(self, session, page):
        ua = UserAgent(browsers=["edge", "chrome"])

        page_url = "http://www.lessoninfo.co.kr/resume/index.php?page=" + str(page + 1)
        async with self.limit:
            async with session.get(page_url, headers={"User-Agent": ua.random}) as response:
                resp = await response.content.read()
                # resp = await loop.run_in_executor(self.executor, urllib.request.urlopen, urllib.request.Request(url=page_url, headers={"User-Agent": ua.random}))

                soup = bs(resp, "lxml", from_encoding="utf-8")
                tutor_list_html = soup.select("#listForm:last-child tbody tr")

                data_list = []
                for tutor in tutor_list_html:
                    tutor_no = str(tutor["id"])[9:]
                    tutor_html_url = "http://www.lessoninfo.co.kr/resume/alba_resume_detail.php?no=" + tutor_no
                    async with session.get(tutor_html_url, headers={"User-Agent": ua.random}) as response:
                        tutor_html = await response.content.read()
                        # tutor_html = await loop.run_in_executor(self.executor, urllib.request.urlopen, urllib.request.Request(url=tutor_html_url, headers={"User-Agent": ua.random}))

                        soup = bs(tutor_html, "lxml", from_encoding="utf-8")
                    data = self.Get_Tutor_info(tutor_no, soup)
                    data_list.append(data)
                else:
                    self.Save_Tutor_info_to_excel(data_list)

    async def crawl(self):
        self.delete_file(self.excel_path)
        # futures = [asyncio.ensure_future(self.fetch(page)) for page in range(self.max_pages)]
        # await asyncio.gather(*futures)
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self.fetch(session, page) for page in range(self.max_pages)])

        print("******** " + str(self.tutor_cnt) + "명의 강사 정보를 입력했습니다. ********")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    start = timeit.default_timer()
    r = LessonInCrawler()
    loop.run_until_complete(r.crawl())
    duration = timeit.default_timer() - start
    print("******** 걸린 시간 : " + str(duration) + " ********")
    loop.close()
