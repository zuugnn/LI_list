from bs4 import BeautifulSoup as bs
import urllib.request
from selenium import webdriver
import pandas as pd
import datetime
import math
import os
import re
import timeit
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from win32comext.shell import shell, shellcon


class LessonInCrawler:
    def __init__(self, max_pages: int = 1):
        self.max_pages = max_pages
        self.executor = ThreadPoolExecutor(
            max_workers=min(max_pages + 3, os.cpu_count())
        )
        self.NOs = []
        self.data_list = []
        self.folder_root = os.path.join(
            shell.SHGetFolderPath(0, shellcon.CSIDL_DESKTOP, 0, 0), "LessonIn"
        )
        csv_name = "tutor_list_" + str(datetime.datetime.now().date())
        self.csv_name = f"{csv_name}.csv"

    def Get_Max_pages(self):
        source_code_from_URL = urllib.request.urlopen(
            "http://www.lessoninfo.co.kr/resume/index.php"
        )
        soup = bs(source_code_from_URL, "lxml", from_encoding="utf-8")
        tutor_total = soup.select_one("#listForm:last-child em span").string
        self.max_pages = math.ceil(int(tutor_total.replace(",", "")) / 25) - 1

    def Get_Tutor_no(self):
        NOs = []
        for page in range(1, self.max_pages + 1):
            source_code_from_URL = urllib.request.urlopen(
                "http://www.lessoninfo.co.kr/resume/index.php?page=" + str(page)
            )
            soup = bs(source_code_from_URL, "lxml", from_encoding="utf-8")
            tutor_list_html = soup.select("#listForm:last-child tbody tr")

            for e in tutor_list_html:
                NOs.append(str(e["id"])[9:])

        self.NOs = NOs

    def Check_dir(self):
        """folrder root 존재 여부 확인"""
        if not os.path.isdir(self.folder_root):
            os.mkdir(self.folder_root)

    def Get_Tutor_info(self, soup):
        t_phone = soup.select_one("#smsSendFrm input[name='rphone']")["value"]
        t_name = soup.select_one("#smsSendFrm input[name='wr_person']")["value"]
        t_id = soup.select_one("#smsSendFrm input[name='wr_receive']")["value"]

        content_1 = soup.select_one(
            "#content > div.content3_wrap.clearfix > div.listWrap.positionR.mt10"
        )
        t_update = content_1.select_one(
            "div.readBtn.clearfix > ul > li:nth-child(1) > span"
        ).string[16:]
        t_title = content_1.select_one(
            "div.resumeDetail.positionR > table:nth-child(1) > tbody > tr:nth-child(1) > th > div > p"
        ).string
        t_photo_tmp = content_1.select_one(
            "div.resumeDetail.positionR > table:nth-child(1) > tbody > div.personphoto > img"
        )
        t_photo = ""
        if t_photo_tmp is not None:
            src = t_photo_tmp["src"][2:]
            if src != "/images/basic/bg_noPhoto.gif":
                t_photo = "http://www.lessoninfo.co.kr" + src
        t_highest_edu = (
            content_1.find(string="최종학력").parent.parent.parent.find("td").string.strip()
        )

        content_2 = soup.select_one(
            "#content > div.content3_wrap.clearfix > div:nth-child(7) > div"
        )
        t_subject_tmp = content_2.select("table > tbody > tr:nth-child(1) > td > ul li")
        t_subject = "|".join(str(i.string) for i in t_subject_tmp)
        t_region_tmp = content_2.select("table > tbody > tr:nth-child(2) > td > ul li")
        t_region = "|".join(str(i.string) for i in t_region_tmp)
        t_career = content_2.select_one(
            "table > tbody > tr:nth-child(3) > td"
        ).string.strip()
        t_pay_tmp = content_2.select("table > tbody > tr:nth-child(4) > td p")
        t_pay = "|".join(
            str(i.string) for i in t_pay_tmp if i.string is not None
        ).replace(",", "")

        t_edu_tmp = soup.select_one(
            "#content > div.content3_wrap.clearfix > div:nth-child(8) > div > table > tbody > tr:nth-child(1)"
        ).find_next_siblings()
        # t_edu = "|".join(str(i) for i in t_edu_tmp)

        print([i.string for i in [j.find_all("td") for j in t_edu_tmp]], "*****")

        t_introduct_tmp = soup.select_one(
            "#content > div.content3_wrap.clearfix > div:nth-child(12) > div > div > ul > li:nth-child(1)"
        ).stripped_strings
        t_introduct = "|".join(t_introduct_tmp).strip(
            "|------------ 이하 생략 --------------"
        )

        data = pd.DataFrame(
            {
                "업데이트날짜": [t_update],
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
                "학력사항": [t_highest_edu],
            }
        )
        # print(data)

        return data

    def Save_Tutor_info_to_csv(self, data_lsit):
        self.Check_dir()
        data = pd.concat(data_lsit)
        """
        if os.path.isfile(os.path.join(self.folder_root, self.csv_name)):
            data.to_csv(
                os.path.join(self.folder_root, self.csv_name),
                mode="a",
                header=False,
                index=False,
                encoding="utf-8-sig",
            )

        else:
            data.to_csv(
                os.path.join(self.folder_root, self.csv_name),
                mode="w",
                header=True,
                index=False,
                encoding="utf-8-sig",
            )
        """
        data.to_csv(
            os.path.join(self.folder_root, self.csv_name),
            mode="w",
            header=True,
            index=False,
            encoding="utf-8-sig",
        )

    async def fetch(self, tutor_no):
        tutor_url = (
            "http://www.lessoninfo.co.kr/resume/alba_resume_detail.php?no=" + tutor_no
        )
        resp = await loop.run_in_executor(
            self.executor, urllib.request.urlopen, tutor_url
        )
        soup = bs(resp, "lxml", from_encoding="utf-8")
        self.data_list.append(self.Get_Tutor_info(soup))

    async def crawl(self):
        # self.Get_Max_pages()
        self.Get_Tutor_no()

        futures = [asyncio.ensure_future(self.fetch(tutor_no)) for tutor_no in self.NOs]

        await asyncio.gather(*futures)

        self.Save_Tutor_info_to_csv(self.data_list)

        print(str(len(self.NOs)) + "명의 강사 정보를 입력했습니다.")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    start = timeit.default_timer()
    r = LessonInCrawler()
    loop.run_until_complete(r.crawl())
    duration = timeit.default_timer() - start
    print("걸린 시간 : ", duration)
