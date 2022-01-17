import re
import requests
from bs4 import BeautifulSoup, Tag, NavigableString
import pandas as pd
import time

#       ****  CRAWl ******
# ***** israelhayom.co.il/ *********
#  get RSS bof this site, it runs every day

# -* request settings *-
headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"}
proxyDict = {"https": "10.253.38.1:808"}

# load last update of date in rss.date
rss_name_df = pd.read_csv("rss.date")
rss_name_df = rss_name_df.sample(frac=1)
rss_name_dict = dict([(i, a) for i, a in zip(rss_name_df.name, rss_name_df.date)])  # convert to dict from dataframe

print("********* this is maariv site ********")
counterDATE = 0
counterLINK = 1
counterRSS = 1
counterNOTEXT = 1
while True:
    for rss_name in rss_name_dict.keys():
        try:
        # request RSS from dict
            rss = "https://www.maariv.co.il/Rss/" + rss_name
            rss_selected = requests.get(rss,
                                        proxies=proxyDict,
                                        headers=headers)
            soup_rss_selected = BeautifulSoup(rss_selected.content.decode(), 'xml')
        except:
            print(" cant request ....")
            time.sleep(10)
            continue
        last_Date_old_df = rss_name_dict[
            rss_name]  # set last date in file inplace of of old date to compare between now date
        last_Date_old_df_changed = time.strptime(last_Date_old_df, "%a, %d %b %Y  %H:%M:%S")

        if len(soup_rss_selected.findAll("item")) != 0:
            print(rss_name , " loaded . . .!")
            for link_index in range(1, len(soup_rss_selected.findAll("item")) + 1):
                crawled_data = pd.DataFrame()  # define empty dataframe

                now_news_date = soup_rss_selected.findAll("item")[-link_index].find("pubDate").contents[0][:-4]
                now_news_date_changed = time.strptime(now_news_date, "%a, %d %b %Y  %H:%M:%S")

                rss_name_dict[rss_name] = now_news_date  # update old date in rss.date
                counterDATE += 1
                df = pd.DataFrame(rss_name_dict.items(), columns=["name", "date"])  # save the update
                df.to_csv("rss.date", index=False)

                if now_news_date_changed > last_Date_old_df_changed:
                    link = soup_rss_selected.findAll("item")[-link_index].find("link").contents[0]

                    try:
                        des = soup_rss_selected.findAll("item")[-link_index].find("description").contents[0].split("<br/>")[1]
                        des = re.sub('[~"]', '', des)
                        des = re.sub('\n', '', des).strip()
                        des = re.sub('\t', '', des).strip()
                    except:
                        des = ""

                    title = soup_rss_selected.findAll("item")[-link_index].find("title").contents[0]
                    title = re.sub('[~"]', '', title)
                    title = re.sub('\n', '', title).strip()
                    title = re.sub('\t', '', title).strip()

                    if link.split("/")[3] == "news":
                        category = link.split("/")[4]
                    else:
                        category = link.split("/")[3]

                    time.sleep(5)
                    site_selected = requests.get(link,
                                                 proxies=proxyDict,
                                                 headers=headers,
                                                 )

                    soup_selected = BeautifulSoup(site_selected.content, 'html.parser')

                    try:
                        text = soup_selected.findAll("div", attrs={"class": "article-body"})[0].text
                        text = re.sub('[~"]', '', text)
                        text = re.sub('\n', '', text).strip()
                        text = re.sub('\t', '', text).strip()
                    except:
                        print("notext ----->", link)
                        continue

                    # fill the empty df to add the old data.csv
                    crawled_data["title"] = [title]
                    crawled_data["description"] = [des]
                    crawled_data["text"] = [text]
                    crawled_data["category"] = [category]
                    crawled_data["date"] = [now_news_date]
                    crawled_data["link"] = [link]

                    crawled_data.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False, mode="a",
                                        header=False)
                    print(counterLINK, " link crawled Done!")
                    counterLINK += 1

                else:
                    print("before detected")
                    continue
        else:
            print("not item on RSS", rss_name)

        time.sleep(30)
        
    df = pd.read_csv("data.csv", quoting=1, sep="~", encoding="utf-8")
    df = df.drop_duplicates(subset=["link"])
    df.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False)
    del df
    time.sleep(1800)

