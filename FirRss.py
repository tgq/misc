import os
import sys
import requests as rq
from datetime import *
import pandas as pd
import sqlite3
import pytz
import xml.etree.ElementTree as ET
import pdfplumber
from sqlalchemy import create_engine, text
from IPython.display import display
import pickle
import sched
import time
import smtplib
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import xlwt


def send_mail(send_to, subject, text, files=None, server="smtp.qiye.163.com"):
    assert isinstance(send_to, list)
    msg = MIMEMultipart()
    send_from = "csafir@ccicgd.com"
    msg["From"] = send_from
    msg["To"] = COMMASPACE.join(send_to)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.attach(MIMEText(text))
    for f in files or []:
        fname = os.path.basename(f)
        with open(f, "rb") as fil:
            part = MIMEApplication(fil.read(), Name=fname)
        part["Content-Disposition"] = (
            'attachment; filename="%s"' % Header(fname, "utf-8").encode()
        )
        msg.attach(part)
    smtp = smtplib.SMTP(server, port=25)
    smtp.starttls()
    smtp.login(send_from, "dQLVKEXHxQktrZvJ")
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()


cni_file = r"D:\CNI\CN-Inspect.mdb"
cni_db = None
rqs = None
fail_flag = False


def save_cook():
    global rqs
    with open("cookie.pck", "wb") as f:
        pickle.dump(rqs.cookies, f)


def connect_cni():
    global cni_db, cni_file
    if not cni_db:
        ACEstr = "access+pyodbc:///?odbc_connect=DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ="
        cni_db = create_engine(ACEstr + cni_file).connect()
    return cni_db


def timestr(dtGMT):
    tz = pytz.timezone("Asia/Shanghai")
    t = datetime.strptime(dtGMT, "%a, %d %b %Y %H:%M:%S %Z").replace(
        tzinfo=timezone.utc
    )
    return t.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def timetype(dtGMT):
    tz = pytz.timezone("Asia/Shanghai")
    t = datetime.strptime(dtGMT, "%a, %d %b %Y %H:%M:%S %Z").replace(
        tzinfo=timezone.utc
    )
    return t.astimezone(tz)


def createTable():
    conn = sqlite3.connect("FirRss.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS rss")
    cur.execute(
        """
    CREATE TABLE rss (
        Title text Primary Key,
        'Factory ID' int,
        'Factory Contract' int,
        'Inspection Date' date,
        Inspector text,
        'Inspection Classes' text,
        'Inspection Product' text,
        SyncTime datetime)
    """
    )
    cur.execute(
        """
    CREATE TABLE [fir] ( 
        [title] VARCHAR(250) NOT NULL,
        [fc] INT NOT NULL,
        [ftyid] INT NOT NULL,
        [master] INT NULL,
        [idate] DATETIME NOT NULL,
        [ftyname] VARCHAR(250) NULL,
        [pages] INT NULL,
        [code] VARCHAR(250) NULL,
        [product] VARCHAR(250) NULL,
        [model] VARCHAR(250) NULL,
        [class] VARCHAR(250) NULL,
        [reprot] VARCHAR(250) NULL,
        [project] VARCHAR(250) NULL,
        [ftycontact] VARCHAR(250) NULL,
        [empid] VARCHAR(250) NULL,
        [empname] VARCHAR(250) NULL,
        [arrival] DATETIME NULL,
        [departure] DATETIME NULL,
        [ftytest] INT NULL,
        [retest] INT NULL,
        PRIMARY KEY ([title])
        )
    """
    )
    conn.close()


def getFFprofile():
    ckfile = "cookies.sqlite"
    prof = ""
    if sys.platform.startswith("win"):
        pth = os.getenv("APPDATA") + "\\Mozilla\\Firefox\\Profiles"
        arr = os.listdir(pth)
        for a in arr:
            if a.endswith(".default"):
                if os.path.exists(os.path.join(pth, a, ckfile)):
                    prof = os.path.join(pth, a)
                    break
    elif sys.platform.startswith("linux"):
        pth = os.path.expanduser("~/.mozilla/firefox/")
        arr = os.listdir(pth)
        for a in arr:
            if a.endswith(".default-esr"):
                if os.path.exists(os.path.join(pth, a, ckfile)):
                    prof = os.path.join(pth, a)
                    break
    return prof


def getFFcook():
    profile_dir = getFFprofile()
    ffcook = os.path.join(profile_dir, "cookies.sqlite")
    if not os.path.exists(ffcook):
        print("Get FireFox Cookies File Error:", ffcook)
        return ""
    con = sqlite3.connect(ffcook)
    spcook = pd.read_sql(
        "select name, value, host from moz_cookies "
        "where (name not like 'e600ad%' and name not like 'nSGt-%') and "
        "(host='csagrporg.sharepoint.com' or host='.sharepoint.com')",
        con,
    )
    con.close()
    cook = {}
    for i, r in spcook.iterrows():
        cook[r["name"]] = r.value
    return cook


def loadFF():
    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service

    ffprofile = getFFprofile()
    if ffprofile == "":
        print("Opps!! No Firefox Profile availabel.")
        return False
    firefox_options = webdriver.FirefoxOptions()
    firefox_options.add_argument("--no-sandbox")
    firefox_options.add_argument("--disable-gpu")
    # firefox_options.add_argument('headless')
    # firefox_options.add_argument('blink-settings=imagesEnabled=false')
    firefox_options.add_argument("-profile")
    firefox_options.add_argument(ffprofile)
    firefox_options.set_preference("permissions.default.image", 1)
    service = Service(
        executable_path=os.path.join(
            os.path.expanduser("~"),
            "geckodriver" + (".exe" if sys.platform.startswith("win") else ""),
        )
    )
    driver = webdriver.Firefox(service=service, options=firefox_options)
    driver.get("https://csagrporg.sharepoint.com/sites/FIR/Documents")
    if driver.title == "Documents - All Documents":
        driver.close()
        return True
    else:
        print("Failed to load FIR/Documents page")
        driver.close()
        return False


def get_cook(ff=False):
    global rqs
    rqs = rq.Session()
    cookfile = "cookie.pck"
    if ff:
        rqs.cookies.update(getFFcook())
    else:
        if os.path.exists(cookfile):
            with open(cookfile, "rb") as f:
                rqs.cookies.update(pickle.load(f))
        else:
            get_cook(1)


def getRss(filename="fir.xml", autoLoadFF=False):
    global rqs, fail_flag
    if not rqs:
        get_cook()
    ur = "https://csagrporg.sharepoint.com/sites/FIR/_layouts/15/listfeed.aspx?List={B349F9B0-F3C8-4DF0-960E-7DCFCB99F221}"
    print("Getting Rss feed from sharepoint...")
    try:
        # r=rq.get(ur,cookies=cook)
        r = rqs.get(ur)
    except rq.exceptions.RequestException as e:  # This is the correct syntax
        print("Net error: ", e)
        return pd.DataFrame()
    if r.status_code == 200:
        fail_flag = False
        save_cook()
        if len(filename) > 0:
            with open(filename, "wb") as f:
                f.write(r.content)
            return rss2df(filename)
        else:
            return rss2df("", r.content)
    elif r.status_code == 304:
        if autoLoadFF and fail_flag != 304:
            print(".304. Trying to refresh with Firefox...")
            loadFF()
            get_cook(1)
            fail_flag = 304
            return getRss(filename, autoLoadFF)
        else:
            print(
                "Rss-get status code: %d\n"
                "Refresh cookies here: %s"
                % (r.status_code, ur.split("_")[0] + "Documents")
            )
            print("...and then run: get_cook(1)")
            fail_flag = True
            return pd.DataFrame()
    else:
        print("Rss-get status code: %d\n" % r.status_code)
        fail_flag = True
        return pd.DataFrame()


def rss2df(filename, string=""):
    if len(string) > 0:
        root = ET.fromstring(string)
    else:
        root = ET.parse(filename).getroot()
    insp = []
    for i in root.iter("item"):
        dv = ET.fromstring("<root>%s</root>" % i.find("description").text)
        v = [[*x.itertext()] for x in dv.iter("div")]
        # vv={x[0].strip(":"):x[1].strip().lstrip("0") for x in v}
        vv = {}
        for x in v:
            if x[0] == "Inspection Date:":
                vv[x[0].strip(":")] = datetime.strptime(
                    x[1].strip(), "%m/%d/%Y"
                ).date()  # .isoformat()
            elif x[0] == "Customer #:":
                pass
            elif x[0] == "Factory Account #:":
                vv["Factory ID"] = x[1].lstrip(" 0")
            else:
                vv[x[0].strip(":# ")] = x[1].strip()
        vv["Title"] = i.find("title").text
        vv["SyncTime"] = timestr(i.find("pubDate").text)  # all to +08:00 timezone
        if "Inspection Date" in vv.keys():
            insp.append(vv)
    print("Items in RSS:\t%d" % len(insp))
    return pd.DataFrame(insp)


def writeDb(df):
    if df.empty:
        print("Nothing to add.")
        return 0
    minSync = df.SyncTime.min()
    conn = sqlite3.connect("FirRss.db")
    rss = pd.read_sql_query(
        "select title from rss where SyncTime>=?", conn, params=(minSync,)
    )
    wdf = df[~df.Title.isin(rss.Title)]
    wdf.to_sql("rss", con=conn, if_exists="append", index=False)
    conn.close()
    added = wdf.shape[0]
    print("Added to DB:\t%d" % added)
    firs = []
    for i in range(added):
        fname = dlPdf(wdf, i)
        if fname:
            firs.append(exfir(fname))
    fir2db(firs)
    return added


def db(return_df=False, show=True):
    conn = sqlite3.connect("FirRss.db")
    df = pd.read_sql_query("select * from rss order by SyncTime desc", conn)
    conn.close()
    if show:
        display(df.fillna(""))
    if return_df:
        return df


def dlPdf(df, n):
    global rqs
    if not rqs:
        get_cook()
    pdf = "https://csagrporg.sharepoint.com/sites/FIR/Documents/%d/FIR/%s.pdf"
    t = df.at[n, "Title"]
    fc = df.at[n, "Factory Contract"]
    ur = pdf % (int(fc), t)
    fname = os.path.join("dl_fir", t + ".pdf")
    # r=rq.get(ur,cookies=getFFcook())
    r = rqs.get(ur)
    if r.status_code == 200:
        with open(fname, "wb") as f:
            f.write(r.content)
        print(fname, "\t %d KB" % (len(r.content) / 1024))
        return fname
    else:
        print("Download status code: %d \nUrl: %s" % (r.status_code, ur))
        return ""


def dl_rssfir(dt, emp=""):
    conn = sqlite3.connect("FirRss.db")
    sql = (
        """
        SELECT rss.Title as rt, [Inspection Date] as idate, Inspector 
        FROM rss LEFT JOIN fir ON fir.title =rt 
        WHERE fir.title IS NULL AND [Inspection Date]> ? """
        + ("AND Inspector= ? " if emp != "" else "")
        + "ORDER BY idate desc"
    )
    dbf = pd.read_sql_query(
        sql, conn, params=(dt, emp) if emp != "" else (dt,), index_col=None
    )
    todl = dbf.rt.tolist()
    print("%d FIR to download." % len(todl))
    firs = []
    for d in todl:
        f = dlPfc(d)
        if f != "":
            firs.append(exfir(f))
    fir2db(firs)


def dlPfc(fc, idate=0):
    global rqs
    if not rqs:
        get_cook()
    pdf = "https://csagrporg.sharepoint.com/sites/FIR/Documents/%d/FIR/%d-%d.pdf"
    if idate == 0:
        two = fc.split("-")
        i_date = int(two[1])
        f_c = int(two[0])
    else:
        f_c = fc
        i_date = idate
    ur = pdf % (f_c, f_c, i_date)
    fname = os.path.join("dl_fir", "%d-%d.pdf" % (f_c, i_date))
    # r=rq.get(ur,cookies=getFFcook())
    r = rqs.get(ur)
    if r.status_code == 200:
        with open(fname, "wb") as f:
            f.write(r.content)
        print(fname, "\t %d KB" % (len(r.content) / 1024))
        return fname
    else:
        print("Download status code: %d \nUrl: %s" % (r.status_code, ur))
        return ""


def cni_fc(dt, empid):
    connect_cni()
    irec = pd.read_sql(
        text(
            "select sub,[date], EmployeeID from tblInspRecordAll "
            "where EmployeeID=? and ChargeDescription='FactoryVisitCharge' and ftyid is not null "
            "and [date]>=#%s#" % dt
        ),
        cni_db,
        params=(empid,),
    )
    for i, r in irec.iterrows():
        dlPfc(int(r["sub"]), int(r["date"].strftime("%Y%m%d")))


def exfir(fname):
    startStr = [
        "Immediate changes required as a condition of continued certification:",
        "Inspection found full compliance with CSA requirements.",
        "Conformity Testing:",
        "Factory Observations",
        "Product Observations",
        "Factory Tests",
        "Signature:",
        "Previous FIR Follow-Up:",
    ]
    changeStr = [
        "Nonconforming Product",
        "Follow up Type: Required Tests",
        "Follow up Type: Required Markings",
        "Required Markings",
        "Follow up Type: Compliance Pending",
        "Product not listed in CSA's Certification Record",
        "Required Tests",
        "Follow up Type: Test Equipment Calibration",
    ]
    VN_Code = "BECCAFEE"
    fuType = [
        "Nonconforming Product",
        "Required Tests",
        "Required Markings",
        "Compliance Pending",
        "Product not listed in CSA's Certification Record",
        "Test Equipment Calibration",
    ]
    fuCode = "BECAFE"
    productStr = [
        "Production found bearing the CSA Mark",
        "Unauthorized product found bearing the CSA Mark",
        "No production Found",
    ]
    retestStr = ["Samples Selected", "Conformity Testing Results:"]
    fir = {}
    txpg = []
    stPoint = {}
    enPoint = {}
    ver = ""
    with pdfplumber.open(fname) as pdf:
        pgs = pdf.pages
        npg = len(pgs)
        try:
            ftyname = pgs[0].extract_tables()[0][0][1].split("\n")[1]
            print(ftyname)
            fir["ftyname"] = ftyname
        except:
            print("Not as expected: Fty Name")
            return {}
        txpg.extend(pgs[0].extract_text().split("\n"))
        if txpg[1] != "FACTORY INSPECTION REPORT":
            print("Not as expected: FIR title")
            return {}
        lastline = txpg[-1]
        if lastline.startswith("QD-1436-TMP Rev. 2023-08-29"):
            ver = "230829"
            enP = 3
        elif lastline.startswith("QD-1436-TMP Rev. 11-01-21"):
            ver = "211101"
            enP = 2
        elif lastline.startswith("DQD513 Rev. 2021-04-12"):
            ver = "210412"
            enP = 2
        if ver == "":
            print("FIR Version Not Known")
            print(lastline)
            return {}
        fir["ftyid"] = int(txpg[0].split(":")[1])
        fir["fc"] = int(txpg[2].split(":")[1])
        fir["master"] = int(txpg[3].split(":")[1])
        fir["idate"] = datetime.strptime(txpg[4].split(":")[1], " %B %d, %Y").date()
        fir["pages"] = int(txpg[-1].split(" ")[-1])
        for i in range(1, fir["pages"]):
            txpg.extend(pgs[i].extract_text().split("\n")[5:-enP])
    prePnt = -1
    stid = -1
    for i in range(8, len(txpg)):
        if txpg[i] in startStr:
            stid = startStr.index(txpg[i])
            stPoint[stid] = i
            if prePnt != -1:
                enPoint[prePnt] = i
            prePnt = stid
    enPoint[stid] = len(txpg)
    # print(txpg)
    # print(enPoint)
    fir["code"] = ""
    for key in stPoint.keys():
        match key:
            case 0:  # VN found
                for i in range(stPoint[key], enPoint[key]):
                    if txpg[i] in changeStr:
                        code2a = VN_Code[changeStr.index(txpg[i])]
                        fir["code"] += code2a if code2a not in fir["code"] else ""
            case 1:  # full OK
                fir["code"] += "K"
            case 2:  # Retest
                for i in range(stPoint[key], enPoint[key]):
                    if txpg[i] in retestStr:
                        if retestStr.index(txpg[i]) == 0:
                            fir["retest"] = True
            case 3:  # Factory
                if txpg[stPoint[key] + 1] == "INSPECTION COULD NOT BE PERFORMED":
                    fir["code"] += "D"
            case 4:  # Product
                for i in range(stPoint[key], enPoint[key]):
                    if txpg[i] in productStr:
                        if productStr.index(txpg[i]) == 2:
                            fir["code"] += "Z"
                        elif productStr.index(txpg[i]) == 0:
                            di = 2
                            fir["product"] = txpg[i + di].split(": ", 1)[1]
                            di += 1
                            if txpg[i + di][0] != "•":
                                di += 1
                            fir["model"] = txpg[i + di].split(": ", 1)[1]
                            di += 1
                            fir["class"] = txpg[i + di].split(": ", 1)[1]
                            di += 1
                            fir["report"] = txpg[i + di].split(": ", 1)[1]
                            di += 1
                            fir["project"] = txpg[i + di].split(": ", 1)[1]
            case 5:  # Factory Test.
                if txpg[stPoint[key] + 1] != "No factory test required":
                    fir["ftytest"] = True
            case 6:  # Signature
                fir["ftycontact"] = txpg[stPoint[key] + 2].strip()
                fir["empid"] = txpg[enPoint[key] - 3].split(" ")[0]
                fir["empname"] = txpg[enPoint[key] - 6].strip()
                las = txpg[enPoint[key] - 1].split(" ")
                fir["arrival"] = datetime.fromisoformat("%sT%s" % (las[1], las[2]))
                fir["departure"] = datetime.fromisoformat("%sT%s" % (las[4], las[5]))
            case 7:  # Previous Followup
                fuPcode = ""
                for i in range(stPoint[key], enPoint[key]):
                    if txpg[i] == "Follow up Description:":
                        if txpg[i + 1] in fuType:
                            fuPcode = fuCode[fuType.index(txpg[i + 1])]
                    elif txpg[i].startswith(
                        "The follow up items noted above were again found out of "
                    ):
                        fir["code"] += fuPcode if fuPcode not in fir["code"] else ""
                        fir["code"] += "R" if "R" not in fir["code"] else ""
        fir["title"] = str(fir["fc"]) + "-" + fir["idate"].strftime("%Y%m%d")
    return fir


def fir_dir(d):
    dfile = os.listdir(d)
    al = []
    for i in dfile:
        if i[-4:] == ".pdf":
            al.append(exfir(d + "/" + i))
    fir2db(al)
    return al


def mergeDb(fname):
    if not os.path.exists(fname):
        print("File not found:", fname)
        return
    conn = sqlite3.connect("FirRss.db")
    con2 = sqlite3.connect(fname)
    df2 = pd.read_sql_query("select * from fir", con2)
    df1 = pd.read_sql_query("select * from fir", conn)
    dd = df2[~df2.title.isin(df1.title)]
    dd.to_sql("fir", conn, if_exists="append", index=False)
    fira = dd.shape[0]
    df2 = pd.read_sql_query("select * from rss", con2)
    df1 = pd.read_sql_query("select * from rss", conn)
    dd = df2[~df2.Title.isin(df1.Title)]
    dd.to_sql("rss", conn, if_exists="append", index=False)
    rssa = dd.shape[0]
    con2.close()
    conn.close()
    print("Fir Added:\t%d" % fira)
    print("Rss Added:\t%d" % rssa)


def fir2db(rslist):
    if len(rslist) == 0:
        return
    rs = pd.DataFrame(rslist)
    minDate = rs.idate.min()
    conn = sqlite3.connect("FirRss.db")
    dbf = pd.read_sql_query(
        "select title from fir where idate>=?", conn, params=(minDate,)
    )
    wdf = rs[~rs.title.isin(dbf.title)]
    wdf.to_sql("fir", conn, if_exists="append", index=False)
    conn.close()
    print("FIR added: %d" % wdf.shape[0])


def dbfir(dt, emp):
    conn = sqlite3.connect("FirRss.db")
    dbf = pd.read_sql_query(
        "select idate,ftyid, fc, master,empid, ftyname, code from fir "
        "where (Empid=?) and idate>=? order by empid, idate, arrival",
        conn,
        params=(emp, dt),
        index_col=None,
    )
    if "ipykernel" in sys.modules:
        display(dbf.style.hide())
    else:
        print(dbf.to_string(index=False))
    conn.close()


def xdate(x):
    dd = datetime.fromisoformat(x["syn"]).strftime("%m-%d %H:%M")
    return ("" if dd[:5] == x["idate"][-5:] else ">") + dd


def rss(reget=True, autoLoadFF=False):
    added = 0
    if reget:
        print(datetime.now().strftime("%b.%d %H:%M:%S"))
        added = writeDb(getRss("", autoLoadFF=autoLoadFF))
        if added == 0:
            return
    cn_emp = {
        "Guangqiu Tan": "球",
        "Yuchun Wu": "郁淳",
        "Jintu Chen": "锦图",
        "Hancheng Wang": "瀚城",
        "Haobin Zeng": "浩彬",
        "Tao Sun": "孙涛",
        "Congchong Shen": "聪翀",
    }
    df = db(1, 0)
    df.columns = ["Title", "fid", "fc", "idate", "emp", "class", "model", "syn"]
    df["sync"] = df[["syn", "idate"]].apply(xdate, axis=1)
    df["empc"] = df["emp"].replace(cn_emp)
    dff = df[["empc", "sync", "fid", "fc", "idate", "class", "model"]]
    display(dff.head(added + 1).fillna(""))


def sch(send_error_mail=True):
    rss(autoLoadFF=True)
    if not fail_flag:
        s = sched.scheduler(time.time, time.sleep)
        current = pd.Timestamp.now("Asia/Shanghai").floor("2h")
        if current.hour < 22:
            if current.weekday() < 5:
                h_delta = 2
            else:
                h_delta = 4
        else:
            h_delta = 12
        next = current + timedelta(hours=h_delta)
        if next.hour < 10:
            next = next.replace(hour=10)
        next = int(next.timestamp())
        s.enterabs(next, 1, sch)
        print("......Waiting for next run on: %s ......" % datetime.fromtimestamp(next))
        s.run()
    elif send_error_mail:
        dt = datetime.now().strftime("%m-%d_%H_%M_%S")
        ffname = "rssfail_%s.jpg" % dt
        time.sleep(1)
        if sys.platform.startswith("win"):
            os.system("nircmd savescreenshotwin %s" % ffname)
        else:
            os.system("scrot -u %s" % ffname)
        send_mail(
            ["ab123321b@qq.com"],
            "FirRss失效提示: " + dt,
            "FirRss发生错误于: " + dt,
            [ffname],
        )


def smfir(for_emp="gz", debug=False):
    gzemp = {
        "15353": "谭广球",
        "15345": "沈聪翀",
        "15354": "王瀚城",
        "15359": "吴郁淳",
        "15313": "陈锦图",
        "15350": "孙涛",
        "15364": "曾浩彬",
    }
    gzmail = {
        "15353": "tanguangqiu",
        "15345": "shencongchong",
        "15354": "wanghancheng",
        "15359": "wuyuchun",
        "15313": "chenjintu",
        "15350": "suntao",
        "15364": "zenghaobin",
    }
    gzcsamail = {
        "15353": "guangqiu.tan",
        "15345": "congchong.shen",
        "15354": "hancheng.wang",
        "15359": "yuchun.wu",
        "15313": "jintu.chen",
        "15350": "tao.sun",
        "15364": "haobin.zeng",
    }
    yan_email = "huangyanjun@ccicgd.com"
    if for_emp == "gz":
        femp = format(tuple(gzemp.keys()))
    else:
        if for_emp in gzemp.keys():
            femp = f"('{for_emp}')"
        else:
            print("ID not found...")
            return pd.DataFrame()
    weekmonday = pd.Period.now("W-SUN").start_time.strftime("%Y-%m-%d")
    conn = sqlite3.connect("FirRss.db")
    dbf = pd.read_sql_query(
        "SELECT idate as DateOfInspection, ftyid as FACCID, fc as AGMTPF, master as MasterFile, "
        "empid as EmployeeID, ftyname as MarketingCode, code as InspectionCode, "
        "'' as TripNo, '' as Location, '' as ChargeDescription FROM fir "
        f"WHERE (Empid in {femp}) and idate>='{weekmonday}' ORDER BY empid, idate, arrival",
        conn,
        index_col=None,
    )
    conn.close()
    if dbf.shape[0] == 0:
        print("Nothing to submit....")
        return pd.DataFrame()
    dbf.DateOfInspection = dbf.DateOfInspection.apply(s2date)
    cnt = (
        dbf.groupby("EmployeeID")
        .agg({"FACCID": pd.Series.nunique, "AGMTPF": pd.Series.nunique})
        .reset_index()
    )
    cnt["name"] = cnt["EmployeeID"].map(gzemp)
    cnt.columns = ["id", "厂", "FIR", "姓名"]
    cnt = cnt[["id", "姓名", "厂", "FIR"]]
    s_today = pd.Timestamp.now().strftime("%Y-%m-%d")
    if for_emp == "gz":
        e_name = "广州中心"
        str_stamnt = "（实际完成情况以检验员的确认邮件为准。）"
        e_to = [yan_email, gzmail["15353"] + "@ccicgd.com"]
    else:
        e_name = gzemp[for_emp]
        str_stamnt = f"\n（请检验员按实际完成情况, 将确认信息转发至 {yan_email} ）"
        e_to = [gzmail[for_emp] + "@ccicgd.com"]
        # e_to=[gzmail["15353"]+'@ccicgd.com', ]
    smt_file = "FIR_submit_(%s)_%s.xls" % (e_name, s_today)
    smt_file = os.path.join("smt_xls", smt_file)
    xlok = smdf2xls(dbf, smt_file)
    if xlok and os.path.exists(smt_file) and not debug:
        send_mail(
            e_to,
            "FIRs Done Report (%s) %s" % (e_name, s_today),
            ("本周完成的FIR，共【 %d 】个 ：%s \n\n" % (dbf.shape[0], str_stamnt))
            + cnt.to_string(index=False),
            [smt_file],
        )
    return cnt, dbf


def s2date(s):
    return datetime.strptime(s, "%Y-%m-%d")


def smdf2xls(df, xfile):
    book = xlwt.Workbook()
    sh = book.add_sheet("submit")  # , cell_overwrite_ok=True)
    col = df.keys()
    colw = [15, 10, 10, 10, 10, 50, 15, 10, 10, 15]
    for x, i in enumerate(col):
        sh.write(0, x, i)
    cw = 256
    for x, i in enumerate(colw):
        sh.col(x).width = cw * i
    cols = len(col)
    clr_font = xlwt.Font()
    clr_font.colour_index = 0x3C  # brown
    date_format = xlwt.XFStyle()
    date_format.num_format_str = "yyyy-mm-dd"
    date_colored = xlwt.XFStyle()
    date_colored.num_format_str = "yyyy-mm-dd"
    date_colored.font = clr_font
    sty_colored = xlwt.XFStyle()
    sty_colored.font = clr_font
    pemp = ""
    ucolor = False
    for i, r in df.iterrows():
        if r.EmployeeID != pemp:
            ucolor = not ucolor
            pemp = r.EmployeeID
        for x in range(cols):
            if x == 0:
                sh.write(
                    i + 1, x, r.iloc[x], date_colored if ucolor else date_format
                )  # date_colored if ucolor else date_format)
            else:
                if ucolor:
                    sh.write(i + 1, x, r.iloc[x], sty_colored)
                else:
                    sh.write(i + 1, x, r.iloc[x])
    try:
        book.save(xfile)
        return True
    except Exception as e:
        print(e)
        return False


def smt():
    gz = smfir()[0]
    print(gz.to_string(index=False))
    if not gz.empty:
        for i, r in gz.iterrows():
            smfir(r.iloc[0])


if __name__ == "__main__":
    if len(sys.argv) == 1:
        rss()
    elif sys.argv[1] == "sch":
        sch()
    else:
        rss(False)
