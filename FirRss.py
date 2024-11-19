import os
import sys
import requests as rq
from datetime import *
import pandas as pd
import sqlite3
import pytz
import xml.etree.ElementTree as ET
import pdfplumber
#import pypyodbc
from sqlalchemy import create_engine,text
from IPython.display import display

cni_file = r'D:\CNI\CN-Inspect.mdb'
cni_db = None
def connect_cni():
    global cni_db, cni_file
    if not cni_db:
        # cni_db = pypyodbc.win_connect_mdb(cni_file)
        ACEstr = 'access+pyodbc:///?odbc_connect=DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='
        cni_db = create_engine(ACEstr+cni_file).connect()
    return cni_db

def timestr(dtGMT):
    tz = pytz.timezone('Asia/Shanghai')
    t=datetime.strptime(dtGMT, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    return t.astimezone(tz).strftime('%Y-%m-%d %H:%M:%S')

def timetype(dtGMT):
    tz = pytz.timezone('Asia/Shanghai')
    t=datetime.strptime(dtGMT, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    return t.astimezone(tz)

def createTable():
    conn=sqlite3.connect("FirRss.db")
    cur=conn.cursor()
    cur.execute('DROP TABLE IF EXISTS rss')
    cur.execute("""
    CREATE TABLE rss (
        Title text Primary Key,
        'Factory ID' int,
        'Factory Contract' int,
        'Inspection Date' date,
        Inspector text,
        'Inspection Classes' text,
        'Inspection Product' text,
        SyncTime datetime)
    """)
    conn.close()

def getFFcook():
    ffcook=''
    if sys.platform.startswith('win'):
        pth=os.getenv('APPDATA')+"\\Mozilla\\Firefox\\Profiles"
        arr = os.listdir(pth)
        for a in arr:
            if a.endswith('.default'):
                ffcook=pth+"\\"+a+'\\cookies.sqlite'
                break
    elif sys.platform.startswith('linux'):
        profile_dir = os.path.expanduser('~/.mozilla/firefox/')
        profile_name = os.listdir(profile_dir)[0]  # 假设只有一个配置文件
        ffcook = os.path.join(profile_dir, profile_name, 'cookies.sqlite')
    if not os.path.exists(ffcook):
        print('Get FireFox Cookies File Error:', ffcook)
        return ''
    con=sqlite3.connect(ffcook)
    spcook=pd.read_sql("select name, value, host from moz_cookies "
                        "where (name not like 'e600ad%' and name not like 'nSGt-%') and "
                        "(host='csagrporg.sharepoint.com' or host='.sharepoint.com')", con)
    con.close()
    cook={}
    for i,r in spcook.iterrows():
        cook[r['name']]=r.value
    return cook

def getRss(filename='fir.xml'):
    cook=getFFcook()
    ur='https://csagrporg.sharepoint.com/sites/FIR/_layouts/15/listfeed.aspx?List={B349F9B0-F3C8-4DF0-960E-7DCFCB99F221}'
    print("Getting Rss feed from sharepoint...")
    try:
        r=rq.get(ur,cookies=cook)
    except rq.exceptions.RequestException as e:  # This is the correct syntax
        print("Net error: ", e)
        return pd.DataFrame()
    if r.status_code==200:
        if len(filename)>0:
            with open(filename,'wb') as f:
                f.write(r.content)
            return rss2df(filename)
        else:
            return rss2df('',r.content)
    else:
        print("Rss-get status code: %d\nRefresh cookies here: %s" %(r.status_code,ur.split('_')[0]+"Documents"))
        return pd.DataFrame()

def rss2df(filename, string=''):
    if len(string)>0:
        root = ET.fromstring(string)
    else:
        root = ET.parse(filename).getroot()
    insp=[]
    for i in root.iter('item'):
        dv=ET.fromstring("<root>%s</root>"% i.find('description').text)
        v=[[*x.itertext()] for x in dv.iter('div')]
        #vv={x[0].strip(":"):x[1].strip().lstrip("0") for x in v}
        vv={}
        for x in v:
            if x[0]=='Inspection Date:':
                vv[x[0].strip(":")]=datetime.strptime(x[1].strip(),'%m/%d/%Y').date() #.isoformat()
            elif x[0]=='Customer #:':
                pass
            elif x[0]=='Factory Account #:':
                vv['Factory ID']=x[1].lstrip(' 0')
            else:
                vv[x[0].strip(":# ")]=x[1].strip()
        vv['Title']=i.find('title').text
        vv['SyncTime']=timetype(i.find('pubDate').text) # timestr()
        if 'Inspection Date' in vv.keys():
            insp.append(vv)
    print("Items in RSS:\t%d" % len(insp))
    return pd.DataFrame(insp)


def writeDb(df):
    if df.empty:
        print("Nothing to add.")
        return
    conn=sqlite3.connect("FirRss.db")
    rss=pd.read_sql('select title from rss',conn)
    wdf=df[~df.Title.isin(rss.Title)]
    wdf.to_sql('rss', con=conn, if_exists='append',index=False)
    conn.close()
    print("Added to DB:\t%d" % wdf.shape[0])

def db(return_df=False, show=True):
    conn=sqlite3.connect("FirRss.db")
    df=pd.read_sql('select * from rss order by SyncTime desc', conn)
    conn.close()
    if show:
        display(df.fillna(""))
    if return_df:
        return df

def dlPdf(df,n):
    pdf='https://csagrporg.sharepoint.com/sites/FIR/Documents/%d/FIR/%s.pdf'
    t=df.at[n,"Title"]
    fc=df.at[n,'Factory Contract']
    ur=pdf%(fc,t)
    fname=os.path.join('dl_fir',t+'.pdf')
    r=rq.get(ur,cookies=getFFcook())
    if r.status_code==200:
        with open(fname,'wb') as f:
            f.write(r.content)
        print(fname, '\t %d KB' % (len(r.content)/1024))
    else:
        print("Download status code: %d \nUrl: %s" %(r.status_code,ur))

def dl_rssfir(dt,emp=''):
    conn=sqlite3.connect("FirRss.db")
    istr="'"
    if emp!='':
        istr="' AND Inspector='%s'"% emp
    sql ='''
        SELECT rss.Title as rt, [Inspection Date] as idate, Inspector 
        FROM rss LEFT JOIN fir ON fir.title =rt 
        WHERE fir.title IS NULL AND [Inspection Date]>'%s
        ORDER BY idate desc
        ''' % (dt+istr)
    dbf=pd.read_sql(sql,conn,index_col=None,)  
    todl=dbf.rt.tolist()
    print("%d FIR to download." % len(todl))
    firs=[]
    for d in todl:
        f= dlPfc(d)
        if f!='':
            firs.append(exfir(f))
    fir2db(firs)
        

def dlPfc(fc,idate=0):
    pdf='https://csagrporg.sharepoint.com/sites/FIR/Documents/%d/FIR/%d-%d.pdf'
    if idate==0:
        two=fc.split('-')
        i_date=int(two[1])
        f_c=int(two[0])
    else:
        f_c=fc
        i_date=idate
    ur=pdf%(f_c, f_c, i_date)
    fname='dl_fir\\%d-%d.pdf'%(f_c, i_date)
    r=rq.get(ur,cookies=getFFcook())
    if r.status_code==200:
        with open(fname,'wb') as f:
            f.write(r.content)
        print(fname, '\t %d KB' % (len(r.content)/1024))
        return fname
    else:
        print("Download status code: %d \nUrl: %s" %(r.status_code,ur))
        return ''

def cni_fc(dt,empid):
    connect_cni()
    irec = pd.read_sql(text('select sub,[date], EmployeeID from tblInspRecordAll '
                       'where EmployeeID=\'%s\' and ChargeDescription=\'FactoryVisitCharge\' and ftyid is not null '
                            'and [date]>=#%s#' % (empid,dt)),
                       cni_db)
    for i,r in irec.iterrows():
        dlPfc(int(r['sub']),int(r['date'].strftime('%Y%m%d')))


def exfir(fname):
    startStr=[
        'Immediate changes required as a condition of continued certification:',
        'Inspection found full compliance with CSA requirements.',
        'Conformity Testing:',
        'Factory Observations',
        'Product Observations',
        'Factory Tests',
        'Signature:'
    ]
    changeStr=[
        'Nonconforming Product',
        'Follow up Type: Required Tests',
        'Follow up Type: Required Markings',
        'Required Markings',
        'Follow up Type: Compliance Pending',
        "Product not listed in CSA's Certification Record",
        'Required Tests',
        'Follow up Type: Test Equipment Calibration'
    ]
    VN_Code='BECCAFEE'
    productStr=[
        'Production found bearing the CSA Mark',
        'Unauthorized product found bearing the CSA Mark',
        'No production Found'
    ]
    retestStr=[
        'Samples Selected',
        'Conformity Testing Results:'
    ]
    fir={}
    txpg=[]
    stPoint={}
    enPoint={}
    ver=''
    with pdfplumber.open(fname) as pdf:
        pgs=pdf.pages
        npg=len(pgs)
        try:
            ftyname=pgs[0].extract_tables()[0][0][1].split('\n')[1]
            print(ftyname)
            fir['ftyname']=ftyname
        except:
            print('Not as expected: Fty Name')
            return {}
        txpg.extend(pgs[0].extract_text().split('\n'))
        if txpg[1]!="FACTORY INSPECTION REPORT":
            print('Not as expected: FIR title')
            return {}
        lastline=txpg[-1]
        if lastline.startswith('QD-1436-TMP Rev. 2023-08-29'):
            ver='230829'
            enP=3
        elif lastline.startswith('QD-1436-TMP Rev. 11-01-21'):
            ver='211101'
            enP=2
        elif lastline.startswith('DQD513 Rev. 2021-04-12'):
            ver='210412'
            enP=2
        if ver=='':
            print('FIR Version Not Known')
            print(lastline)
            return {}
        fir['ftyid']=int(txpg[0].split(':')[1])
        fir['fc']=int(txpg[2].split(':')[1])
        fir['master']=int(txpg[3].split(':')[1])
        fir['idate']=datetime.strptime(txpg[4].split(':')[1], ' %B %d, %Y').date()
        fir['pages']=int(txpg[-1].split(' ')[-1])
        for i in range(1,fir['pages']):
            txpg.extend(pgs[i].extract_text().split('\n')[5:-enP])
    prePnt=-1
    stid=-1
    for i in range(8,len(txpg)):
        if txpg[i] in startStr:
            stid=startStr.index(txpg[i])
            stPoint[stid]=i
            if prePnt!=-1:
                enPoint[prePnt]=i
            prePnt=stid
    enPoint[stid]=len(txpg)
    #print(txpg)
    #print(enPoint)
    fir['code']=''
    for key in stPoint.keys():
        match key:
            case 0: # VN found
                for i in range(stPoint[key],enPoint[key]):
                    if txpg[i] in changeStr:
                        code2a=VN_Code[changeStr.index(txpg[i])]
                        fir['code']+=code2a if code2a not in fir['code'] else ''
            case 1: # full OK
                fir['code']+="K"
            case 2: # Retest
                for i in range(stPoint[key],enPoint[key]):
                    if txpg[i] in retestStr:
                        if retestStr.index(txpg[i])==0:
                            fir['retest']=True
            case 3:  # Factory
                if txpg[stPoint[key]+1] =='INSPECTION COULD NOT BE PERFORMED':
                    fir['code']+="D"
            case 4:  # Product
                for i in range(stPoint[key],enPoint[key]):
                    if txpg[i] in productStr:
                        if productStr.index(txpg[i])==2:
                            fir['code']+="Z"
                        elif productStr.index(txpg[i])==0:
                            fir['product']=txpg[i+2].split(': ',1)[1]
                            fir['model']=txpg[i+3].split(': ',1)[1]
                            fir['class']=txpg[i+4].split(': ',1)[1]
                            fir['reprot']=txpg[i+5].split(': ',1)[1]
                            fir['project']=txpg[i+6].split(': ',1)[1]
            case 5:  # Factory Test.
                if txpg[stPoint[key]+1]!='No factory test required':
                    fir['ftytest']=True
            case 6:  # Signature
                fir['ftycontact']=txpg[stPoint[key]+2].strip()
                fir['empid']=txpg[enPoint[key]-3].split(' ')[0]
                fir['empname']=txpg[enPoint[key]-6].strip()
                las=txpg[enPoint[key]-1].split(' ')
                fir['arrival']=datetime.fromisoformat('%sT%s'%(las[1],las[2]))
                fir['departure']=datetime.fromisoformat('%sT%s'%(las[4],las[5]))
        fir['title']=str(fir['fc'])+'-'+fir['idate'].strftime('%Y%m%d')
    return fir

def fir_dir(d):
    dfile=os.listdir(d)
    al=[]
    for i in dfile:
        if i[-4:]=='.pdf':
            al.append(exfir(d+'/'+i))
    fir2db(al)
    return al

def fir2db(rslist):
    if len(rslist)==0:
        return
    rs=pd.DataFrame(rslist)
    conn=sqlite3.connect("FirRss.db")
    dbf=pd.read_sql('select title from fir',conn)
    wdf=rs[~rs.title.isin(dbf.title)]
    wdf.to_sql('fir',conn, if_exists='append',index=False)
    conn.close()
    print("FIR added: %d"%wdf.shape[0])

def dbfir(dt,emp):
    conn=sqlite3.connect("FirRss.db")
    dbf=pd.read_sql('select idate,ftyid, fc, master,empid, ftyname, code from fir '
                    'where Empid=\'%s\' and idate>=\'%s\' order by idate, arrival'%(emp,dt),conn,index_col=None,)
    display(dbf.style.hide())
    conn.close()

def xdate(x):
    dd=datetime.fromisoformat(x['syn']).strftime('%m-%d %H:%M')
    return ('' if dd[:5]==x['idate'][-5:] else ">") + dd

def rss(reget=True):
    if reget:
        writeDb(getRss(''))
    df=db(1,0)
    df.columns=['Title','fid','fc','idate','emp','class','model','syn']
    df['sync']=df[['syn','idate']].apply(xdate, axis=1)
    df['empc']=df['emp'].replace({
        'Guangqiu Tan':"球",
        'Yuchun Wu':'郁淳',
        'Jintu Chen':'锦图',
        'Hancheng Wang':'瀚城'})
    dff=df[['empc','sync','fid','fc','idate','class','model']]
    display(dff.head(30).fillna(''))

if __name__=='__main__':
    rss(len(sys.argv)==1)