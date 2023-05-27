#!/usr/bin/env python
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formatdate
import sys, time
import socket
import struct
import requests
import json
import re
import os

import global_var

def SendEmail(toMail, subject, body, record_wf, type='plain'): 
    # email-info
    ret = None
    smtpPort = '25'#'465'
    smtpHost = 'mails.tsinghua.edu.cn'
    fromMail = 'slt20@' + smtpHost
    #toMail1 = '18101191080@163.com'
    username = fromMail
    password = 'sltthu123456'
    # init
    encoding = 'utf-8'
    mail = MIMEText(body.encode(encoding), type, encoding)
    mail['Subject'] = Header(subject, encoding)
    mail['From'] = '"slt" <' + fromMail + '>'
    mail['To'] = toMail
    #mail['Reply-to'] = '"slt" <' + toMail + '>'
    mail['Date'] = formatdate()
    try:
        ## smtp-server: plain/SSL/TLS
        #smtp = smtplib.SMTP(smtpHost,smtpPort)
        #smtp.ehlo()
        #smtp.login(username,password)
        # tls
        smtp = smtplib.SMTP(smtpHost,smtpPort, timeout=10)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(username,password)
        ## ssl
        #sslPort  = '465'
        #smtp = smtplib.SMTP_SSL(smtpHost,sslPort)
        #smtp.ehlo()
        #smtp.login(username,password)
        # send email
        smtp.sendmail(fromMail, toMail, mail.as_string())
        smtp.quit()
        print('Send email success')
        record_wf.write(toMail + ': send email success\n')
        ret = True
    except Exception as e:
        print('Error: unable to send email: %s' %e)
        record_wf.write(toMail + ': error, unable to send email: %s\n' %e)
        ret = False
    return ret

def FillBodySingleFstAs(fst_as, snd_as):
    body = """Dear Administrator of AS %s:
  \tI'm a PhD student from Tsinghua University, and my research is related to interdomain routing problems. Recently, when I was analyzing the BGP update messages collected by RouteViews, I found some announcements quite suspicious, where your AS seemed to be a victim. The case is as follows:
  \tDuring the period from January, 2018 to May, 2020, AS %s had been repeatedly announced routes in which the nexthop was AS %s, so it seemed that you directly announced those routes to AS %s. However, from data-plane probing, we found that AS %s did not route any traffic directly to AS %s, but rather via some other ASes. 
  \tWe are interested in this phenomenon, and to get the ground truth, we would like to ask for your help. Could you do us a favor to check whether AS %s directly peers with AS %s, and had announced routes to it? If not, this will be a forged announcement. 
  \tWe will use this information for the validation of our analysis only, and will not disclose it to any third party (even in our paper). Thank you very much and your reply will be greatly appreciated.

  Yours Sincerely,
  Sun Letong
  Department of Computer Science and Technology, Tsinghua University
  """ %(snd_as, fst_as, snd_as, fst_as, fst_as, snd_as, snd_as, fst_as)
    return body


def FillBodyMultiFstAses(fst_as_list, snd_as):
    fst_ases_str = ','.join(fst_as_list)
    body = """Dear Administrator of AS %s:
  \tI'm a PhD student from Tsinghua University, and my research is related to interdomain routing problems. Recently, when I was analyzing the BGP update messages collected by RouteViews, I found some announcements quite suspicious, where your AS seemed to be a victim. The case is as follows:
  \tDuring the period from January, 2018 to May, 2020, %d ASes: %s, had been repeatedly announced routes in which the nexthop was AS %s, so it seemed that you directly announced those routes to these ASes (%s). However, from data-plane probing, we found that these ASes (%s) did not route any traffic directly to AS %s, but rather via some other ASes. 
  \tWe are interested in this phenomenon, and to get the ground truth, we would like to ask for your help. Could you do us a favor to check whether AS %s directly peers with ASes %s, and had announced routes to them? If not, this will be a forged announcement. 
  \tWe will use this information for the validation of our analysis only, and will not disclose it to any third party (even in our paper). Thank you very much and your reply will be greatly appreciated.

  Yours Sincerely,
  Sun Letong
  Department of Computer Science and Technology, Tsinghua University
  """ %(snd_as, len(fst_as_list), fst_ases_str, snd_as, fst_ases_str, fst_ases_str, snd_as, snd_as, fst_ases_str)
    return body

#email_form = re.compile(r'(\"[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+\")')
email_form = re.compile(r'(\"(\w)+(\.\w+)*@(\w)+((\.\w+)+)\")')

nouse_emails = ["helpdesk@apnic.net", "hostmaster@apnic.net"]
def GetEmailOfAs(req, asn):
    url_apnic = "https://wq.apnic.net/query?searchtext=" #default: apnic
    url_ripe = "https://apps.db.ripe.net/db-web-ui/api/whois/search?abuse-contact=true&ignore404=true&managed-attributes=true&resource-holder=true&flags=r&offset=0&limit=20&query-string=AS"
    url_arin = "https://search.arin.net/rdap/?query=AS"
    #url_afrinic = ""
    urls = [url_apnic, url_ripe, url_arin]
    for url_pre in urls:
        url = url_pre + asn
        res = set()
        #print(ip)
        headers = {"accept":"application/json"}
        resource = req.get(url, headers=headers) 
        if resource:                
            if resource.status_code == 200:
                match_res = re.findall(email_form, resource.text)
                for elem in match_res:
                    cur_email = elem[0].strip('\"')
                    if cur_email not in nouse_emails:
                        res.add(cur_email)
        if res:
            return res #找到即返回
    return set()

def GetEmailsOfSndAsFile(filename):
    req = requests.Session()
    wf = open(filename + '_with_emails', 'w')
    with open(filename, 'r') as rf:
        curline_snd_as = rf.readline()
        while curline_snd_as:
            curline_fst_as = rf.readline()
            asn = curline_snd_as[:curline_snd_as.index('(')]
            print(asn)
            emails = GetEmailOfAs(req, asn)
            wf.write(curline_snd_as)
            wf.write(curline_fst_as)
            for elem in emails:
                wf.write('\t\t %s\n' %elem)
            curline_snd_as = rf.readline()
    wf.close()

already_send_emails = set()
def GetSendLog(send_log):
    global already_send_emails
    with open(send_log, 'r') as rf:
        curline = rf.readline()
        while curline:
            if curline.__contains__('send email success'):
                already_send_emails.add(curline.split(':')[0].strip(' '))
            curline = rf.readline()

default_emails = ['helpdesk@apnic.net', 'hostmaster@apnic.net']
def SendEmailsToSndAses(to_send_filename, send_log):
    global default_emails
    global already_send_emails
    ases_no_email = []
    failed_emails = []
    GetSendLog(send_log)
    wf = open('tmp.log', 'w')
    subject = ("""Could you help to confirm whether these BGP announcements are forged or not? """)
    with open(to_send_filename, 'r') as rf:
        curline = rf.readline()
        snd_as = ''
        fst_as_list = []
        has_email = False
        while curline:
            if curline.startswith('\t\t'): #email
                cur_email = curline.strip('\t').strip('\n').strip(' ')
                if (cur_email in default_emails) or (cur_email in already_send_emails):
                    curline = rf.readline()
                    continue
                print('\t' + cur_email)
                has_email = True
                body = ''
                if len(fst_as_list) == 1:
                    body = FillBodySingleFstAs(fst_as_list[0], snd_as)
                else:
                    body = FillBodyMultiFstAses(fst_as_list, snd_as)
                if body:
                    if not SendEmail(cur_email, subject, body, wf):
                        failed_emails.append([snd_as, cur_email])
                        if len(failed_emails) > 10:
                              break
            elif curline.startswith('\t'): #fst as
                fst_as_list = curline.strip('\t').strip('\n').strip(' ').split(' ')
            else: #snd as
                if not has_email:
                    ases_no_email.append(snd_as)
                snd_as = curline[:curline.index('(')]
                print(snd_as)
                has_email = False
            curline = rf.readline()
    print('ases not having emails: ')
    print(ases_no_email)
    print('failed emails: ')
    for elem in failed_emails:
      print(elem)
    wf.close()
    os.system('cp %s %s_back' %(send_log, send_log))
    os.system('cat %s_back tmp.log > %s' %(send_log, send_log))

def SendEmailsToSndAses_2(to_send_filename, send_log):
    global default_emails
    global already_send_emails
    failed_emails = []
    #GetSendLog(send_log)
    wf = open('send_2.log', 'w')
    subject = ("""Could you help to confirm whether these BGP announcements are forged or not? """)
    with open(to_send_filename, 'r') as rf:
        curline = rf.readline()
        snd_as = ''
        fst_as_list = []
        while curline:
            if curline.startswith('\t'): #email
                cur_email = curline.strip('\n').strip('\t').strip(' ')
                body = ''
                if len(fst_as_list) == 1:
                    body = FillBodySingleFstAs(fst_as_list[0], snd_as)
                else:
                    body = FillBodyMultiFstAses(fst_as_list, snd_as)
                if body:
                    print('email: %s, snd_as: %s' %(cur_email, snd_as))
                    if not SendEmail(cur_email, subject, body, wf):
                        failed_emails.append([snd_as, cur_email])
                        # if len(failed_emails) > 10:
                        #       break
            else: #snd as
                (snd_as, fst_ases) = curline.strip('\n').strip(')').split('(')
                #print(snd_as)
                fst_as_list = fst_ases.split(',')
            curline = rf.readline()
    print('\n\n\nfailed emails: ')
    for elem in failed_emails:
      print(elem)
    wf.close()
    # os.system('cp %s %s_back' %(send_log, send_log))
    # os.system('cat %s_back tmp.log > %s' %(send_log, send_log))

#补丁函数
#src_filename里含有所有的email，send_log记录了已发送的email
#要从cur_filename（没有email信息）里，找到还没有发送的email, 即将发送
def StillNotEmail(src_filename, cur_filename, send_log):
    global default_emails
    global already_send_emails
    as_info_dict = dict()
    with open(src_filename, 'r') as rf:
        curline = rf.readline()
        cur_as = None
        email_list = []
        while curline:
            if curline.startswith('\t\t'): #email list
                email_list.append(curline.strip('\t').strip('\n').strip(' '))
            elif curline.startswith('\t'): #fst as
                pass
            else:
                if cur_as:
                    as_info_dict[cur_as] = email_list
                email_list = []
                cur_as = curline[:curline.index('(')]
            curline = rf.readline()            
        if cur_as:
            as_info_dict[cur_as] = email_list
    GetSendLog(send_log)
    wf_without_email = open(cur_filename + '_without_email', 'w')
    wf_to_send_email = open(cur_filename + '_to_send_email', 'w')
    with open(cur_filename, 'r') as rf:
        curline_snd_as = rf.readline()
        while curline_snd_as:
            curline_fst_as = rf.readline()
            cur_as = curline_snd_as[:curline_snd_as.index('(')]
            if cur_as not in as_info_dict.keys():
                print('Error: %s not in orignal email file' %cur_as)
            else:
                email_list = as_info_dict[cur_as]
                if len(email_list) == 0:
                    wf_without_email.write(curline_snd_as)
                    wf_without_email.write(curline_fst_as)
                else:
                    not_send_email_list = []
                    for cur_email in email_list:
                        if (cur_email not in default_emails) and (cur_email not in already_send_emails):
                            not_send_email_list.append(cur_email)
                    if len(not_send_email_list) > 0: #has emails to send
                        wf_to_send_email.write(curline_snd_as)
                        wf_to_send_email.write(curline_fst_as)
                        for elem in not_send_email_list:
                            wf_to_send_email.write('\t\t%s\n' %elem)
            curline_snd_as = rf.readline()
    wf_without_email.close()
    wf_to_send_email.close()

def GetEmailsOfSndAsFile_2(filename):
    snd_as_dict = dict()
    req = requests.Session()
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir + '/')
    with open(filename, 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            curline = rf.readline()
            while curline and curline.startswith('\t'):
                curline = rf.readline()
            snd_as = curline_ab_snd[:curline_ab_snd.index(':')]
            if snd_as not in snd_as_dict.keys():
                print(snd_as)
                snd_as_dict[snd_as] = [set(), set()]
                #snd_as_dict[snd_as][1] = GetEmailOfAs(req, snd_as)
            fst_as = curline_ab_fst[:curline_ab_fst.index(':')]
            snd_as_dict[snd_as][0].add(fst_as)
            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break
    # with open(filename + '_with_emails', 'w') as wf:
    #     for (snd_as, val) in snd_as_dict.items():
    #         wf.write(snd_as + '(' + ','.join(list(val[0])) + ')\n')
    #         wf.write('\t\n'.join(val[1]) + '\n')
    print('snd_as num: %d' %len(snd_as_dict))
    sum = 0
    for (snd_as, val) in snd_as_dict.items():
        sum += len(val[0])
    print('link num: %d' %sum)

if __name__ == '__main__':
    #sendEmailFun(subject, body)
    # req = requests.Session()
    # print(GetEmailOfAs(req, '58593'))
    #GetEmailsOfSndAsFile_2('bgp_ab_link_2_rel_trace_with_info_prior_ab_after_manual_filter')
    SendEmailsToSndAses_2(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace_with_info_prior_ab_after_manual_filter_with_emails', 'sendemail.log')

    # os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    # GetEmailsOfSndAsFile('bgp_ab_link_2_prior_ab_snd_as_info')
    #SendEmailsToSndAses('bgp_ab_link_2_snd_as_info_with_emails', 'sendemail.log')
    
    #StillNotEmail('bgp_ab_link_2_snd_as_info_with_emails', 'bgp_ab_link_2_prior_ab_snd_as_info', 'sendemail.log') #补丁函数
    #SendEmailsToSndAses('bgp_ab_link_2_prior_ab_snd_as_info_to_send_email', 'sendemail.log')
