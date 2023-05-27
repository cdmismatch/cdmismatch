from ftplib import FTP
import os
 
 
def ftpconnect(host, username, password):
    ftp = FTP()  # 设置变量
    timeout = 30
    port = 21
    ftp.connect(host, port, timeout)  # 连接FTP服务器
    ftp.login(username,password)  # 登录
    return ftp
 
def downloadfile(ftp, remotepath, localpath):
    #lst = ftp.nlst()
    ftp.cwd(remotepath)  # 设置FTP远程目录(路径)
    list = ftp.nlst()  # 获取目录下的文件,获得目录列表
    for name in list:
        if not name.endswith('gz'):
            continue
        print(name)
        path = localpath + name  # 定义文件保存路径
        if os.path.exists(path):
            continue
        f = open(path, 'wb')  # 打开要保存文件
        filename = 'RETR ' + name  # 保存FTP文件
        ftp.retrbinary(filename, f.write)  # 保存FTP上的文件
    ftp.set_debuglevel(0)         #关闭调试
    f.close()                    #关闭文件
 
if __name__ == "__main__":
    ftp = ftpconnect('ftp.radb.net', '', '')
    downloadfile(ftp,'radb/dbase/archive/','/mountdisk3/irr_data/')
    ftp.quit()