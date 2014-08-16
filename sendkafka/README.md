# Pre-requisites

* sudo apt-get install zlib1g-dev
* need librdkafka installed in parent directory
* https://github.com/edenhill/librdkafka
* need kafka broker running locally

# rsyslog v8

add Ubuntu repository for rsyslog (precise)

edit /etc/apt/sources.d/adiscon

```
deb http://ubuntu.adiscon.com/v8-devel precise/
deb-src http://ubuntu.adiscon.com/v8-devel precise/
```

...and then... (make sure it installs v8)

```
apt-get install rsyslog
```

# Building and installing sendkafka


make

make  install

#Sendkafka log path

mkdir /var/log/sendkafka

#Sendkafka configuration
default path 
/etc/sendkafka.conf

# Rsyslog configuration

/etc/rsyslog.conf configuration directive:

At the start:

```
$ModLoad omprog
```

After the very last line:

*.* action(type="omprog"
        binary=/usr/local/bin/sendkafka)



```

Check configuration

```
rsyslogd -f /etc/rsyslog.conf -N1
```

Modify AppArmor:

```
/etc/apparmor.d/local/usr.sbin.rsyslogd
```

Reload

If you're having problems, look into AppArmor or SELinux CAP.